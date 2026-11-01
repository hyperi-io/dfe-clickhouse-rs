//! Server-side TCP packet decoders.
//!
//! Mirrors clickhouse-cpp-client `Client::Impl::ReceivePacket()`
//! (lines 679-830), `ReceiveHello()` (1207-1253), and
//! `ReceiveException()` (955-1004). Field order and revision-gating
//! match cpp-client byte-for-byte; revision-constant values match
//! ClickHouse server `Core/ProtocolDefines.h`.
//!
//! Wire primitives (varint, length-prefixed string) come from
//! [`crate::native::io::ClickHouseRead`]; tokio's `AsyncReadExt`
//! supplies fixed-width LE reads on the same trait object. No new
//! io.rs in this module.
//!
//! Data packets carry either a schema block (`num_rows == 0`) or a
//! payload block (`num_rows > 0`). Schema blocks surface their
//! `(name, type_name)` column pairs in [`ServerPacket::Data::columns`];
//! payload blocks are decoded inline through
//! [`crate::native::decode::decode_block`] and surface as
//! [`ServerPacket::DataBlock`]. Decoding inline is the only way to
//! advance past a block without misaligning the next packet's leading
//! varuint, because the column bytes run straight on from the block
//! header; `Log` and `ProfileEvents` are read-and-discarded here for the
//! same reason. Compressed blocks do not appear: the Query packet
//! always negotiates compression off.

use tokio::io::AsyncReadExt;

use crate::error::{Error, Result};
use crate::native::decode::{BlockKind, DecodedBlock, decode_block};
use crate::native::io::ClickHouseRead;
use crate::tcp::protocol::{
    DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS,
    DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES,
    DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS,
    DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS, DBMS_MIN_REVISION_WITH_BLOCK_INFO,
    DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO, DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2,
    DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION, DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME,
    DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE, DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES,
    DBMS_MIN_REVISION_WITH_VERSION_PATCH,
    DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL, DBMS_TCP_PROTOCOL_VERSION,
    Exception, ProfileInfo, Progress, ServerHello, ServerPacketId, TCP_EXCEPTION_STACK_TRACE_CAP,
    TableColumns, truncate_on_char_boundary,
};

/// Upper bound on the column count a single block header may declare.
/// The wire value is an unbounded varuint that both block bodies size
/// their allocations from, and `Vec::with_capacity` aborts the process
/// rather than erroring, which a library cannot catch. ClickHouse itself
/// refuses tables far below this, so no legitimate block reaches it.
///
/// Duplicates `native::io`'s constant of the same name and value; the two
/// collapse to one once the codec's copy is reachable from here.
const MAX_BLOCK_COLUMNS: u64 = 16_384;

/// Cap on the password-complexity rules a server may declare in Hello.
///
/// The count is an unbounded varuint that drives a read loop, so a hostile
/// or corrupt value would otherwise stall the handshake. A real server ships
/// a handful.
const MAX_PASSWORD_COMPLEXITY_RULES: u64 = 256;

/// Revision at which the server started emitting `total_rows_to_read`
/// inside the Progress packet, matching clickhouse-cpp-client
/// `client.cpp:25`. Every revision this client negotiates clears it (the
/// `CLIENT_INFO` floor is 54032), so the gate is defensive cover for a
/// server we never reach.
pub(crate) const DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS: u64 = 51554;

/// Decoded server-to-client packet.
///
/// A schema block (`num_rows == 0`) surfaces its `(name, type_name)`
/// pairs in [`ServerPacket::Data::columns`]; a payload block is decoded
/// inline and surfaces as [`ServerPacket::DataBlock`]. `Log` and
/// `ProfileEvents` are read and discarded here.
///
/// Several fields exist only because the bytes must be consumed to keep
/// the stream aligned; surfacing progress, profile info and table
/// columns to callers is a separate feature.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum ServerPacket {
    /// Schema block (Data packet with `num_rows == 0`).
    ///
    /// `table_name` is the empty string for default-target INSERTs;
    /// servers below `DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES` (50264)
    /// omit it on the wire (the field is `None` in that case).
    /// `columns` carries the `(name, type_name)` pairs the server
    /// echoed for the upcoming INSERT or SELECT stream. Payload
    /// blocks surface separately as [`ServerPacket::DataBlock`].
    Data {
        table_name: Option<String>,
        num_columns: u64,
        num_rows: u64,
        columns: Vec<(String, String)>,
    },
    /// Fully decoded payload block (`num_rows > 0`). The column-bytes
    /// payload was consumed inline by
    /// [`crate::native::decode::decode_block`]; downstream cursors
    /// iterate rows out of the [`DecodedBlock`] directly. Decoding
    /// inline is the only way to keep the reader's stream pointer
    /// aligned against the next packet.
    DataBlock(DecodedBlock),
    Exception(Exception),
    Progress(Progress),
    ProfileInfo(ProfileInfo),
    Pong,
    EndOfStream,
    /// Server-log packet (sent when the caller set `send_logs_level`,
    /// which is forwarded onto the TCP Query). Its leading tag string +
    /// Native block are read and discarded at this layer to keep the
    /// stream aligned; the log lines are not surfaced to callers.
    Log,
    TableColumns(TableColumns),
    /// ProfileEvents telemetry. The packet's leading string + Native
    /// block are read and discarded at this layer to keep the stream
    /// aligned; the event values are not surfaced to callers.
    ProfileEvents,
    /// Timezone update string sent mid-stream when the server's
    /// session timezone changes.
    TimezoneUpdate(String),
}

/// Read the server Hello packet. Mirrors cpp `ReceiveHello()`
/// lines 1207-1253. If the server sent an Exception in place of
/// Hello (auth failure, server still starting, etc.), the wire
/// frame is read and flattened into [`Error::ServerException`]
/// via [`Exception::into_error`].
pub(crate) async fn read_hello<R: ClickHouseRead>(r: &mut R) -> Result<ServerHello> {
    let packet_type = r.read_var_uint().await?;
    let id = ServerPacketId::from_u64(packet_type)?;
    match id {
        ServerPacketId::Hello => {
            let server_name = r.read_utf8_string().await?;
            let major = r.read_var_uint().await?;
            let minor = r.read_var_uint().await?;
            let revision = r.read_var_uint().await?;
            // Field presence is governed by the NEGOTIATED (effective)
            // revision = min(what we advertised, what the server
            // reports). The server gates the fields it writes here on
            // OUR advertised revision (TCPHandler::sendHello), so the
            // client must read them on the same value -- not on the raw
            // server revision, which can be higher and would make us
            // expect fields the server did not send. `revision` is
            // stored raw on `ServerHello` for version reporting; gating
            // uses `effective`. (Today all handled fields sit below the
            // advertised pin, so the two agree; this is the correct,
            // future-bump-safe value.)
            let effective = revision.min(DBMS_TCP_PROTOCOL_VERSION);
            // Fields follow the server's write order in
            // `TCPHandler::sendHello`, not the numeric order of their gates.
            if effective >= DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL {
                let _parallel_replicas_version = r.read_var_uint().await?;
            }
            let timezone = if effective >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
                Some(r.read_utf8_string().await?)
            } else {
                None
            };
            let display_name = if effective >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
                Some(r.read_utf8_string().await?)
            } else {
                None
            };
            let patch = if effective >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
                r.read_var_uint().await?
            } else {
                0
            };
            if effective >= DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS {
                // The server's send and recv capabilities. We answer
                // `notchunked` in the addendum, which it defaults to, so the
                // framing stays as it is.
                let _proto_send = r.read_utf8_string().await?;
                let _proto_recv = r.read_utf8_string().await?;
            }
            if effective >= DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES {
                let rules = r.read_var_uint().await?;
                // A hostile count would otherwise buy an unbounded read loop.
                if rules > MAX_PASSWORD_COMPLEXITY_RULES {
                    return Err(Error::BadResponse(format!(
                        "tcp: server declared {rules} password-complexity rules, over the \
                         {MAX_PASSWORD_COMPLEXITY_RULES} cap"
                    )));
                }
                for _ in 0..rules {
                    let _pattern = r.read_utf8_string().await?;
                    let _message = r.read_utf8_string().await?;
                }
            }
            if effective >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2 {
                // Sent to every client, not only an interserver peer.
                let mut nonce = [0u8; 8];
                r.read_exact(&mut nonce).await?;
            }
            Ok(ServerHello {
                server_name,
                version: (major, minor, patch),
                revision,
                timezone,
                display_name,
            })
        }
        ServerPacketId::Exception => {
            let exc = read_exception(r).await?;
            Err(exc.into_error())
        }
        other => Err(Error::BadResponse(format!(
            "tcp: unexpected packet during handshake: {other:?}"
        ))),
    }
}

/// Read a server Exception frame: signed-LE i32 `code`, length-prefixed
/// `name`, `message` and `stack_trace`, then the obsolete one-byte
/// `has_nested` flag.
///
/// The flag is read and discarded, never recursed on. The server writes
/// it hardcoded false (`src/IO/WriteHelpers.cpp:91-92`) and its own
/// reader marks the field `/// Obsolete` and does not recurse either
/// (`src/IO/ReadHelpers.cpp:1964,1970`), so a nested chain is not a shape
/// this wire produces; recursing on a peer-supplied byte would only add a
/// stack-exhaustion surface.
///
/// `stack_trace` is capped at [`TCP_EXCEPTION_STACK_TRACE_CAP`] on a
/// character boundary. This frame is reachable pre-auth through
/// [`read_hello`], so the bytes are untrusted.
///
/// # Errors
///
/// I/O errors from the underlying reader.
pub(crate) async fn read_exception<R: ClickHouseRead>(r: &mut R) -> Result<Exception> {
    // cpp reads the code as a fixed-width int32 (ReadFixed<int32_t>),
    // i.e. signed little-endian. Server-side codes are small positives
    // in practice but the wire is signed.
    let code = r.read_i32_le().await?;
    let name = r.read_utf8_string().await?;
    let message = r.read_utf8_string().await?;
    let mut stack_trace = r.read_utf8_string().await?;
    if stack_trace.len() > TCP_EXCEPTION_STACK_TRACE_CAP {
        tracing::warn!(
            truncated_from = stack_trace.len(),
            cap = TCP_EXCEPTION_STACK_TRACE_CAP,
            "tcp: server exception stack_trace truncated"
        );
        truncate_on_char_boundary(&mut stack_trace, TCP_EXCEPTION_STACK_TRACE_CAP);
    }
    let _obsolete_has_nested = r.read_u8().await?;
    Ok(Exception {
        code,
        name,
        message,
        stack_trace,
    })
}

/// Read a Progress packet. Field set widens with the negotiated
/// revision, matching cpp `case ServerCodes::Progress` 735-764:
///
/// - `rows_read`, `bytes_read` are always present.
/// - `total_rows_to_read` is sent at revisions >=
///   [`DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS`] (51554).
/// - `written_rows`, `written_bytes` are sent at revisions >=
///   [`DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO`] (54420).
///
/// Omitted fields read back as zero.
pub(crate) async fn read_progress<R: ClickHouseRead>(
    r: &mut R,
    server_revision: u64,
) -> Result<Progress> {
    // The server gates each field on the revision WE advertised
    // (`ProgressValues::write`), so read on the effective one.
    let effective = server_revision.min(DBMS_TCP_PROTOCOL_VERSION);
    let rows_read = r.read_var_uint().await?;
    let bytes_read = r.read_var_uint().await?;
    let total_rows_to_read = if effective >= DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS {
        r.read_var_uint().await?
    } else {
        0
    };
    // Field order is the server's, which is not the order of the gates:
    // total_bytes precedes the write info despite a higher gate, and elapsed
    // time trails it despite a lower one.
    if effective >= DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS {
        let _total_bytes_to_read = r.read_var_uint().await?;
    }
    let (written_rows, written_bytes) = if effective >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO {
        let wr = r.read_var_uint().await?;
        let wb = r.read_var_uint().await?;
        (wr, wb)
    } else {
        (0, 0)
    };
    if effective >= DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS {
        let _elapsed_ns = r.read_var_uint().await?;
    }
    Ok(Progress {
        rows_read,
        bytes_read,
        total_rows_to_read,
        written_rows,
        written_bytes,
    })
}

/// Read a ProfileInfo packet. Five varints followed by a one-byte
/// `applied_limit` flag. Mirrors cpp `case ServerCodes::ProfileInfo`
/// 706-732.
///
/// cpp reads the trailing `calculated_rows_before_limit` flag too;
/// it is not surfaced through this client's [`ProfileInfo`] struct
/// and is discarded after read so the stream pointer advances
/// correctly to the next packet.
pub(crate) async fn read_profile_info<R: ClickHouseRead>(
    r: &mut R,
    server_revision: u64,
) -> Result<ProfileInfo> {
    let rows = r.read_var_uint().await?;
    let blocks = r.read_var_uint().await?;
    let bytes = r.read_var_uint().await?;
    let applied_limit = r.read_u8().await? != 0;
    let rows_before_limit = r.read_var_uint().await?;
    // calculated_rows_before_limit -- discarded, see rustdoc above.
    let _ = r.read_u8().await?;
    if server_revision.min(DBMS_TCP_PROTOCOL_VERSION)
        >= DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION
    {
        let _applied_aggregation = r.read_u8().await?;
        let _rows_before_aggregation = r.read_var_uint().await?;
    }
    Ok(ProfileInfo {
        rows,
        blocks,
        bytes,
        applied_limit,
        rows_before_limit,
    })
}

/// Read a TableColumns packet -- two length-prefixed strings:
/// the external-table name (empty for default INSERT target)
/// followed by the columns-definition DDL fragment.
pub(crate) async fn read_table_columns<R: ClickHouseRead>(r: &mut R) -> Result<TableColumns> {
    let external_table_name = r.read_utf8_string().await?;
    let columns_definition = r.read_utf8_string().await?;
    Ok(TableColumns {
        external_table_name,
        columns_definition,
    })
}

/// Read the Data block header -- `(table_name, num_columns,
/// num_rows)`. Mirrors cpp `SendData()` field order on the client side
/// and `ReadBlock()` for the block-info section (853-887).
///
/// `num_columns` is bounded by [`MAX_BLOCK_COLUMNS`] here, at the single
/// point both body readers get it from: the schema path reserves that
/// many `(name, type_name)` pairs and [`decode_block`] sizes its own
/// vectors from it infallibly.
///
/// The caller consumes the body: [`read_empty_data_block_schema`] for a
/// schema block, [`decode_block`] for a payload block.
async fn read_data_block_header<R: ClickHouseRead>(
    r: &mut R,
    server_revision: u64,
) -> Result<(Option<String>, u64, u64)> {
    let table_name = if server_revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES {
        Some(r.read_utf8_string().await?)
    } else {
        None
    };

    if server_revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO {
        crate::native::decode::read_block_info(r).await?;
    }

    let num_columns = r.read_var_uint().await?;
    if num_columns > MAX_BLOCK_COLUMNS {
        return Err(Error::BadResponse(format!(
            "tcp: block header declares {num_columns} columns, above the \
             {MAX_BLOCK_COLUMNS} cap"
        )));
    }
    let num_rows = r.read_var_uint().await?;
    Ok((table_name, num_columns, num_rows))
}

/// Read the body of an empty Data block (num_rows == 0) -- the
/// `num_columns` pairs of `(name, type_name)` strings, plus the
/// custom-serialization flag byte per column at and above revision
/// 54454. Mirrors the shape [`crate::native::encode_columns`] produces,
/// and lets the reader advance past a schema block without running the
/// full Native decoder.
///
/// [`read_data_block_header`] has already bounded `num_columns` by
/// [`MAX_BLOCK_COLUMNS`]; the fallible reserve below is what keeps this
/// safe when it is called with an unchecked count.
///
/// # Errors
///
/// [`Error::BadResponse`] when the allocation for `num_columns` cannot be
/// reserved, or when a column carries a non-zero custom-serialization
/// flag. I/O errors from the underlying reader otherwise. The
/// `(name, type_name)` strings inherit the `MAX_STRING_SIZE` cap from
/// [`ClickHouseRead::read_utf8_string`], so a corrupt or malicious schema
/// block cannot OOM the client.
pub(crate) async fn read_empty_data_block_schema<R: ClickHouseRead>(
    r: &mut R,
    num_columns: u64,
    server_revision: u64,
) -> Result<Vec<(String, String)>> {
    let has_custom_ser = server_revision
        >= crate::native::encode::DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION;
    // Reserve fallibly: the count is peer-supplied, and an infallible
    // `with_capacity` aborts the process instead of erroring.
    let capacity = usize::try_from(num_columns).unwrap_or(usize::MAX);
    let mut out: Vec<(String, String)> = Vec::new();
    out.try_reserve(capacity)
        .map_err(|e| Error::BadResponse(format!("tcp: cannot reserve {capacity} columns: {e}")))?;
    for _ in 0..num_columns {
        let name = r.read_utf8_string().await?;
        let type_name = r.read_utf8_string().await?;
        if has_custom_ser {
            // 0 = normal serialisation. A non-zero flag means the column
            // body is framed differently, so reading it as normal would
            // misalign every following packet.
            let flag = r.read_u8().await?;
            if flag != 0 {
                return Err(Error::BadResponse(format!(
                    "tcp: column '{name}' uses custom serialization flag {flag} \
                     -- only normal (0) is supported"
                )));
            }
        }
        out.push((name, type_name));
    }
    Ok(out)
}

/// Read and discard a server telemetry block (`Log` / `ProfileEvents`).
///
/// Both packets are framed as one leading length-prefixed string (the
/// log tag / host name) followed by a Native block, which cpp-client's
/// `ReceivePacket` handles with `SkipString` + `ReadBlock` and
/// clickhouse-go reads-and-drops. [`read_data_block_header`] consumes
/// the leading string in its table-name slot, valid because this client
/// always negotiates at or above revision 50264. The bytes must be read
/// or the next packet's leading varuint misaligns.
async fn consume_telemetry_block<R: ClickHouseRead>(r: &mut R, server_revision: u64) -> Result<()> {
    let (_tag, num_columns, num_rows) = read_data_block_header(r, server_revision).await?;
    if num_rows == 0 {
        let _ = read_empty_data_block_schema(r, num_columns, server_revision).await?;
    } else {
        let _ = decode_block(r, num_columns, num_rows, server_revision).await?;
    }
    Ok(())
}

/// Dispatch a single server packet. Reads the leading varint
/// packet ID and dispatches to the per-packet decoder. Unknown
/// IDs return [`Error::BadResponse`] via
/// [`ServerPacketId::from_u64`].
///
/// For `Data` packets with `num_rows == 0` (the INSERT schema
/// block) the body's `(name, type_name)` pairs are consumed via
/// [`read_empty_data_block_schema`] and exposed in
/// `ServerPacket::Data::columns`. For `num_rows > 0` the column-
/// bytes payload is consumed inline via
/// [`crate::native::decode::decode_block`] and surfaced as
/// [`ServerPacket::DataBlock`].
pub(crate) async fn read_packet<R: ClickHouseRead>(
    r: &mut R,
    server_revision: u64,
) -> Result<ServerPacket> {
    let packet_type = r.read_var_uint().await?;
    let id = ServerPacketId::from_u64(packet_type)?;
    match id {
        // Totals (WITH TOTALS) and Extremes (extremes=1) are Native
        // result blocks framed identically to Data (cpp `ReceivePacket`
        // + clickhouse-go decode them the same way). They decode through
        // the Data path and carry their packet's kind, so a reader can
        // keep them apart from the result rows.
        ServerPacketId::Data | ServerPacketId::Totals | ServerPacketId::Extremes => {
            let (table_name, num_columns, num_rows) =
                read_data_block_header(r, server_revision).await?;
            if num_rows == 0 {
                let columns = read_empty_data_block_schema(r, num_columns, server_revision).await?;
                Ok(ServerPacket::Data {
                    table_name,
                    num_columns,
                    num_rows,
                    columns,
                })
            } else {
                let mut block = decode_block(r, num_columns, num_rows, server_revision).await?;
                block.kind = match id {
                    ServerPacketId::Totals => BlockKind::Totals,
                    ServerPacketId::Extremes => BlockKind::Extremes,
                    _ => BlockKind::Data,
                };
                Ok(ServerPacket::DataBlock(block))
            }
        }
        ServerPacketId::Exception => Ok(ServerPacket::Exception(read_exception(r).await?)),
        ServerPacketId::Progress => Ok(ServerPacket::Progress(
            read_progress(r, server_revision).await?,
        )),
        ServerPacketId::ProfileInfo => Ok(ServerPacket::ProfileInfo(
            read_profile_info(r, server_revision).await?,
        )),
        ServerPacketId::Pong => Ok(ServerPacket::Pong),
        ServerPacketId::EndOfStream => Ok(ServerPacket::EndOfStream),
        ServerPacketId::Log => {
            // Sent whenever the caller set `send_logs_level`, which apps
            // may set globally, so rejecting the block would poison
            // every query under that setting.
            consume_telemetry_block(r, server_revision).await?;
            Ok(ServerPacket::Log)
        }
        ServerPacketId::TableColumns => {
            Ok(ServerPacket::TableColumns(read_table_columns(r).await?))
        }
        ServerPacketId::ProfileEvents => {
            // Sent during normal query execution (rev >= 54451, always
            // negotiated). Same string + Native block framing as Log.
            consume_telemetry_block(r, server_revision).await?;
            Ok(ServerPacket::ProfileEvents)
        }
        ServerPacketId::TimezoneUpdate => {
            Ok(ServerPacket::TimezoneUpdate(r.read_utf8_string().await?))
        }
        // A mid-stream Hello is a protocol surprise; surface it rather
        // than advance the stream pointer past unknown payload bytes.
        other @ ServerPacketId::Hello => Err(Error::BadResponse(format!(
            "tcp: unexpected server packet {other:?} mid-stream"
        ))),
    }
}

#[cfg(test)]
mod tests {
    // The fuzz driver's PRNG: every draw is immediately taken modulo a small
    // bound, so narrowing it is the point rather than a hazard.
    #![allow(clippy::cast_possible_truncation)]

    use super::*;
    use crate::native::io::ClickHouseWrite;
    use crate::tcp::protocol::DBMS_TCP_PROTOCOL_VERSION;
    use std::io::Cursor;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn hello_writer_reader_roundtrip() {
        let mut buf = Vec::new();
        buf.write_var_uint(ServerPacketId::Hello as u64)
            .await
            .unwrap();
        buf.write_string("ClickHouse server".as_bytes())
            .await
            .unwrap();
        buf.write_var_uint(25).await.unwrap();
        buf.write_var_uint(4).await.unwrap();
        buf.write_var_uint(DBMS_TCP_PROTOCOL_VERSION).await.unwrap();
        // Every gated field the advertised revision clears, in the server's
        // write order rather than the order of the gates.
        buf.write_var_uint(0).await.unwrap();
        buf.write_string("Etc/UTC".as_bytes()).await.unwrap();
        buf.write_string("ch-01".as_bytes()).await.unwrap();
        buf.write_var_uint(7).await.unwrap();
        buf.write_string("notchunked".as_bytes()).await.unwrap();
        buf.write_string("notchunked".as_bytes()).await.unwrap();
        buf.write_var_uint(0).await.unwrap();
        buf.write_all(&[0u8; 8]).await.unwrap();
        let mut cur = Cursor::new(buf);
        let hello = read_hello(&mut cur).await.unwrap();
        assert_eq!(hello.server_name, "ClickHouse server");
        assert_eq!(hello.version, (25, 4, 7));
        assert_eq!(hello.revision, DBMS_TCP_PROTOCOL_VERSION);
        assert_eq!(hello.timezone.as_deref(), Some("Etc/UTC"));
        assert_eq!(hello.display_name.as_deref(), Some("ch-01"));
    }

    #[tokio::test]
    async fn exception_truncates_stack_trace_at_cap() {
        let big = "x".repeat(TCP_EXCEPTION_STACK_TRACE_CAP + 1024);
        let mut buf = Vec::new();
        buf.write_i32_le(100i32).await.unwrap();
        buf.write_string("DB::Exception".as_bytes()).await.unwrap();
        buf.write_string("msg".as_bytes()).await.unwrap();
        buf.write_string(big.as_bytes()).await.unwrap();
        buf.write_u8(0).await.unwrap();
        let mut cur = Cursor::new(buf);
        let exc = read_exception(&mut cur).await.unwrap();
        assert_eq!(exc.code, 100);
        assert_eq!(exc.name, "DB::Exception");
        assert_eq!(exc.message, "msg");
        assert_eq!(exc.stack_trace.len(), TCP_EXCEPTION_STACK_TRACE_CAP);
    }

    /// A server stack trace is arbitrary UTF-8, so the per-frame cap has
    /// to cut on a character boundary or the read panics.
    #[tokio::test]
    async fn exception_stack_trace_truncation_is_char_boundary_safe() {
        // 1 << 20 is not divisible by 3, so a run of three-byte
        // characters guarantees the cap lands mid-character.
        let big = "\u{20ac}".repeat(TCP_EXCEPTION_STACK_TRACE_CAP);
        assert!(!big.is_char_boundary(TCP_EXCEPTION_STACK_TRACE_CAP));
        let mut buf = Vec::new();
        buf.write_i32_le(100i32).await.unwrap();
        buf.write_string("DB::Exception".as_bytes()).await.unwrap();
        buf.write_string("msg".as_bytes()).await.unwrap();
        buf.write_string(big.as_bytes()).await.unwrap();
        buf.write_u8(0).await.unwrap();
        let mut cur = Cursor::new(buf);
        let exc = read_exception(&mut cur).await.unwrap();
        assert!(exc.stack_trace.len() <= TCP_EXCEPTION_STACK_TRACE_CAP);
        assert!(exc.stack_trace.len() > TCP_EXCEPTION_STACK_TRACE_CAP - 3);
    }

    /// The `has_nested` byte is obsolete on both sides: the server
    /// hardcodes it false (`WriteHelpers.cpp:91-92`) and its own reader
    /// ignores it (`ReadHelpers.cpp:1964,1970`). Reading it and stopping
    /// is what keeps a peer-supplied byte from steering recursion, and
    /// the trailing sentinel proves the stream stays aligned.
    #[tokio::test]
    async fn read_exception_ignores_the_obsolete_nested_flag() {
        let mut buf = Vec::new();
        buf.write_i32_le(60i32).await.unwrap();
        buf.write_string(b"DB::Exception").await.unwrap();
        buf.write_string(b"outer").await.unwrap();
        buf.write_string(b"").await.unwrap();
        buf.write_u8(1).await.unwrap(); // obsolete has_nested = true
        buf.write_var_uint(ServerPacketId::EndOfStream as u64)
            .await
            .unwrap();

        let mut cur = Cursor::new(buf);
        let exc = read_exception(&mut cur)
            .await
            .expect("the obsolete flag must not change the frame shape");
        assert_eq!(exc.code, 60);
        assert_eq!(exc.message, "outer");

        let trailing = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        assert!(matches!(trailing, ServerPacket::EndOfStream));
    }

    /// `num_columns` is an unbounded varuint from the peer that both
    /// block bodies size allocations from, and `Vec::with_capacity`
    /// aborts rather than erroring, so the header read rejects it before
    /// either body is entered -- the packet is truncated after
    /// `num_columns`, which only parses if nothing reads past it.
    #[tokio::test]
    async fn read_packet_rejects_an_absurd_column_count() {
        let mut buf = Vec::new();
        buf.write_var_uint(ServerPacketId::Data as u64)
            .await
            .unwrap();
        buf.write_string(b"").await.unwrap();
        buf.write_var_uint(1).await.unwrap();
        buf.write_u8(0).await.unwrap();
        buf.write_var_uint(2).await.unwrap();
        buf.write_i32_le(-1).await.unwrap();
        buf.write_var_uint(0).await.unwrap();
        buf.write_var_uint(1 << 60).await.unwrap(); // num_columns

        let mut cur = Cursor::new(buf);
        let err = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .expect_err("an absurd column count must be refused, not allocated");
        match err {
            Error::BadResponse(msg) => assert!(msg.contains("above the"), "got {msg}"),
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }

    /// Called with an unchecked count, the schema reader still errors
    /// rather than aborting the process on the allocation.
    #[tokio::test]
    async fn read_empty_data_block_schema_reserves_fallibly() {
        let mut cur = Cursor::new(Vec::new());
        let err = read_empty_data_block_schema(&mut cur, 1 << 60, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .expect_err("an absurd column count must not be reserved infallibly");
        match err {
            Error::BadResponse(msg) => assert!(msg.contains("cannot reserve"), "got {msg}"),
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }

    /// A non-zero custom-serialization flag means the column body is
    /// framed differently, so reading on would misalign the stream.
    /// Matches `decode_block`'s handling of the same byte.
    #[tokio::test]
    async fn read_empty_data_block_schema_rejects_custom_serialization() {
        let mut buf = Vec::new();
        buf.write_string(b"n").await.unwrap();
        buf.write_string(b"UInt64").await.unwrap();
        buf.write_u8(1).await.unwrap();
        let mut cur = Cursor::new(buf);
        let err = read_empty_data_block_schema(&mut cur, 1, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .expect_err("a non-zero custom-serialization flag must be refused");
        match err {
            Error::BadResponse(msg) => {
                assert!(msg.contains("custom serialization flag 1"), "got {msg}");
            }
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }

    /// xorshift64*, so the property test needs no dev-dependency and
    /// reproduces exactly from its seed.
    struct Prng(u64);

    impl Prng {
        fn next_u64(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        fn next_byte(&mut self) -> u8 {
            (self.next_u64() >> 33) as u8
        }
    }

    /// `read_hello` is the most exposed reader in the crate: it runs before
    /// anything has authenticated the peer, so its whole input is hostile by
    /// default. Raising the advertised revision to 54473 widened it -- a
    /// parallel-replicas varuint, two chunked-protocol strings, a
    /// peer-counted password-complexity list and an 8-byte nonce -- and the
    /// list's count is the one field that sizes a loop from a number the peer
    /// chose. `MAX_PASSWORD_COMPLEXITY_RULES` bounds it; this proves the
    /// bound holds for inputs nobody thought to write by hand.
    #[tokio::test]
    async fn read_hello_survives_garbage_and_truncation() {
        let mut good = Vec::new();
        good.write_string(b"ClickHouse server").await.unwrap();
        good.write_var_uint(25).await.unwrap();
        good.write_var_uint(4).await.unwrap();
        good.write_var_uint(DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        good.write_var_uint(0).await.unwrap();
        good.write_string(b"Etc/UTC").await.unwrap();
        good.write_string(b"ch-01").await.unwrap();
        good.write_var_uint(7).await.unwrap();
        good.write_string(b"notchunked").await.unwrap();
        good.write_string(b"notchunked").await.unwrap();
        good.write_var_uint(0).await.unwrap();
        good.write_all(&[0u8; 8]).await.unwrap();

        let mut rng = Prng(0x2545_F491_4F6C_DD1D);
        for case in 0..4096u32 {
            let mut bytes = good.clone();
            match case % 4 {
                0 => {
                    let cut = (rng.next_u64() as usize) % (bytes.len() + 1);
                    bytes.truncate(cut);
                }
                1 => {
                    let at = (rng.next_u64() as usize) % bytes.len();
                    bytes[at] = rng.next_byte();
                }
                // Corrupt the complexity-rule count specifically, which is
                // where a peer-supplied number reaches a loop bound.
                2 => {
                    let at = bytes.len() - 9;
                    for b in bytes.iter_mut().skip(at).take(4) {
                        *b = rng.next_byte() | 0x80;
                    }
                }
                _ => {
                    let len = (rng.next_u64() as usize) % 96;
                    bytes = (0..len).map(|_| rng.next_byte()).collect();
                }
            }
            let mut cur = Cursor::new(bytes);
            // Either outcome is fine; a panic, a hang or an abort is not.
            let _ = read_hello(&mut cur).await;
        }
    }

    /// `read_packet` faces bytes from an unauthenticated peer until the
    /// handshake completes and from a possibly-buggy one after, so it
    /// must return `Ok` or `Err` for any input -- never panic, and never
    /// abort on an allocation.
    #[tokio::test]
    async fn read_packet_survives_garbage_and_truncation() {
        // A well-formed schema-block packet, mutated below.
        let mut good = Vec::new();
        good.write_var_uint(ServerPacketId::Data as u64)
            .await
            .unwrap();
        good.write_string(b"").await.unwrap();
        good.write_var_uint(1).await.unwrap();
        good.write_u8(0).await.unwrap();
        good.write_var_uint(2).await.unwrap();
        good.write_i32_le(-1).await.unwrap();
        good.write_var_uint(0).await.unwrap();
        good.write_var_uint(2).await.unwrap();
        good.write_var_uint(0).await.unwrap();
        good.write_string(b"n").await.unwrap();
        good.write_string(b"UInt64").await.unwrap();
        good.write_u8(0).await.unwrap();
        good.write_string(b"s").await.unwrap();
        good.write_string(b"String").await.unwrap();
        good.write_u8(0).await.unwrap();

        let mut rng = Prng(0x5DEE_CE66_D1CE_B00D);
        for case in 0..4096u32 {
            let mut bytes = good.clone();
            match case % 4 {
                // Truncation at an arbitrary point.
                0 => {
                    let cut = (rng.next_u64() as usize) % (bytes.len() + 1);
                    bytes.truncate(cut);
                }
                // A single flipped byte.
                1 => {
                    let at = (rng.next_u64() as usize) % bytes.len();
                    bytes[at] = rng.next_byte();
                }
                // A corrupt prefix over the header fields.
                2 => {
                    let n = 1 + (rng.next_u64() as usize) % 12;
                    for b in bytes.iter_mut().take(n) {
                        *b = rng.next_byte();
                    }
                }
                // Wholly random input of a random length.
                _ => {
                    let len = (rng.next_u64() as usize) % 64;
                    bytes = (0..len).map(|_| rng.next_byte()).collect();
                }
            }
            let mut cur = Cursor::new(bytes);
            // Either outcome is fine; a panic or an abort is not.
            let _ = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION).await;
        }
    }

    #[tokio::test]
    async fn progress_revision_gating_legacy() {
        // Legacy: only rows_read + bytes_read on the wire.
        let mut buf = Vec::new();
        buf.write_var_uint(100).await.unwrap();
        buf.write_var_uint(2048).await.unwrap();
        let mut cur = Cursor::new(buf);
        // Revision below the total-rows gate.
        let legacy_revision = DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS - 1;
        let p = read_progress(&mut cur, legacy_revision).await.unwrap();
        assert_eq!(p.rows_read, 100);
        assert_eq!(p.bytes_read, 2048);
        assert_eq!(p.total_rows_to_read, 0);
        assert_eq!(p.written_rows, 0);
        assert_eq!(p.written_bytes, 0);
    }

    #[tokio::test]
    async fn progress_revision_gating_modern() {
        let mut buf = Vec::new();
        buf.write_var_uint(100).await.unwrap(); // rows_read
        buf.write_var_uint(2048).await.unwrap(); // bytes_read
        buf.write_var_uint(10_000).await.unwrap(); // total_rows_to_read
        buf.write_var_uint(65_536).await.unwrap(); // total_bytes_to_read
        buf.write_var_uint(50).await.unwrap(); // written_rows
        buf.write_var_uint(1024).await.unwrap(); // written_bytes
        buf.write_var_uint(12_345).await.unwrap(); // elapsed_ns
        let mut cur = Cursor::new(buf);
        let p = read_progress(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        assert_eq!(p.rows_read, 100);
        assert_eq!(p.bytes_read, 2048);
        assert_eq!(p.total_rows_to_read, 10_000);
        assert_eq!(p.written_rows, 50);
        assert_eq!(p.written_bytes, 1024);
    }

    #[tokio::test]
    async fn profile_info_roundtrip() {
        let mut buf = Vec::new();
        buf.write_var_uint(1000).await.unwrap(); // rows
        buf.write_var_uint(5).await.unwrap(); // blocks
        buf.write_var_uint(32768).await.unwrap(); // bytes
        buf.write_u8(1).await.unwrap(); // applied_limit = true
        buf.write_var_uint(500).await.unwrap(); // rows_before_limit
        buf.write_u8(0).await.unwrap(); // calculated_rows_before_limit (discarded)
        buf.write_u8(1).await.unwrap(); // applied_aggregation (discarded)
        buf.write_var_uint(250).await.unwrap(); // rows_before_aggregation (discarded)
        let mut cur = Cursor::new(buf);
        let pi = read_profile_info(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        assert_eq!(pi.rows, 1000);
        assert_eq!(pi.blocks, 5);
        assert_eq!(pi.bytes, 32768);
        assert!(pi.applied_limit);
        assert_eq!(pi.rows_before_limit, 500);
    }

    #[tokio::test]
    async fn table_columns_roundtrip() {
        let mut buf = Vec::new();
        buf.write_string("".as_bytes()).await.unwrap();
        buf.write_string(
            "columns format version: 1\n2 columns:\n`a` Int32\n`b` String\n".as_bytes(),
        )
        .await
        .unwrap();
        let mut cur = Cursor::new(buf);
        let tc = read_table_columns(&mut cur).await.unwrap();
        assert_eq!(tc.external_table_name, "");
        assert!(tc.columns_definition.starts_with("columns format version"));
    }

    #[tokio::test]
    async fn read_packet_dispatches_pong() {
        let mut buf = Vec::new();
        buf.write_var_uint(ServerPacketId::Pong as u64)
            .await
            .unwrap();
        let mut cur = Cursor::new(buf);
        let pkt = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        assert!(matches!(pkt, ServerPacket::Pong));
    }

    #[tokio::test]
    async fn read_packet_dispatches_end_of_stream() {
        let mut buf = Vec::new();
        buf.write_var_uint(ServerPacketId::EndOfStream as u64)
            .await
            .unwrap();
        let mut cur = Cursor::new(buf);
        let pkt = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        assert!(matches!(pkt, ServerPacket::EndOfStream));
    }

    #[tokio::test]
    async fn read_packet_consumes_empty_data_block_schema() {
        // The server's INSERT schema block: Data packet, table_name = "",
        // block-info, num_columns = 2, num_rows = 0, then per-column
        // (name, type_name, custom_ser_flag).
        let mut buf = Vec::new();
        buf.write_var_uint(ServerPacketId::Data as u64)
            .await
            .unwrap();
        buf.write_string(b"").await.unwrap(); // table_name
        // Block info -- mirror the writer side (field_id, value) pairs + terminator.
        buf.write_var_uint(1).await.unwrap();
        buf.write_u8(0).await.unwrap();
        buf.write_var_uint(2).await.unwrap();
        buf.write_i32_le(-1).await.unwrap();
        buf.write_var_uint(0).await.unwrap();
        buf.write_var_uint(2).await.unwrap(); // num_columns
        buf.write_var_uint(0).await.unwrap(); // num_rows
        // Column 1.
        buf.write_string(b"n").await.unwrap();
        buf.write_string(b"UInt64").await.unwrap();
        buf.write_u8(0).await.unwrap(); // custom-serialization flag
        // Column 2.
        buf.write_string(b"s").await.unwrap();
        buf.write_string(b"String").await.unwrap();
        buf.write_u8(0).await.unwrap();
        // Trailing sentinel so an over-read would show up as misalignment.
        buf.write_var_uint(ServerPacketId::EndOfStream as u64)
            .await
            .unwrap();

        let mut cur = Cursor::new(buf);
        let pkt = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        match pkt {
            ServerPacket::Data {
                table_name,
                num_columns,
                num_rows,
                columns,
            } => {
                assert_eq!(table_name.as_deref(), Some(""));
                assert_eq!(num_columns, 2);
                assert_eq!(num_rows, 0);
                assert_eq!(
                    columns,
                    vec![
                        ("n".to_string(), "UInt64".to_string()),
                        ("s".to_string(), "String".to_string()),
                    ]
                );
            }
            other => panic!("expected Data, got {other:?}"),
        }
        // The trailing EndOfStream must still be readable -- proves the
        // schema-block consume left the stream pointer aligned.
        let trailing = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        assert!(matches!(trailing, ServerPacket::EndOfStream));
    }

    #[tokio::test]
    async fn read_packet_consumes_profile_events_block() {
        // ProfileEvents framing: packet id, a leading host/tag string,
        // then a Native block (block-info, num_columns, num_rows,
        // per-column name/type/flag/data). The reader must consume the
        // whole thing so the next packet stays aligned. One UInt64
        // column, one row, value 42.
        let mut buf = Vec::new();
        buf.write_var_uint(ServerPacketId::ProfileEvents as u64)
            .await
            .unwrap();
        buf.write_string(b"host-01").await.unwrap(); // leading tag
        // Block info.
        buf.write_var_uint(1).await.unwrap();
        buf.write_u8(0).await.unwrap();
        buf.write_var_uint(2).await.unwrap();
        buf.write_i32_le(-1).await.unwrap();
        buf.write_var_uint(0).await.unwrap();
        buf.write_var_uint(1).await.unwrap(); // num_columns
        buf.write_var_uint(1).await.unwrap(); // num_rows
        buf.write_string(b"value").await.unwrap(); // col name
        buf.write_string(b"UInt64").await.unwrap(); // type
        buf.write_u8(0).await.unwrap(); // custom-serialization flag
        buf.write_u64_le(42).await.unwrap(); // the one row's value
        // Trailing sentinel.
        buf.write_var_uint(ServerPacketId::EndOfStream as u64)
            .await
            .unwrap();

        let mut cur = Cursor::new(buf);
        let pkt = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        assert!(matches!(pkt, ServerPacket::ProfileEvents));
        // The trailing EndOfStream reads cleanly only if the whole
        // ProfileEvents block was consumed.
        let trailing = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        assert!(matches!(trailing, ServerPacket::EndOfStream));
    }

    #[tokio::test]
    async fn read_packet_consumes_log_block() {
        // A Log packet is framed exactly like ProfileEvents (leading tag
        // string + Native block) and must be consumed so the stream
        // stays aligned -- an app that set send_logs_level would
        // otherwise poison every TCP query. One String column, one row.
        let mut buf = Vec::new();
        buf.write_var_uint(ServerPacketId::Log as u64)
            .await
            .unwrap();
        buf.write_string(b"log-tag").await.unwrap(); // leading tag
        // Block info.
        buf.write_var_uint(1).await.unwrap();
        buf.write_u8(0).await.unwrap();
        buf.write_var_uint(2).await.unwrap();
        buf.write_i32_le(-1).await.unwrap();
        buf.write_var_uint(0).await.unwrap();
        buf.write_var_uint(1).await.unwrap(); // num_columns
        buf.write_var_uint(1).await.unwrap(); // num_rows
        buf.write_string(b"text").await.unwrap(); // col name
        buf.write_string(b"String").await.unwrap(); // type
        buf.write_u8(0).await.unwrap(); // custom-serialization flag
        buf.write_string(b"hello from server").await.unwrap(); // the row value
        // Trailing sentinel proves the block was fully consumed.
        buf.write_var_uint(ServerPacketId::EndOfStream as u64)
            .await
            .unwrap();

        let mut cur = Cursor::new(buf);
        let pkt = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        assert!(matches!(pkt, ServerPacket::Log));
        let trailing = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        assert!(matches!(trailing, ServerPacket::EndOfStream));
    }

    #[tokio::test]
    async fn read_packet_decodes_totals_block_then_eos() {
        // A `WITH TOTALS` query emits a Totals packet (id 7) framed
        // exactly like Data: table_name, block-info, num_columns,
        // num_rows, then the column payload. The reader must decode it
        // (as a DataBlock) so the query does not poison the connection,
        // and leave the stream aligned for the trailing EndOfStream.
        // One UInt64 column, one totals row = 99.
        let mut buf = Vec::new();
        buf.write_var_uint(ServerPacketId::Totals as u64)
            .await
            .unwrap();
        buf.write_string(b"").await.unwrap(); // table_name
        buf.write_var_uint(1).await.unwrap(); // block-info field 1
        buf.write_u8(0).await.unwrap();
        buf.write_var_uint(2).await.unwrap(); // block-info field 2
        buf.write_i32_le(-1).await.unwrap();
        buf.write_var_uint(0).await.unwrap(); // terminator
        buf.write_var_uint(1).await.unwrap(); // num_columns
        buf.write_var_uint(1).await.unwrap(); // num_rows
        buf.write_string(b"total").await.unwrap(); // col name
        buf.write_string(b"UInt64").await.unwrap(); // type
        buf.write_u8(0).await.unwrap(); // custom-serialization flag
        buf.write_u64_le(99).await.unwrap(); // the totals row
        buf.write_var_uint(ServerPacketId::EndOfStream as u64)
            .await
            .unwrap();

        let mut cur = Cursor::new(buf);
        let pkt = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        match pkt {
            ServerPacket::DataBlock(block) => {
                assert_eq!(block.num_rows, 1);
                assert_eq!(block.kind, BlockKind::Totals, "the packet's kind is kept");
                match &block.columns[0] {
                    crate::native::decode::DecodedColumn::UInt64(v) => assert_eq!(v, &vec![99u64]),
                    other => panic!("expected UInt64 totals, got {other:?}"),
                }
            }
            other => panic!("expected DataBlock for Totals, got {other:?}"),
        }
        let trailing = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        assert!(matches!(trailing, ServerPacket::EndOfStream));
    }
}
