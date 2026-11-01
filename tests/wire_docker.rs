// Project:   dfe-clickhouse
// File:      tests/wire_docker.rs
// Purpose:   Native wire round trip against a pinned ClickHouse in Docker
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Type-matrix proof against a real server, not a mock.
//!
//! One container per test, pinned to a fixed server version and stopped
//! before the test returns. The server builds every value, so a
//! failure here is this crate's decoder rather than its encoder; the encoder
//! is covered separately, and by one test that uses the server as its oracle.
//!
//! None of these is `#[ignore]`d. `hyperi-ci` runs `cargo nextest` with no way
//! to pass `--run-ignored`, so an ignored test here is one CI never runs.
//! Without a container runtime they fail under `$CI` and skip with a message
//! anywhere else.
//!
//! ```text
//! cargo test --all-features --test wire_docker
//! ```
//!
//! A server already running takes the place of the per-test container: set
//! `DFE_CH_WIRE_NATIVE_PORT`, `DFE_CH_WIRE_HTTP_PORT`,
//! `DFE_CH_WIRE_SECURE_PORT`, `DFE_CH_WIRE_TLS_CA` (the PEM of a CA that
//! signed its certificate, valid for `localhost` alone), and
//! `DFE_CH_WIRE_HOST` when it is not `127.0.0.1`. [`TLS_CONFIG`] is the
//! server-side TLS such a server needs.

#![cfg(all(feature = "tcp", feature = "unified", feature = "dynamic"))]
// Helpers sit outside #[test], so clippy's in-test exemption misses them.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;
use std::time::Duration;

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, CopyTargetOptions, GenericImage, ImageExt};
use tokio::sync::Semaphore;

use clickhouse::Client;
use dfe_clickhouse::native::DecodedBlock;
use dfe_clickhouse::{Columns, TcpClient, UnifiedClient};

/// Pinned, never `latest`: the wire format moves between server versions.
const IMAGE_TAG: &str = "26.3.21.7";

/// Enough for the matrix, far below what an unbounded server would take.
const CONTAINER_MEMORY_BYTES: i64 = 2 * 1024 * 1024 * 1024;

/// One server at a time. `cargo test` runs a binary's tests as threads in one
/// process, so this bounds them; `nextest` forks a process per test, where
/// `.config/nextest.toml`'s test-group does the same job instead.
static ONE_SERVER: Semaphore = Semaphore::const_new(1);

/// Sockets `testcontainers` will find without `DOCKER_HOST` being set.
const DOCKER_SOCKETS: &[&str] = &["/var/run/docker.sock", "/run/docker.sock"];

fn docker_endpoint_exists() -> bool {
    if std::env::var_os("DOCKER_HOST").is_some() {
        return true;
    }
    if DOCKER_SOCKETS.iter().any(|s| Path::new(s).exists()) {
        return true;
    }
    std::env::var_os("XDG_RUNTIME_DIR").is_some_and(|dir| {
        let dir = Path::new(&dir);
        dir.join("docker.sock").exists() || dir.join("podman/podman.sock").exists()
    })
}

/// Gate every test on a container runtime: a missing endpoint fails under
/// `$CI`, so the suite cannot go missing there, and skips with a message
/// anywhere else.
macro_rules! require_docker {
    () => {
        if !docker_endpoint_exists() {
            assert!(
                std::env::var_os("CI").is_none(),
                "CI runs the wire suite: no Docker endpoint (DOCKER_HOST unset, no socket)"
            );
            eprintln!("skipped: no Docker endpoint -- start dockerd or set DOCKER_HOST");
            return;
        }
    };
}

struct Server {
    /// `None` for a server the environment names, which the run does not own.
    container: Option<ContainerAsync<GenericImage>>,
    host: String,
    native_port: u16,
    http_port: u16,
    /// Native protocol over TLS, the certificate valid for `localhost` only.
    secure_port: u16,
    /// PEM of the CA that signed the server's certificate.
    tls_ca: Vec<u8>,
}

/// Server-side TLS, read from `config.d/`, with the certificate and key in
/// `/etc/clickhouse-server/certs/`.
const TLS_CONFIG: &str = "<clickhouse>
    <https_port>8443</https_port>
    <tcp_port_secure>9440</tcp_port_secure>
    <openSSL>
        <server>
            <certificateFile>/etc/clickhouse-server/certs/server.pem</certificateFile>
            <privateKeyFile>/etc/clickhouse-server/certs/server.key</privateKeyFile>
            <verificationMode>none</verificationMode>
            <loadDefaultCAFile>false</loadDefaultCAFile>
            <disableProtocols>sslv2,sslv3,tlsv1,tlsv1_1</disableProtocols>
        </server>
    </openSSL>
</clickhouse>
";

/// A CA minted for one run and a certificate it signed for `localhost` alone,
/// as `(ca, certificate, key)` PEM.
fn mint_tls() -> (String, String, String) {
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer,
        KeyPair, KeyUsagePurpose,
    };

    let ca_key = KeyPair::generate().unwrap();
    let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    ca.distinguished_name
        .push(DnType::CommonName, "dfe-clickhouse wire test CA");
    let ca_pem = ca.self_signed(&ca_key).unwrap().pem();

    let key = KeyPair::generate().unwrap();
    let mut leaf = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    leaf.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let cert = leaf.signed_by(&key, &Issuer::new(ca, ca_key)).unwrap();
    (ca_pem, cert.pem(), key.serialize_pem())
}

/// A port from the environment, or `None` when it is unset.
fn env_port(key: &str) -> Option<u16> {
    let raw = std::env::var(key).ok()?;
    Some(
        raw.parse()
            .unwrap_or_else(|_| panic!("{key}={raw:?} is not a port")),
    )
}

/// Start a server for ONE test, ready to answer on every port.
///
/// Owned by the test and stopped when it returns. A `static` shared across
/// the binary would be faster, and was the first shape of this: a `static` is
/// never dropped at process exit, so every run left a server behind.
/// `testcontainers`' `watchdog` does not cover a normal exit either.
async fn server() -> Server {
    if let Some(native_port) = env_port("DFE_CH_WIRE_NATIVE_PORT") {
        let server = Server {
            container: None,
            host: std::env::var("DFE_CH_WIRE_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
            native_port,
            http_port: env_port("DFE_CH_WIRE_HTTP_PORT").expect("DFE_CH_WIRE_HTTP_PORT is set"),
            secure_port: env_port("DFE_CH_WIRE_SECURE_PORT")
                .expect("DFE_CH_WIRE_SECURE_PORT is set"),
            tls_ca: std::fs::read(
                std::env::var("DFE_CH_WIRE_TLS_CA").expect("DFE_CH_WIRE_TLS_CA is set"),
            )
            .expect("DFE_CH_WIRE_TLS_CA names a readable PEM file"),
        };
        await_ready(&server).await;
        return server;
    }

    // No log wait: the image writes one line to stdout and its trace to a
    // file, so no message on that stream marks readiness. Readiness is the
    // ping loop below; testcontainers' own `http_wait` would cost a reqwest
    // dependency for one poll.
    let (ca, cert, key) = mint_tls();
    let readable = |path: &str| CopyTargetOptions::new(path).with_mode(0o644);
    let container = GenericImage::new("clickhouse/clickhouse-server", IMAGE_TAG)
        .with_exposed_port(9000.tcp())
        .with_exposed_port(8123.tcp())
        .with_exposed_port(9440.tcp())
        .with_wait_for(WaitFor::Nothing)
        .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1")
        .with_copy_to(
            readable("/etc/clickhouse-server/config.d/tls.xml"),
            TLS_CONFIG.as_bytes().to_vec(),
        )
        .with_copy_to(
            readable("/etc/clickhouse-server/certs/server.pem"),
            cert.into_bytes(),
        )
        .with_copy_to(
            readable("/etc/clickhouse-server/certs/server.key"),
            key.into_bytes(),
        )
        .with_host_config_modifier(|host| host.memory = Some(CONTAINER_MEMORY_BYTES))
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .expect("the pinned ClickHouse image starts");
    let native_port = container.get_host_port_ipv4(9000).await.unwrap();
    let http_port = container.get_host_port_ipv4(8123).await.unwrap();
    let secure_port = container.get_host_port_ipv4(9440).await.unwrap();
    let server = Server {
        container: Some(container),
        host: "127.0.0.1".into(),
        native_port,
        http_port,
        secure_port,
        tls_ca: ca.into_bytes(),
    };
    await_ready(&server).await;
    server
}

impl Server {
    fn tcp(&self) -> UnifiedClient {
        UnifiedClient::Tcp(TcpClient::new(format!(
            "{}:{}",
            self.host, self.native_port
        )))
    }

    fn http(&self) -> UnifiedClient {
        UnifiedClient::Http(
            Client::default().with_url(format!("http://{}:{}", self.host, self.http_port)),
        )
    }

    /// Stop the container, if this run started one. `Drop` also stops it, but
    /// an explicit call keeps the teardown on the test's own timeline rather
    /// than a background task.
    async fn stop(self) {
        if let Some(container) = self.container {
            let _ = container.rm().await;
        }
    }
}

/// Poll until the server answers on both ports, so a test never races the
/// startup it did not wait for.
async fn await_ready(server: &Server) {
    let (http, tcp) = (server.http(), server.tcp());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut last = String::from("never attempted");
    while tokio::time::Instant::now() < deadline {
        match (http.ping().await, tcp.ping().await) {
            (Ok(()), Ok(())) => return,
            (Err(e), _) | (_, Err(e)) => last = e.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("the server never answered on both ports; last error: {last}");
}

/// One matrix row: a column type and the SQL that produces its values. The
/// server builds the values, so nothing here depends on this crate's encoder.
struct Case {
    ch_type: &'static str,
    /// SQL expressions, one per row, each already of `ch_type`.
    values: &'static [&'static str],
    /// Set when the codec is known not to read this shape yet, naming the
    /// reason. The case is then asserted to FAIL, so fixing the codec fails
    /// this test until the marker is removed -- a marker cannot rot into a
    /// silently skipped type.
    ///
    /// Empty today. It earned its keep on the nested-prefix defect: four
    /// shapes carried a marker, and each fix took one off by failing the
    /// suite rather than passing quietly.
    known_broken: Option<&'static str>,
}

/// Shorthand for a case that must round-trip.
const fn case(ch_type: &'static str, values: &'static [&'static str]) -> Case {
    Case {
        ch_type,
        values,
        known_broken: None,
    }
}

const CASES: &[Case] = &[
    case("UInt8", &["0", "255"]),
    case("UInt16", &["0", "65535"]),
    case("UInt32", &["0", "4294967295"]),
    case("UInt64", &["0", "18446744073709551615"]),
    case("UInt128", &["0", "340282366920938463463374607431768211455"]),
    case("UInt256", &["0", "12345678901234567890"]),
    case("Int8", &["-128", "127"]),
    case("Int16", &["-32768", "32767"]),
    case("Int32", &["-2147483648", "2147483647"]),
    case("Int64", &["-9223372036854775808", "9223372036854775807"]),
    case("Int128", &["-170141183460469231731687303715884105728", "0"]),
    case("Int256", &["-1", "1"]),
    case("Float32", &["-1.5", "3.25"]),
    case("Float64", &["-1.5", "3.25"]),
    case("BFloat16", &["1.5", "-2"]),
    case("Bool", &["true", "false"]),
    case("Decimal(9, 2)", &["'-1234567.89'", "'0.01'"]),
    case("Decimal(18, 4)", &["'-1.0001'", "'12345678901.2345'"]),
    case("Decimal(38, 10)", &["'-1.0000000001'", "'0'"]),
    case("Decimal(76, 20)", &["'-1.00000000000000000001'", "'0'"]),
    case("Date", &["'1970-01-01'", "'2149-06-06'"]),
    case("Date32", &["'1900-01-01'", "'2299-12-31'"]),
    case(
        "DateTime",
        &["'1970-01-01 00:00:00'", "'2106-02-07 06:28:15'"],
    ),
    case(
        "DateTime('Australia/Sydney')",
        &["'2026-09-02 08:00:00'", "'1970-01-01 10:00:00'"],
    ),
    case(
        "DateTime64(3, 'UTC')",
        &["'2026-09-02 08:00:00.123'", "'1970-01-01 00:00:00.000'"],
    ),
    case("DateTime64(9)", &["'2026-09-02 08:00:00.123456789'"]),
    case(
        "UUID",
        &["'00000000-0000-0000-0000-000000000000'", "generateUUIDv4()"],
    ),
    case("IPv4", &["'127.0.0.1'", "'255.255.255.255'"]),
    case("IPv6", &["'::1'", "'2001:db8::1'"]),
    case("Enum8('a' = 1, 'b' = -2)", &["'a'", "'b'"]),
    case("Enum16('a' = 1, 'b' = -300)", &["'a'", "'b'"]),
    // Unicode and an embedded NUL: ClickHouse Strings are bytes, not UTF-8.
    case("String", &["''", "'ünïcode'", "concat('a', char(0), 'b')"]),
    case("FixedString(4)", &["'ab'", "'abcd'"]),
    case("Nullable(UInt64)", &["NULL", "7"]),
    case("Nullable(String)", &["NULL", "'x'"]),
    case(
        "Nullable(DateTime64(3))",
        &["NULL", "'2026-09-02 08:00:00.500'"],
    ),
    case("LowCardinality(String)", &["'a'", "'b'", "'a'"]),
    case("LowCardinality(Nullable(String))", &["NULL", "'a'", "'a'"]),
    case("Array(Int32)", &["[]", "[-1, 0, 1]"]),
    case("Array(Nullable(UInt8))", &["[NULL, 1]", "[]"]),
    case("Array(Array(String))", &["[]", "[['a'], [], ['b', 'c']]"]),
    case("Array(LowCardinality(String))", &["[]", "['a', 'b', 'a']"]),
    case("Map(String, Int64)", &["map()", "map('a', 1, 'b', -2)"]),
    case("Map(String, Array(String))", &["map('a', ['x', 'y'])"]),
    case(
        "Map(String, LowCardinality(Nullable(String)))",
        &["map('a', NULL, 'b', 'v')"],
    ),
    case("Tuple(UInt8, String, Array(Int32))", &["(1, 'a', [1, 2])"]),
    case(
        "Array(Tuple(String, Map(String, UInt8)))",
        &["[('a', map('k', 1))]", "[]"],
    ),
    case("Point", &["(1.5, -2.5)"]),
    case("SimpleAggregateFunction(sum, UInt64)", &["1", "2"]),
    case("JSON", &[r#"'{"a":1,"b":{"c":"x"}}'"#, "'{}'"]),
    case("JSON", &[r#"'{"arr":[1,2,3]}'"#]),
    case(
        "Variant(UInt64, String)",
        &["'text'::String", "42::UInt64", "NULL"],
    ),
    case("Dynamic", &["'text'", "42::UInt64", "NULL"]),
    // Prefix-bearing types nested one level down. A Tuple writes no data of
    // its own before its fields, so its child's prefix lands first either
    // way and these pass; an Array's offsets come first, which is what
    // pushes the child's prefix out of place.
    case("Tuple(LowCardinality(String), UInt8)", &["('a', 1)"]),
    case("Array(JSON)", &[r#"['{"a":1}']"#, "[]"]),
    case("Array(Dynamic)", &["['x', 42::UInt64]", "[]"]),
    case("Array(Variant(UInt64, String))", &["['x'::String]", "[]"]),
];

/// The decoded shape, rendered. `DecodedColumn` is not `PartialEq`, and the
/// rendering compares the variant and its values together, which is the whole
/// claim. Every matrix case is a handful of rows, so the server sends one
/// block on either transport; a split would make the renderings differ for a
/// reason that is not a defect, so it is asserted rather than assumed.
fn rendered(columns: &Columns, name: &str) -> Result<String, String> {
    let blocks: Vec<&DecodedBlock> = columns.blocks().iter().filter(|b| b.num_rows > 0).collect();
    match blocks.as_slice() {
        [block] => match block.column(name) {
            Some(column) => Ok(format!("{column:?}")),
            None => Err(format!("block declares no column '{name}'")),
        },
        other => Err(format!("expected one payload block, got {}", other.len())),
    }
}

/// Every type in the matrix must decode to the same thing over TCP and over
/// HTTP. Both go through this crate's Native codec, so a divergence is a
/// transport-specific bug in the framing around it.
#[tokio::test]
async fn every_type_decodes_identically_on_both_transports() {
    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let (tcp, http) = (server.tcp(), server.http());
    tcp.execute("CREATE DATABASE IF NOT EXISTS wire")
        .await
        .unwrap();

    let mut failures = Vec::new();
    for (i, case) in CASES.iter().enumerate() {
        let outcome = run_case(&tcp, &http, case, i).await;
        match (outcome, case.known_broken) {
            // Behaved as declared: round-tripped, or failed and marked.
            (Ok(()), None) | (Err(_), Some(_)) => {}
            (Err(e), None) => failures.push(format!("{}: {e}", case.ch_type)),
            (Ok(()), Some(reason)) => failures.push(format!(
                "{} now round-trips -- drop its known_broken marker ({reason})",
                case.ch_type
            )),
        }
    }

    // Stop before asserting: a panic would skip the teardown.
    server.stop().await;
    assert!(
        failures.is_empty(),
        "{} cases failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Create, fill and read one case on both transports.
async fn run_case(
    tcp: &UnifiedClient,
    http: &UnifiedClient,
    case: &Case,
    index: usize,
) -> Result<(), String> {
    let table = format!("wire.t{index}");
    let create = format!(
        "CREATE OR REPLACE TABLE {table} (c {}) ENGINE = MergeTree ORDER BY tuple()",
        case.ch_type
    );
    tcp.execute(&create)
        .await
        .map_err(|e| format!("CREATE rejected: {e}"))?;

    let rows: Vec<String> = case.values.iter().map(|v| format!("({v})")).collect();
    let insert = format!("INSERT INTO {table} VALUES {}", rows.join(", "));
    tcp.execute(&insert)
        .await
        .map_err(|e| format!("INSERT rejected: {e}"))?;

    let sql = format!("SELECT c FROM {table}");
    let over_tcp = tcp
        .fetch_columns(&sql)
        .await
        .map_err(|e| format!("tcp SELECT failed: {e}"))?;
    let over_http = http
        .fetch_columns(&sql)
        .await
        .map_err(|e| format!("http SELECT failed: {e}"))?;

    let (t, h) = (rendered(&over_tcp, "c")?, rendered(&over_http, "c")?);
    if t == h {
        Ok(())
    } else {
        Err(format!("tcp {t} != http {h}"))
    }
}

/// Columns whose wire representation this test pins, as
/// `(name, ClickHouse type, row 0, row 1)`.
///
/// Left out deliberately: `UUID`, whose 16 bytes have a byte order worth
/// pinning against the server source rather than against a value guessed here,
/// and the composites, which have no flat Rust shape to compare.
const VALUE_COLUMNS: &[(&str, &str, &str, &str)] = &[
    ("u8", "UInt8", "0", "255"),
    ("u64", "UInt64", "0", "18446744073709551615"),
    (
        "u128",
        "UInt128",
        "0",
        "340282366920938463463374607431768211455",
    ),
    ("i8", "Int8", "-128", "127"),
    (
        "i64",
        "Int64",
        "-9223372036854775808",
        "9223372036854775807",
    ),
    (
        "i128",
        "Int128",
        "-170141183460469231731687303715884105728",
        "0",
    ),
    ("f64", "Float64", "-1.5", "3.25"),
    ("b", "Bool", "true", "false"),
    ("s", "String", "''", "concat('a', char(0), 'b')"),
    ("d", "Date", "'1970-01-01'", "'2149-06-06'"),
    (
        "dt",
        "DateTime('UTC')",
        "'1970-01-01 00:00:00'",
        "'2106-02-07 06:28:15'",
    ),
    (
        "dt64",
        "DateTime64(3, 'UTC')",
        "'1970-01-01 00:00:00.000'",
        "'1970-01-01 00:00:01.500'",
    ),
    ("dec32", "Decimal(9, 2)", "'-1234567.89'", "'0.01'"),
    ("dec64", "Decimal(18, 4)", "'-1.0001'", "'12345678901.2345'"),
    ("u256", "UInt256", "0", "1"),
    ("ip4", "IPv4", "'0.0.0.0'", "'127.0.0.1'"),
    ("ip6", "IPv6", "'::'", "'::1'"),
    ("e8", "Enum8('a' = 1, 'b' = -2)", "'a'", "'b'"),
];

/// The matrix asserts the two transports agree, and since
/// `client_protocol_version` put HTTP on this crate's own codec, both sides of
/// that comparison run the same decoder -- so it cannot catch a value misread
/// the same way twice.
///
/// These expectations come from the SQL literal and `ClickHouse`'s own
/// representation of it instead: days since the epoch, unscaled decimal
/// integers, network-order address bytes.
#[tokio::test]
async fn decoded_values_match_what_the_literals_asked_for() {
    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let client = server.tcp();
    client
        .execute("CREATE DATABASE IF NOT EXISTS wire")
        .await
        .unwrap();

    let columns: Vec<String> = VALUE_COLUMNS
        .iter()
        .map(|(name, ty, ..)| format!("{name} {ty}"))
        .collect();
    let first: Vec<&str> = VALUE_COLUMNS.iter().map(|(.., a, _)| *a).collect();
    let second: Vec<&str> = VALUE_COLUMNS.iter().map(|(.., b)| *b).collect();
    client
        .execute(&format!(
            "CREATE OR REPLACE TABLE wire.values_ (ord UInt8, {}) \
             ENGINE = MergeTree ORDER BY ord",
            columns.join(", ")
        ))
        .await
        .unwrap();
    client
        .execute(&format!(
            "INSERT INTO wire.values_ VALUES (0, {}), (1, {})",
            first.join(", "),
            second.join(", ")
        ))
        .await
        .unwrap();

    let got = client
        .fetch_columns("SELECT * FROM wire.values_ ORDER BY ord")
        .await;
    server.stop().await;
    let got = got.expect("the pinned-value row pair reads back");

    assert_eq!(got.get::<u8>("u8").unwrap(), [0, 255]);
    assert_eq!(got.get::<u64>("u64").unwrap(), [0, u64::MAX]);
    assert_eq!(got.get::<u128>("u128").unwrap(), [0, u128::MAX]);
    assert_eq!(got.get::<i8>("i8").unwrap(), [i8::MIN, i8::MAX]);
    assert_eq!(got.get::<i64>("i64").unwrap(), [i64::MIN, i64::MAX]);
    assert_eq!(got.get::<i128>("i128").unwrap(), [i128::MIN, 0]);
    assert_eq!(got.get::<f64>("f64").unwrap(), [-1.5, 3.25]);
    assert_eq!(got.get::<bool>("b").unwrap(), [true, false]);
    // A ClickHouse String is bytes, so the NUL survives and UTF-8 is not
    // assumed.
    assert_eq!(
        got.get::<Vec<u8>>("s").unwrap(),
        [b"".to_vec(), b"a\0b".to_vec()]
    );
    // Date is days since the epoch on a UInt16 backing; 2149-06-06 is the last
    // representable day.
    assert_eq!(got.get::<u16>("d").unwrap(), [0, u16::MAX]);
    assert_eq!(got.get::<u32>("dt").unwrap(), [0, u32::MAX]);
    // DateTime64(3) counts milliseconds.
    assert_eq!(got.get::<i64>("dt64").unwrap(), [0, 1_500]);
    // A Decimal reads as its unscaled backing integer: -1234567.89 at scale 2.
    assert_eq!(got.get::<i32>("dec32").unwrap(), [-123_456_789, 1]);
    assert_eq!(
        got.get::<i64>("dec64").unwrap(),
        [-10_001, 123_456_789_012_345]
    );
    // 256-bit values come back as raw little-endian bytes, so 1 is a low byte.
    let u256 = got.get::<[u8; 32]>("u256").unwrap();
    assert_eq!(u256[0], [0u8; 32]);
    let mut one = [0u8; 32];
    one[0] = 1;
    assert_eq!(u256[1], one);
    // IPv4 is a UInt32: 127.0.0.1 is 0x7F000001.
    assert_eq!(got.get::<u32>("ip4").unwrap(), [0, 0x7F00_0001]);
    // IPv6 is 16 bytes in network order, so ::1 sets only the last.
    let ip6 = got.get::<[u8; 16]>("ip6").unwrap();
    assert_eq!(ip6[0], [0u8; 16]);
    let mut loopback = [0u8; 16];
    loopback[15] = 1;
    assert_eq!(ip6[1], loopback);
    // An Enum8 reads as its signed ordinal, not its name.
    assert_eq!(got.get::<i8>("e8").unwrap(), [1, -2]);
}

/// The encoder faces the same prefix ordering as the decoder did. Rather than
/// assert which way round it is, write the same row twice -- once by the
/// server from SQL, once through this crate's encoder -- and require the two
/// tables to read back identically.
#[tokio::test]
async fn a_nested_low_cardinality_insert_matches_the_server_s_own() {
    use std::sync::Arc;

    use serde_json::{Map, json};

    use dfe_clickhouse::dynamic::{ColumnDef, DynamicInsert, DynamicSchema};

    const TYPE: &str = "Array(LowCardinality(String))";
    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let client = server.tcp();
    client
        .execute("CREATE DATABASE IF NOT EXISTS wire")
        .await
        .unwrap();

    for table in ["wire.lc_by_server", "wire.lc_by_encoder"] {
        client
            .execute(&format!(
                "CREATE OR REPLACE TABLE {table} (c {TYPE}) \
                 ENGINE = MergeTree ORDER BY tuple()"
            ))
            .await
            .unwrap();
    }
    client
        .execute("INSERT INTO wire.lc_by_server VALUES (['a', 'b', 'a'])")
        .await
        .unwrap();

    let schema = Arc::new(DynamicSchema::from_columns(
        "wire.lc_by_encoder",
        vec![ColumnDef::new("c", TYPE)],
    ));
    let mut insert = DynamicInsert::tcp(
        client.as_tcp().expect("the tcp arm").clone(),
        "wire",
        "lc_by_encoder",
        schema,
    );
    let mut row = Map::new();
    row.insert("c".to_string(), json!(["a", "b", "a"]));
    insert.write_map(&row).await.expect("the row encodes");
    insert.end().await.expect("the insert commits");

    let by_server = client
        .fetch_columns("SELECT c FROM wire.lc_by_server")
        .await
        .expect("read back what the server wrote");
    let by_encoder = client
        .fetch_columns("SELECT c FROM wire.lc_by_encoder")
        .await
        .expect("read back what this crate wrote");

    let (by_encoder, by_server) = (
        rendered(&by_encoder, "c").unwrap(),
        rendered(&by_server, "c").unwrap(),
    );
    server.stop().await;
    assert_eq!(
        by_encoder, by_server,
        "this crate's encoder must lay out {TYPE} the way the server does"
    );
}

/// A root-level JSON array is unwritable as-is, which is why the dynamic
/// encoder wraps one. Both halves are proved here rather than assumed: the
/// server's refusal, and the wrapped document reading back.
#[tokio::test]
async fn a_root_json_array_is_rejected_raw_and_round_trips_wrapped() {
    use std::sync::Arc;

    use serde_json::{Map, json};

    use dfe_clickhouse::dynamic::{
        ColumnDef, DynamicInsert, DynamicSchema, JSON_ARRAY_WRAPPER_KEY,
    };

    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let client = server.tcp();
    client
        .execute("CREATE DATABASE IF NOT EXISTS wire")
        .await
        .unwrap();
    client
        .execute("CREATE OR REPLACE TABLE wire.tags (t JSON) ENGINE = MergeTree ORDER BY tuple()")
        .await
        .unwrap();

    let unwrapped = client
        .execute(r#"INSERT INTO wire.tags VALUES (CAST('["a","b"]', 'JSON'))"#)
        .await;

    let schema = Arc::new(DynamicSchema::from_columns(
        "wire.tags",
        vec![ColumnDef::new("t", "JSON")],
    ));
    let mut insert = DynamicInsert::tcp(
        client.as_tcp().expect("the tcp arm").clone(),
        "wire",
        "tags",
        schema,
    );
    let mut row = Map::new();
    row.insert("t".to_string(), json!(["a", "b"]));
    insert.write_map(&row).await.expect("the row encodes");
    insert.end().await.expect("the insert commits");

    let read = client.fetch_columns("SELECT t FROM wire.tags").await;
    server.stop().await;

    let err = unwrapped.expect_err("a root array is not a JSON document the column accepts");
    assert!(
        format!("{err}").contains("117"),
        "the server must refuse it as code 117, got: {err}"
    );
    assert_eq!(
        read.expect("the wrapped document reads back")
            .get::<String>("t")
            .unwrap(),
        [format!(r#"{{"{JSON_ARRAY_WRAPPER_KEY}":["a","b"]}}"#)]
    );
}

/// Document shapes a real ingest sends at a `JSON` column, as
/// `(name, the document)`.
///
/// The type matrix covers `ClickHouse` types; these cover the payloads. Each
/// one is written twice -- once by the server from the same SQL literal, once
/// through this crate's encoder -- and the two must read back identically, so
/// the oracle is the server rather than this crate's reading of itself.
const JSON_DOCUMENTS: &[(&str, &str)] = &[
    ("empty object", r"{}"),
    (
        "ecs-shaped event",
        r#"{"@timestamp":"2026-09-02T08:00:00Z","host":{"name":"h1"},"tags":["beats","filebeat"]}"#,
    ),
    // A dotted KEY and real nesting reach the same path in ClickHouse's JSON
    // type, so both must survive a round trip without collapsing into one.
    ("dotted key", r#"{"a.b":1}"#),
    ("nested object", r#"{"a":{"b":1}}"#),
    ("null inside", r#"{"a":null,"b":1}"#),
    ("empty string value", r#"{"a":""}"#),
    ("unicode and emoji", r#"{"ünïcode":"héllo","emoji":"🎯"}"#),
    (
        "escapes",
        r#"{"quote":"he said \"hi\"","back\\slash":"c:\\tmp","newline":"a\nb","tab":"a\tb"}"#,
    ),
    // The widest integers a JSON column holds; past this the server refuses
    // the document outright, which the test below pins.
    (
        "integer bounds",
        r#"{"i":-9223372036854775808,"u":18446744073709551615}"#,
    ),
    ("float forms", r#"{"e":1.5e300,"neg":-0.0,"small":1e-300}"#),
    ("array of objects", r#"{"items":[{"id":1},{"id":2}]}"#),
    ("nested arrays", r#"{"m":[[1,2],[3]]}"#),
    ("deep nesting", r#"{"a":{"b":{"c":{"d":{"e":{"f":1}}}}}}"#),
];

/// Every shape a real ingest sends at a `JSON` column must survive this
/// crate's encoder and come back as what the server itself stored.
#[tokio::test]
async fn json_documents_round_trip_as_the_server_stores_them() {
    use std::sync::Arc;

    use serde_json::{Map, Value};

    use dfe_clickhouse::dynamic::{ColumnDef, DynamicInsert, DynamicSchema};

    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let client = server.tcp();
    client
        .execute("CREATE DATABASE IF NOT EXISTS wire")
        .await
        .unwrap();

    let mut failures = Vec::new();
    for (i, (name, document)) in JSON_DOCUMENTS.iter().enumerate() {
        let table = format!("wire.doc{i}");
        client
            .execute(&format!(
                "CREATE OR REPLACE TABLE {table} (d JSON) ENGINE = MergeTree ORDER BY tuple()"
            ))
            .await
            .unwrap();

        // The server's own copy, from the literal.
        let quoted = document.replace('\\', r"\\").replace('\'', r"\'");
        if let Err(e) = client
            .execute(&format!(
                "INSERT INTO {table} VALUES (CAST('{quoted}', 'JSON'))"
            ))
            .await
        {
            failures.push(format!("{name}: the server rejected its own literal: {e}"));
            continue;
        }

        // This crate's copy, through the dynamic encoder.
        let schema = Arc::new(DynamicSchema::from_columns(
            &table,
            vec![ColumnDef::new("d", "JSON")],
        ));
        let mut insert = DynamicInsert::tcp(
            client.as_tcp().expect("the tcp arm").clone(),
            "wire",
            &format!("doc{i}"),
            schema,
        );
        let parsed: Value = serde_json::from_str(document).expect("the case is valid JSON");
        let mut row = Map::new();
        row.insert("d".to_string(), parsed);
        if let Err(e) = insert.write_map(&row).await {
            failures.push(format!("{name}: encode failed: {e}"));
            continue;
        }
        if let Err(e) = insert.end().await {
            failures.push(format!("{name}: insert failed: {e}"));
            continue;
        }

        match client
            .fetch_columns(&format!("SELECT d FROM {table} ORDER BY toString(d)"))
            .await
        {
            Ok(read) => {
                let docs = read
                    .get::<String>("d")
                    .expect("a JSON column reads as text");
                match docs.as_slice() {
                    [by_encoder, by_server] if by_encoder == by_server => {}
                    [a, b] => failures.push(format!("{name}: encoder {a} != server {b}")),
                    other => failures.push(format!("{name}: expected 2 rows, got {}", other.len())),
                }
            }
            Err(e) => failures.push(format!("{name}: read failed: {e}")),
        }
    }

    server.stop().await;
    assert!(
        failures.is_empty(),
        "{} document(s) failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// An integer too wide for any `ClickHouse` integer type is stored as a
/// `Float64`, losing precision, and nothing reports it.
///
/// The two routes disagree, which is what makes this worth pinning.
/// `CAST('<doc>', 'JSON')` refuses the document with code 117. The insert path
/// accepts it -- a `JSON` column is declared `String` on the wire and the
/// server converts on the way in -- and the conversion silently rounds to
/// about 17 significant digits. A pipeline sending large integers gets neither
/// the value nor an error.
#[tokio::test]
async fn an_oversized_json_number_is_stored_as_a_lossy_float() {
    use std::sync::Arc;

    use serde_json::{Map, Value};

    use dfe_clickhouse::dynamic::{ColumnDef, DynamicInsert, DynamicSchema};

    const HUGE: &str = r#"{"huge":123456789012345678901234567890}"#;

    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let client = server.tcp();
    client
        .execute("CREATE DATABASE IF NOT EXISTS wire")
        .await
        .unwrap();
    client
        .execute("CREATE OR REPLACE TABLE wire.huge (d JSON) ENGINE = MergeTree ORDER BY tuple()")
        .await
        .unwrap();

    let by_server = client
        .execute(&format!(
            "INSERT INTO wire.huge VALUES (CAST('{HUGE}', 'JSON'))"
        ))
        .await;

    let schema = Arc::new(DynamicSchema::from_columns(
        "wire.huge",
        vec![ColumnDef::new("d", "JSON")],
    ));
    let mut insert = DynamicInsert::tcp(
        client.as_tcp().expect("the tcp arm").clone(),
        "wire",
        "huge",
        schema,
    );
    let parsed: Value = serde_json::from_str(HUGE).expect("the literal is valid JSON");
    let mut row = Map::new();
    row.insert("d".to_string(), parsed);
    // The encoder writes the text unchanged; the server decides.
    insert.write_map(&row).await.expect("the row encodes");
    insert.end().await.expect("the insert commits");

    let read = client.fetch_columns("SELECT d FROM wire.huge").await;
    server.stop().await;

    let server_err = by_server.expect_err("a CAST refuses the oversized literal");
    assert!(
        format!("{server_err}").contains("117"),
        "expected code 117, got: {server_err}"
    );
    // 1.2345678901234568e29 against the 1.2345678901234567890123456789e29 that
    // went in: the tail is gone and no error was raised.
    assert_eq!(
        read.expect("the inserted document reads back")
            .get::<String>("d")
            .unwrap(),
        [r#"{"huge":1.2345678901234568e29}"#]
    );
}

/// A path whose type changes between rows is the shape a mixed ingest
/// produces, and the one a static schema cannot hold.
#[tokio::test]
async fn a_json_path_that_changes_type_between_rows_reads_back() {
    use std::sync::Arc;

    use serde_json::{Map, json};

    use dfe_clickhouse::dynamic::{ColumnDef, DynamicInsert, DynamicSchema};

    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let client = server.tcp();
    client
        .execute("CREATE DATABASE IF NOT EXISTS wire")
        .await
        .unwrap();
    client
        .execute("CREATE OR REPLACE TABLE wire.mixed (d JSON) ENGINE = MergeTree ORDER BY tuple()")
        .await
        .unwrap();

    let schema = Arc::new(DynamicSchema::from_columns(
        "wire.mixed",
        vec![ColumnDef::new("d", "JSON")],
    ));
    let mut insert = DynamicInsert::tcp(
        client.as_tcp().expect("the tcp arm").clone(),
        "wire",
        "mixed",
        schema,
    );
    for value in [json!({"a": 1}), json!({"a": "x"}), json!({"a": [1, 2]})] {
        let mut row = Map::new();
        row.insert("d".to_string(), value);
        insert.write_map(&row).await.expect("the row encodes");
    }
    insert.end().await.expect("the insert commits");

    let read = client
        .fetch_columns("SELECT d FROM wire.mixed ORDER BY toString(d)")
        .await;
    server.stop().await;

    let mut docs = read
        .expect("the mixed-type column reads back")
        .get::<String>("d")
        .unwrap();
    docs.sort();
    assert_eq!(docs, [r#"{"a":"x"}"#, r#"{"a":1}"#, r#"{"a":[1,2]}"#]);
}

/// The JSON serialisation this crate does NOT decode, pinned as the clean
/// error it is rather than left to surprise someone.
///
/// A `JSON` column reads with or without the string flag, which is what
/// advertising 54473 bought.
///
/// Below the V2 gate (`ProtocolDefines.h:109`) a server sends Object `V1` --
/// wire value 0, per `SerializationObject.h:37` -- and this decoder has no
/// reader for it, so the flag was the only way through. At 54473 the server
/// sends V2 instead, which it does read, so turning
/// `output_format_native_write_json_as_string` off now yields the document
/// rather than an error.
#[tokio::test]
async fn json_reads_with_or_without_the_string_flag() {
    const SQL: &str = r#"SELECT CAST('{"a":1}', 'JSON') AS doc"#;

    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let with_flag = server.tcp().fetch_columns(SQL).await;
    let without = TcpClient::new(format!("{}:{}", server.host, server.native_port))
        .with_json_as_string(false)
        .query(SQL)
        .fetch_blocks()
        .await;
    server.stop().await;

    assert_eq!(
        with_flag
            .expect("the default reads JSON")
            .get::<String>("doc")
            .unwrap(),
        [r#"{"a":1}"#]
    );
    // The raw path returns blocks; the payload block is the one with rows.
    let blocks = without.expect("V2 serialisation reads without the flag");
    let payload: Vec<String> = blocks
        .iter()
        .filter(|b| b.num_rows > 0)
        .flat_map(|b| b.column_as::<String>("doc").expect("doc reads as text"))
        .collect();
    assert_eq!(payload, [r#"{"a":1}"#]);
}

/// A column whose values the server moved out of their typed storage: a JSON
/// path past `max_dynamic_paths` (shared data), a JSON or `Dynamic` type past
/// its type limit (`SharedVariant`).
struct Spill {
    ty: &'static str,
    /// SQL literals, one per row.
    values: &'static [&'static str],
    /// The client setting the plain read uses.
    json_as_string: bool,
    /// What the caller is told to do instead.
    remedy: &'static str,
    /// A SELECT expression that reads every value, and what it returns.
    whole: &'static str,
    want: &'static [&'static str],
}

const SPILLS: &[Spill] = &[
    Spill {
        ty: "JSON(max_dynamic_paths=0)",
        values: &[r#"'{"a":1,"b":"x"}'"#],
        json_as_string: false,
        remedy: "with_json_as_string(true)",
        whole: "d",
        want: &[r#"{"a":1,"b":"x"}"#],
    },
    Spill {
        ty: "JSON(max_dynamic_paths=2)",
        values: &[r#"'{"a":1,"b":2,"c":3,"d":4}'"#],
        json_as_string: false,
        remedy: "with_json_as_string(true)",
        whole: "d",
        want: &[r#"{"a":1,"b":2,"c":3,"d":4}"#],
    },
    Spill {
        ty: "JSON(max_dynamic_types=1)",
        values: &[r#"'{"a":1}'"#, r#"'{"a":"x"}'"#, r#"'{"a":[1,2]}'"#],
        json_as_string: false,
        remedy: "with_json_as_string(true)",
        whole: "d",
        want: &[r#"{"a":1}"#, r#"{"a":"x"}"#, r#"{"a":[1,2]}"#],
    },
    // No client setting applies to Dynamic, so even the default read refuses.
    Spill {
        ty: "Dynamic(max_types=1)",
        values: &["42::UInt64", "'x'", "[1, 2]::Array(UInt8)"],
        json_as_string: true,
        remedy: "AS String)",
        whole: "CAST(d AS String) AS d",
        want: &["42", "x", "[1,2]"],
    },
];

/// A spilled value must fail the read loudly, naming the column and the way
/// to read it, and that way must read every value.
#[tokio::test]
async fn spilled_values_are_refused_never_dropped() {
    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let client = server.tcp();
    client
        .execute("CREATE DATABASE IF NOT EXISTS wire")
        .await
        .unwrap();

    let mut failures = Vec::new();
    for (i, spill) in SPILLS.iter().enumerate() {
        let (ty, table) = (spill.ty, format!("wire.spill{i}"));
        client
            .execute(&format!(
                "CREATE OR REPLACE TABLE {table} (n UInt8, d {ty}) ENGINE = MergeTree ORDER BY n"
            ))
            .await
            .unwrap();
        let rows: Vec<String> = (0..)
            .zip(spill.values)
            .map(|(n, v)| format!("({n}, {v})"))
            .collect();
        client
            .execute(&format!("INSERT INTO {table} VALUES {}", rows.join(", ")))
            .await
            .unwrap();

        let whole = format!("SELECT {} FROM {table} ORDER BY n", spill.whole);
        match client
            .fetch_columns(&whole)
            .await
            .and_then(|c| c.get::<String>("d"))
        {
            Ok(got) if got == spill.want => {}
            other => failures.push(format!("{ty}: {} read {other:?}", spill.whole)),
        }
        let raw = TcpClient::new(format!("{}:{}", server.host, server.native_port))
            .with_json_as_string(spill.json_as_string)
            .query(&format!("SELECT d FROM {table} ORDER BY n"))
            .fetch_blocks()
            .await;
        match raw {
            Err(e)
                if e.to_string().contains("column 'd'") && e.to_string().contains(spill.remedy) => {
            }
            other => failures.push(format!(
                "{ty}: expected an error naming column 'd' and {}, got {:?}",
                spill.remedy,
                other.map(|b| b
                    .iter()
                    .filter(|b| b.num_rows > 0)
                    .flat_map(|b| b.column_as::<String>("d").unwrap_or_default())
                    .collect::<Vec<_>>())
            )),
        }
        // HTTP reads through the same decoder, always with the string flag on.
        if spill.json_as_string {
            match server
                .http()
                .fetch_columns(&format!("SELECT d FROM {table} ORDER BY n"))
                .await
            {
                Err(e)
                    if e.to_string().contains("column 'd'")
                        && e.to_string().contains(spill.remedy) => {}
                other => failures.push(format!(
                    "{ty}: over HTTP, expected an error naming column 'd', got {:?}",
                    other.and_then(|c| c.get::<String>("d"))
                )),
            }
        }
    }

    server.stop().await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Documents for a plain `JSON` column, as `(name, rows, INSERT settings,
/// refused)`: one row per value type the server can hold in a path, scalar,
/// in an array and in a nested object, then the nesting shapes. A row that is
/// not refused must read back from the path-based serialisation byte for byte
/// as the server prints it.
const PATH_JSON: &[(&str, &[&str], &str, bool)] = &[
    ("Bool true", &[r#"{"v":true}"#], "", false),
    ("Bool false", &[r#"{"v":false}"#], "", false),
    ("Int64 min", &[r#"{"v":-9223372036854775808}"#], "", false),
    ("Int64 max", &[r#"{"v":9223372036854775807}"#], "", false),
    ("UInt64 max", &[r#"{"v":18446744073709551615}"#], "", false),
    (
        "Float64 fractional",
        &[r#"{"v":1.5,"w":0.1,"x":-0.25}"#],
        "",
        false,
    ),
    (
        "Float64 large",
        &[r#"{"v":1e300,"w":1.2345678901234568e29}"#],
        "",
        false,
    ),
    (
        "Float64 small",
        &[r#"{"v":1e-300,"w":0.000001}"#],
        "",
        false,
    ),
    (
        "Float64 whole",
        &[r#"{"v":100000000000000000000.0}"#],
        "",
        false,
    ),
    (
        "String escapes",
        &[r#"{"v":"q\"b\\s/n\nt\tc\u0001"}"#],
        "",
        false,
    ),
    ("String unicode", &[r#"{"v":"ünïcode 🎯"}"#], "", false),
    (
        "String control and separators",
        &[r#"{"v":"\b\f\r\u001f\u007f\u2028\u2029"}"#],
        "",
        false,
    ),
    // The edges of the plain-decimal range the server prints in.
    (
        "Float64 notation edges",
        &[r#"{"a":0.000001,"b":0.0000001,"c":123456789012345680000.0,"d":1e21,"e":-0.0}"#],
        "",
        false,
    ),
    // The server infers dates from strings and prints them in its time zone.
    ("Date", &[r#"{"v":"2026-11-01"}"#], "", true),
    (
        "DateTime64",
        &[r#"{"v":"2026-11-01 08:00:00.123456789"}"#],
        "",
        true,
    ),
    ("null", &[r#"{"v":null,"w":1}"#], "", false),
    ("array of Bool", &[r#"{"v":[true,false]}"#], "", false),
    (
        "array of Int64",
        &[r#"{"v":[-9223372036854775808,9223372036854775807]}"#],
        "",
        false,
    ),
    (
        "array of UInt64",
        &[r#"{"v":[18446744073709551615]}"#],
        "",
        false,
    ),
    ("array of Float64", &[r#"{"v":[1.5,1e300,0.1]}"#], "", false),
    ("array of String", &[r#"{"v":["a\"b","ü"]}"#], "", false),
    ("array of Date", &[r#"{"v":["2026-11-01"]}"#], "", true),
    ("array with null", &[r#"{"v":[1,null]}"#], "", false),
    ("array of arrays", &[r#"{"v":[[1,2],[3]]}"#], "", false),
    (
        "object of each scalar",
        &[
            r#"{"o":{"b":true,"f":0.1,"i":-9223372036854775808,"s":"x\ny","u":18446744073709551615}}"#,
        ],
        "",
        false,
    ),
    (
        "object holding a Date",
        &[r#"{"o":{"d":"2026-11-01"}}"#],
        "",
        true,
    ),
    ("one level", &[r#"{"a":{"b":1}}"#], "", false),
    (
        "mixed depths in one row",
        &[r#"{"a":{"b":{"c":1},"d":"x"},"e":2}"#],
        "",
        false,
    ),
    // An array of objects is `Array(JSON)`, whose own prefix the path-based
    // reader does not walk.
    (
        "array of objects",
        &[r#"{"items":[{"id":1},{"id":2,"tag":{"k":"v"}}]}"#],
        "",
        true,
    ),
    // `b-` sorts ahead of `b.x` as a full path, behind `b` as a key.
    (
        "sibling order",
        &[r#"{"a":{"b":{"x":1},"b-":2}}"#],
        "",
        false,
    ),
    (
        "a scalar and an object at one path in different rows",
        &[r#"{"a":1}"#, r#"{"a":{"b":2}}"#],
        "",
        false,
    ),
    // One row holding a value at `a` and at `a.b` has no faithful nesting.
    (
        "a scalar and an object at one path in one row",
        &[r#"{"a":1,"a.b":2}"#],
        "",
        true,
    ),
    // An escaped dot belongs to the key, which the path alone cannot show.
    (
        "a key holding a dot",
        &[r#"{"a.b":1}"#],
        " SETTINGS json_type_escape_dots_in_keys = 1",
        true,
    ),
];

/// The setting that makes the server send the flattened JSON serialisation
/// rather than V2.
const FLATTENED: &str = "output_format_native_use_flattened_dynamic_and_json_serialization";

/// The path-based read, in both serialisation versions, must print each
/// document exactly as the server does, and refuse the ones it cannot.
#[tokio::test]
async fn path_based_json_reads_byte_for_byte_like_the_server() {
    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let client = server.tcp();
    let raw = TcpClient::new(format!("{}:{}", server.host, server.native_port))
        .with_json_as_string(false);
    let readers = [
        ("V2", raw.clone()),
        ("flattened", raw.with_setting(FLATTENED, "1")),
    ];
    client
        .execute("CREATE DATABASE IF NOT EXISTS wire")
        .await
        .unwrap();

    let mut failures = Vec::new();
    for (i, (name, docs, settings, refused)) in PATH_JSON.iter().enumerate() {
        let table = format!("wire.path{i}");
        client
            .execute(&format!(
                "CREATE OR REPLACE TABLE {table} (n UInt8, d JSON) ENGINE = MergeTree ORDER BY n"
            ))
            .await
            .unwrap();
        let rows: Vec<String> = (0..)
            .zip(docs.iter())
            .map(|(n, doc)| {
                let quoted = doc.replace('\\', r"\\").replace('\'', r"\'");
                format!("({n}, '{quoted}')")
            })
            .collect();
        if let Err(e) = client
            .execute(&format!(
                "INSERT INTO {table}{settings} VALUES {}",
                rows.join(", ")
            ))
            .await
        {
            failures.push(format!("{name}: the server refused the insert: {e}"));
            continue;
        }

        let sql = format!("SELECT d FROM {table} ORDER BY n");
        let printed = client
            .fetch_columns(&sql)
            .await
            .and_then(|c| c.get::<String>("d"));
        for (version, reader) in &readers {
            let rebuilt = reader.query(&sql).fetch_blocks().await.map(|blocks| {
                blocks
                    .iter()
                    .filter(|b| b.num_rows > 0)
                    .flat_map(|b| b.column_as::<String>("d").unwrap_or_default())
                    .collect::<Vec<_>>()
            });
            match (refused, &printed, rebuilt) {
                (false, Ok(p), Ok(r)) if *p == r => {}
                (true, Ok(_), Err(e))
                    if e.to_string().contains("column 'd'")
                        && e.to_string().contains("with_json_as_string(true)") => {}
                (_, p, r) => failures.push(format!(
                    "{name} ({version}): server printed {p:?}, path read {r:?}"
                )),
            }
        }
    }

    server.stop().await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Standalone `Dynamic` and `Variant` values, as `(column type, SQL value,
/// refused)`. A value that is not refused must read back as the server's own
/// `toJSONString` of it.
const DYNAMIC_VALUES: &[(&str, &str, bool)] = &[
    ("Dynamic", "true::Bool", false),
    ("Dynamic", "'q/x\"y'::String", false),
    ("Dynamic", "-9223372036854775808::Int64", false),
    ("Dynamic", "0.1::Float32", false),
    ("Dynamic", "1e-7::Float32", false),
    ("Dynamic", "3.4e38::Float32", false),
    ("Dynamic", "'2026-11-01'::Date", true),
    (
        "Dynamic",
        "'2026-11-01 08:00:00.123456789'::DateTime64(9)",
        true,
    ),
    ("Variant(Bool, String)", "true::Bool", false),
    ("Variant(Bool, String)", "'q/x\"y'::String", false),
    (
        "Variant(Int64, String)",
        "-9223372036854775808::Int64",
        false,
    ),
    ("Variant(Float32, String)", "0.1::Float32", false),
    ("Variant(Float32, String)", "1e-7::Float32", false),
    ("Variant(Float32, String)", "3.4e38::Float32", false),
    ("Variant(Date, String)", "'2026-11-01'::Date", true),
    (
        "Variant(DateTime64(9), String)",
        "'2026-11-01 08:00:00.123456789'::DateTime64(9)",
        true,
    ),
];

/// A `Dynamic` or `Variant` value must print as the server prints it, or be
/// refused by name with the cast that reads it, on both Dynamic serialisations.
#[tokio::test]
async fn dynamic_and_variant_values_print_like_the_server_or_refuse() {
    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let client = server.tcp();
    let readers = [
        ("V2", client.clone()),
        (
            "flattened",
            UnifiedClient::Tcp(
                TcpClient::new(format!("{}:{}", server.host, server.native_port))
                    .with_setting(FLATTENED, "1"),
            ),
        ),
    ];
    client
        .execute("CREATE DATABASE IF NOT EXISTS wire")
        .await
        .unwrap();

    let mut failures = Vec::new();
    for (i, (ty, value, refused)) in DYNAMIC_VALUES.iter().enumerate() {
        let table = format!("wire.dynval{i}");
        client
            .execute(&format!(
                "CREATE OR REPLACE TABLE {table} (d {ty}) ENGINE = MergeTree ORDER BY tuple()"
            ))
            .await
            .unwrap();
        client
            .execute(&format!("INSERT INTO {table} VALUES ({value})"))
            .await
            .unwrap();
        let printed = client
            .fetch_columns(&format!(
                "SELECT assumeNotNull(toJSONString(d)) AS d FROM {table}"
            ))
            .await
            .and_then(|c| c.get::<String>("d"));
        for (version, reader) in &readers {
            let read = reader
                .fetch_columns(&format!("SELECT d FROM {table}"))
                .await
                .and_then(|c| c.get::<String>("d"));
            match (refused, &printed, read) {
                (false, Ok(p), Ok(r)) if *p == r => {}
                (true, Ok(_), Err(e))
                    if e.to_string().contains("column 'd'")
                        && e.to_string().contains("AS String)") => {}
                (_, p, r) => failures.push(format!(
                    "{ty} {value} ({version}): server printed {p:?}, read {r:?}"
                )),
            }
        }
    }

    server.stop().await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Server types the decoder has no representation for, as `(column type, the
/// value inserted, the server's spelling of the value's type)`.
///
/// Every SQL route into a JSON path infers its type from text, so a JSON path
/// of an unparsed type is pinned by `a_json_path_of_an_unparsed_type_is_unsupported`
/// in `src/native/columns.rs` instead.
const UNREPRESENTABLE: &[(&str, &str, &str)] = &[
    (
        "Dynamic",
        "CAST(tuple(1), 'Tuple(x UInt8)')",
        "Tuple(x UInt8)",
    ),
    ("Dynamic", "toIntervalSecond(5)", "IntervalSecond"),
];

/// A value of a type the decoder cannot represent is an unsupported value,
/// never a malformed response: the error is `Unsupported` and names the column
/// and the server's spelling of the type, on both serialisations.
#[tokio::test]
async fn an_unrepresentable_type_is_unsupported_not_a_bad_response() {
    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let client = server.tcp();
    let raw = TcpClient::new(format!("{}:{}", server.host, server.native_port))
        .with_json_as_string(false);
    let readers = [
        ("V2", raw.clone()),
        ("flattened", raw.with_setting(FLATTENED, "1")),
    ];
    client
        .execute("CREATE DATABASE IF NOT EXISTS wire")
        .await
        .unwrap();

    let mut failures = Vec::new();
    for (i, (ty, value, spelling)) in UNREPRESENTABLE.iter().enumerate() {
        let table = format!("wire.unrep{i}");
        client
            .execute(&format!(
                "CREATE OR REPLACE TABLE {table} (d {ty}) ENGINE = MergeTree ORDER BY tuple()"
            ))
            .await
            .unwrap();
        if let Err(e) = client
            .execute(&format!("INSERT INTO {table} SELECT {value}"))
            .await
        {
            failures.push(format!("{ty}: the server refused the insert: {e}"));
            continue;
        }
        for (version, reader) in &readers {
            match reader
                .query(&format!("SELECT d FROM {table}"))
                .fetch_blocks()
                .await
            {
                Err(dfe_clickhouse::Error::Unsupported(m))
                    if m.contains("column 'd'") && m.contains(spelling) => {}
                other => failures.push(format!(
                    "{ty} holding {spelling} ({version}): {:?}",
                    other.map(|b| b.len())
                )),
            }
        }
    }

    server.stop().await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A JSON column with a typed path inserts through the dynamic path on both
/// transports and reads back in string mode on both. The path-based read
/// would have to decode the typed sub-column, so it refuses.
#[tokio::test]
async fn typed_path_json_round_trips_in_string_mode() {
    const DOC: &str = r#"{"a":{"b":7},"x":"y"}"#;

    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let (tcp, http) = (server.tcp(), server.http());
    tcp.execute("CREATE DATABASE IF NOT EXISTS wire")
        .await
        .unwrap();
    tcp.execute(
        "CREATE OR REPLACE TABLE wire.typed (n UInt32, d JSON(`a.b` UInt32)) \
         ENGINE = MergeTree ORDER BY n",
    )
    .await
    .unwrap();

    let mut failures = Vec::new();
    for (n, client) in [(0u32, &http), (1, &tcp)] {
        let cache = dfe_clickhouse::dynamic::DynamicSchemaCache::new(Duration::from_secs(60));
        let outcome = async {
            let mut insert = client.dynamic_insert("wire", "typed", cache).await?;
            let mut row = serde_json::Map::new();
            row.insert("n".to_string(), n.into());
            row.insert("d".to_string(), serde_json::from_str(DOC).unwrap());
            insert.write_map(&row).await?;
            insert.end().await?;
            Ok::<_, dfe_clickhouse::Error>(())
        }
        .await;
        if let Err(e) = outcome {
            failures.push(format!("{:?} insert: {e}", client.transport()));
        }
    }
    for client in [&tcp, &http] {
        match client
            .fetch_columns("SELECT d FROM wire.typed ORDER BY n")
            .await
            .and_then(|c| c.get::<String>("d"))
        {
            Ok(docs) if docs == [DOC, DOC] => {}
            other => failures.push(format!("{:?} read {other:?}", client.transport())),
        }
    }
    let raw = TcpClient::new(format!("{}:{}", server.host, server.native_port))
        .with_json_as_string(false)
        .query("SELECT d FROM wire.typed")
        .fetch_blocks()
        .await;
    match raw {
        Err(e) if e.to_string().contains("column 'd'") && e.to_string().contains("typed") => {}
        other => failures.push(format!("path-based read: {:?}", other.map(|b| b.len()))),
    }

    server.stop().await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A `MergeTree` column that is almost all defaults, merged into one part, is
/// stored with sparse serialisation. Real tables look like this.
///
/// Passing does not prove the sparse form reached the wire: the server may
/// have materialised the column before sending. This decoder rejects a
/// non-zero custom-serialization flag outright, so it would have failed loudly
/// if it had not. Proving the wire form needs that flag observed rather than
/// the values alone.
#[tokio::test]
async fn a_sparse_column_reads_back_on_both_transports() {
    const TOTAL: &str = "SELECT sum(c) AS total FROM wire.sparse";
    // The column itself, where the sparse serialisation reaches the wire
    // rather than being collapsed by an aggregate.
    const RAW: &str = "SELECT c FROM wire.sparse ORDER BY n LIMIT 6000";

    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let client = server.tcp();
    client
        .execute("CREATE DATABASE IF NOT EXISTS wire")
        .await
        .unwrap();
    // The default ratio is 0.95; 1 non-default row in 10,000 is well under it.
    client
        .execute(
            "CREATE OR REPLACE TABLE wire.sparse (n UInt64, c UInt64) \
             ENGINE = MergeTree ORDER BY n \
             SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9",
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO wire.sparse \
             SELECT number, if(number = 5000, 42, 0) FROM system.numbers LIMIT 10000",
        )
        .await
        .unwrap();
    client
        .execute("OPTIMIZE TABLE wire.sparse FINAL")
        .await
        .unwrap();

    let over_tcp = client.fetch_columns(TOTAL).await.expect("tcp reads it");
    let over_http = server
        .http()
        .fetch_columns(TOTAL)
        .await
        .expect("http reads it");
    let values = client
        .fetch_columns(RAW)
        .await
        .expect("tcp reads the column")
        .get::<u64>("c")
        .unwrap();

    server.stop().await;
    assert_eq!(over_tcp.get::<u64>("total").unwrap(), [42]);
    assert_eq!(over_http.get::<u64>("total").unwrap(), [42]);
    assert_eq!(values.len(), 6000);
    assert_eq!(values[5000], 42, "the one non-default row");
    assert_eq!(values[0], 0);
}

/// A result set the server splits into several blocks must read back whole
/// and in order, on both transports.
#[tokio::test]
async fn a_multi_block_result_reads_back_whole_and_in_order() {
    const SQL: &str = "SELECT number AS n FROM system.numbers LIMIT 200000";

    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let over_tcp = server.tcp().fetch_columns(SQL).await.unwrap();
    let over_http = server.http().fetch_columns(SQL).await.unwrap();
    let ns = over_tcp.get::<u64>("n").unwrap();
    let http_ns = over_http.get::<u64>("n").unwrap();
    server.stop().await;

    assert_eq!(ns.len(), 200_000, "every row must arrive");
    assert_eq!(ns[0], 0);
    assert_eq!(ns[199_999], 199_999);
    assert_eq!(http_ns, ns, "transports disagree");
}

/// An exception raised part-way through a result set must surface as an
/// error, not as a short read that looks like success.
#[tokio::test]
async fn a_mid_stream_exception_surfaces_rather_than_truncating() {
    const SQL: &str = "SELECT throwIf(number = 50000) FROM system.numbers LIMIT 100000";

    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let err = server
        .tcp()
        .fetch_columns(SQL)
        .await
        .expect_err("the server throws part-way through");
    server.stop().await;
    assert!(
        format!("{err}").contains("Value passed to 'throwIf'"),
        "the server's own message must reach the caller: {err}"
    );
}

/// The connection is reusable after a failed query: the actor drains and
/// returns to idle rather than staying poisoned.
#[tokio::test]
async fn a_failed_query_leaves_the_connection_usable() {
    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let client = server.tcp();
    let _ = client.fetch_columns("SELECT * FROM no_such_table").await;

    let ok = client.fetch_columns("SELECT 1 AS n").await.unwrap();
    let n = ok.get::<u8>("n").unwrap();
    server.stop().await;
    assert_eq!(n, [1]);
}

/// One column type DFE schemas declare, as the `CREATE TABLE` spells it.
struct DfeType {
    ch_type: &'static str,
    /// `(the JSON value a row carries, what toString reads back)`.
    rows: &'static [(&'static str, &'static str)],
    /// The dynamic insert must refuse the type on both transports.
    refused: bool,
}

const fn reads(ch_type: &'static str, rows: &'static [(&'static str, &'static str)]) -> DfeType {
    DfeType {
        ch_type,
        rows,
        refused: false,
    }
}

/// `rows` carry the values written, and nothing is read back.
const fn refused(ch_type: &'static str, rows: &'static [(&'static str, &'static str)]) -> DfeType {
    DfeType {
        ch_type,
        rows,
        refused: true,
    }
}

/// Every type the DFE schemas emit, drawn from dfe-schemas' type registry and
/// column overrides, plus the two shapes the insert has no encoding for.
const DFE_TYPES: &[DfeType] = &[
    reads("String", &[(r#""x""#, "x"), (r#""""#, "")]),
    reads("Nullable(String)", &[("null", "NULL"), (r#""x""#, "x")]),
    reads("LowCardinality(String)", &[(r#""lc""#, "lc")]),
    // An empty string shares its bytes with the NULL slot's placeholder.
    reads(
        "LowCardinality(Nullable(String))",
        &[("null", "NULL"), (r#""lc""#, "lc"), (r#""""#, "")],
    ),
    reads("Int32", &[("-2147483648", "-2147483648")]),
    reads("Int64", &[("-9223372036854775808", "-9223372036854775808")]),
    reads("Nullable(Int64)", &[("null", "NULL"), ("7", "7")]),
    reads("UInt8", &[("255", "255")]),
    reads("UInt32", &[("4294967295", "4294967295")]),
    reads(
        "UInt64",
        &[("18446744073709551615", "18446744073709551615")],
    ),
    reads("Float64", &[("1.5", "1.5")]),
    reads("Nullable(Float64)", &[("null", "NULL"), ("-0.25", "-0.25")]),
    reads("Bool", &[("true", "true"), ("false", "false")]),
    reads("Date", &[(r#""2026-11-01""#, "2026-11-01")]),
    reads(
        "Nullable(Date)",
        &[("null", "NULL"), (r#""2026-11-01""#, "2026-11-01")],
    ),
    reads(
        "DateTime",
        &[(r#""2026-11-01T08:00:00Z""#, "2026-11-01 08:00:00")],
    ),
    reads(
        "DateTime('UTC')",
        &[(r#""2026-11-01T08:00:00Z""#, "2026-11-01 08:00:00")],
    ),
    reads(
        "DateTime64(3)",
        &[(r#""2026-11-01T08:00:00.123Z""#, "2026-11-01 08:00:00.123")],
    ),
    reads(
        "DateTime64(3, 'UTC')",
        &[(r#""2026-11-01T08:00:00.123Z""#, "2026-11-01 08:00:00.123")],
    ),
    reads(
        "Nullable(DateTime64(3, 'UTC'))",
        &[
            ("null", "NULL"),
            (r#""2026-11-01T08:00:00.5Z""#, "2026-11-01 08:00:00.500"),
        ],
    ),
    reads(
        "DateTime64(6, 'UTC')",
        &[(
            r#""2026-11-01T08:00:00.123456Z""#,
            "2026-11-01 08:00:00.123456",
        )],
    ),
    reads(
        "DateTime64(9)",
        &[(
            r#""2026-11-01T08:00:00.123456789Z""#,
            "2026-11-01 08:00:00.123456789",
        )],
    ),
    reads("IPv6", &[(r#""2001:db8::1""#, "2001:db8::1")]),
    reads("Nullable(IPv6)", &[("null", "NULL"), (r#""::1""#, "::1")]),
    reads(
        "UUID",
        &[(
            r#""6f1c2e8a-0000-4000-8000-000000000001""#,
            "6f1c2e8a-0000-4000-8000-000000000001",
        )],
    ),
    reads(
        "Nullable(UUID)",
        &[
            ("null", "NULL"),
            (
                r#""6f1c2e8a-0000-4000-8000-000000000002""#,
                "6f1c2e8a-0000-4000-8000-000000000002",
            ),
        ],
    ),
    reads(
        "JSON",
        &[(r#"{"a":1,"b":{"c":"x"}}"#, r#"{"a":1,"b":{"c":"x"}}"#)],
    ),
    reads(
        "JSON(max_dynamic_paths=2048)",
        &[(r#"{"a":1,"b":{"c":"x"}}"#, r#"{"a":1,"b":{"c":"x"}}"#)],
    ),
    reads(
        "JSON(max_dynamic_types=8, max_dynamic_paths=16, SKIP a.b, SKIP REGEXP 'x.*')",
        &[(
            r#"{"a":{"b":1,"c":2},"xy":3,"k":"v"}"#,
            r#"{"a":{"c":2},"k":"v"}"#,
        )],
    ),
    reads(
        "Nullable(JSON)",
        &[("null", "NULL"), (r#"{"a":1}"#, r#"{"a":1}"#)],
    ),
    reads("Point", &[("[1.5, -2.5]", "(1.5,-2.5)")]),
    reads(
        "Point",
        &[(r#"{"lat": -33.86, "lon": 151.21}"#, "(151.21,-33.86)")],
    ),
    reads("Enum8('a' = 1, 'b' = 2)", &[(r#""b""#, "b")]),
    reads(
        "Map(LowCardinality(String), String)",
        &[(r#"{"k":"v"}"#, "{'k':'v'}"), ("{}", "{}")],
    ),
    reads(
        "Array(String)",
        &[(r#"["a","b"]"#, "['a','b']"), ("[]", "[]")],
    ),
    reads("Array(IPv6)", &[(r#"["::1"]"#, "['::1']")]),
    reads("Array(Int64)", &[("[-1, 2]", "[-1,2]")]),
    reads("Array(Float64)", &[("[1.5]", "[1.5]")]),
    reads("Array(Float32)", &[("[0.5]", "[0.5]")]),
    reads(
        "Array(UInt64)",
        &[("[18446744073709551615]", "[18446744073709551615]")],
    ),
    reads(
        "Array(DateTime)",
        &[(r#"["2026-11-01T08:00:00Z"]"#, "['2026-11-01 08:00:00']")],
    ),
    reads(
        "Array(DateTime64(9))",
        &[(
            r#"["2026-11-01T08:00:00.000000001Z"]"#,
            "['2026-11-01 08:00:00.000000001']",
        )],
    ),
    reads(
        "Array(LowCardinality(String))",
        &[(r#"["a","a"]"#, "['a','a']")],
    ),
    reads(
        "Array(Map(LowCardinality(String), String))",
        &[(r#"[{"k":"v"}]"#, "[{'k':'v'}]")],
    ),
    reads("Array(JSON)", &[(r#"[{"a":1}]"#, r#"['{"a":1}']"#)]),
    reads(
        "JSON(`a.b` UInt32)",
        &[(r#"{"a":{"b":1},"c":"x"}"#, r#"{"a":{"b":1},"c":"x"}"#)],
    ),
    // An aggregate state has no value form a row could carry.
    refused("AggregateFunction(sum, UInt64)", &[("1", "")]),
];

/// Write each row of `case` through the dynamic insert on `client`, numbering
/// rows from `first`.
async fn dynamic_write(
    client: &UnifiedClient,
    table: &str,
    case: &DfeType,
    first: u32,
) -> Result<(), String> {
    use std::sync::Arc;

    use dfe_clickhouse::dynamic::DynamicSchemaCache;
    use serde_json::{Map, Value};

    let cache: Arc<DynamicSchemaCache> = DynamicSchemaCache::new(Duration::from_secs(60));
    let mut insert = client
        .dynamic_insert("wire", table, cache)
        .await
        .map_err(|e| e.to_string())?;
    for (n, (value, _)) in (first..).zip(case.rows) {
        let mut row = Map::new();
        row.insert("n".to_string(), Value::from(n));
        row.insert(
            "c".to_string(),
            serde_json::from_str(value).expect("the case value is valid JSON"),
        );
        insert.write_map(&row).await.map_err(|e| e.to_string())?;
    }
    insert.end().await.map(drop).map_err(|e| e.to_string())
}

/// Run one table row: write it over HTTP then TCP, and read both copies back
/// over both transports.
async fn dfe_type_round_trip(
    tcp: &UnifiedClient,
    http: &UnifiedClient,
    case: &DfeType,
    index: usize,
) -> Result<(), String> {
    let table = format!("dfe{index}");
    tcp.execute(&format!(
        "CREATE OR REPLACE TABLE wire.{table} (n UInt32, c {}) ENGINE = MergeTree ORDER BY n",
        case.ch_type
    ))
    .await
    .map_err(|e| format!("CREATE rejected: {e}"))?;

    let width = u32::try_from(case.rows.len()).unwrap();
    let written = [
        ("http", dynamic_write(http, &table, case, 0).await),
        ("tcp", dynamic_write(tcp, &table, case, width).await),
    ];
    if case.refused {
        return match written
            .iter()
            .find(|(_, r)| !r.as_ref().is_err_and(|e| e.contains("unsupported")))
        {
            None => Ok(()),
            Some((name, outcome)) => Err(format!(
                "{name} should refuse as unsupported, got {outcome:?}"
            )),
        };
    }
    for (name, outcome) in &written {
        outcome
            .as_ref()
            .map_err(|e| format!("{name} insert failed: {e}"))?;
    }

    let want: Vec<&str> = case.rows.iter().chain(case.rows).map(|(_, s)| *s).collect();
    let sql = format!(
        "SELECT CAST(ifNull(toString(c), 'NULL') AS String) AS s FROM wire.{table} ORDER BY n"
    );
    for (name, client) in [("tcp", tcp), ("http", http)] {
        let got = client
            .fetch_columns(&sql)
            .await
            .and_then(|c| c.get::<String>("s"))
            .map_err(|e| format!("{name} SELECT failed: {e}"))?;
        if got != want {
            return Err(format!("{name} read {got:?}, wanted {want:?}"));
        }
    }
    Ok(())
}

/// Every DFE column type must insert through the dynamic path on both
/// transports, with the schema each reads from `system.columns`, and read back
/// as the value that went in.
#[tokio::test]
async fn every_dfe_type_round_trips_through_the_dynamic_insert() {
    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let (tcp, http) = (server.tcp(), server.http());
    tcp.execute("CREATE DATABASE IF NOT EXISTS wire")
        .await
        .unwrap();

    let mut failures = Vec::new();
    for (i, case) in DFE_TYPES.iter().enumerate() {
        if let Err(e) = dfe_type_round_trip(&tcp, &http, case, i).await {
            failures.push(format!("{}: {e}", case.ch_type));
        }
    }

    server.stop().await;
    assert!(
        failures.is_empty(),
        "{} of {} types failed:\n{}",
        failures.len(),
        DFE_TYPES.len(),
        failures.join("\n")
    );
}

/// Write `rows` of `(n, d)` through the dynamic insert, leaving `d` out of the
/// row map where it is `None`.
async fn write_n_d(
    client: &UnifiedClient,
    table: &str,
    rows: &[Option<&str>],
) -> Result<u64, dfe_clickhouse::Error> {
    use dfe_clickhouse::dynamic::DynamicSchemaCache;
    use serde_json::{Map, Value};

    let cache = DynamicSchemaCache::new(Duration::from_secs(60));
    let mut insert = client.dynamic_insert("wire", table, cache).await?;
    for (n, d) in (0u32..).zip(rows) {
        let mut row = Map::new();
        row.insert("n".to_string(), Value::from(n));
        if let Some(d) = d {
            row.insert("d".to_string(), Value::from(*d));
        }
        insert.write_map(&row).await?;
    }
    Ok(insert.end().await?)
}

/// A defaulted column takes the row's value where the row carries one and the
/// server's default where it does not, whichever shape the first row has. Over
/// HTTP the whole batch is still one INSERT.
#[tokio::test]
async fn a_defaulted_column_is_kept_or_defaulted_row_by_row() {
    const ORDERS: [&[Option<&str>]; 2] = [
        &[None, Some("given"), None],
        &[Some("given"), None, Some("again")],
    ];

    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let (tcp, http) = (server.tcp(), server.http());
    tcp.execute("CREATE DATABASE IF NOT EXISTS wire")
        .await
        .unwrap();

    let mut failures = Vec::new();
    for (name, client) in [("http", &http), ("tcp", &tcp)] {
        for (i, rows) in ORDERS.iter().enumerate() {
            let table = format!("defaults_{name}_{i}");
            tcp.execute(&format!(
                "CREATE OR REPLACE TABLE wire.{table} \
                 (n UInt32, d String DEFAULT 'dflt') ENGINE = MergeTree ORDER BY n"
            ))
            .await
            .unwrap();
            match write_n_d(client, &table, rows).await {
                Ok(3) => {}
                other => failures.push(format!("{name} {rows:?} insert: {other:?}")),
            }
            let want: Vec<&str> = rows.iter().map(|d| d.unwrap_or("dflt")).collect();
            let got = tcp
                .fetch_columns(&format!("SELECT d FROM wire.{table} ORDER BY n"))
                .await
                .and_then(|c| c.get::<String>("d"));
            if !got.as_ref().is_ok_and(|got| *got == want) {
                failures.push(format!("{name} {rows:?} read {got:?}, wanted {want:?}"));
            }
        }
    }

    tcp.execute("SYSTEM FLUSH LOGS").await.unwrap();
    for i in 0..ORDERS.len() {
        let inserts = tcp
            .fetch_columns(&format!(
                "SELECT count() AS c FROM system.query_log WHERE type = 'QueryFinish' \
                 AND query LIKE 'INSERT INTO `wire`.`defaults_http_{i}`%'"
            ))
            .await
            .and_then(|c| c.get::<u64>("c"));
        if !inserts.as_ref().is_ok_and(|c| *c == [1]) {
            failures.push(format!(
                "http order {i}: INSERT count {inserts:?}, wanted 1"
            ));
        }
    }

    server.stop().await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// An HTTP dynamic insert sends its rows as it goes: the server is running the
/// INSERT while rows are still being written, so the batch is never held whole.
#[tokio::test]
async fn an_http_dynamic_insert_streams_before_it_ends() {
    use dfe_clickhouse::dynamic::DynamicSchemaCache;
    use serde_json::{Map, Value};

    const ROWS: u32 = 64;
    const RUNNING: &str = "SELECT count() AS c FROM system.processes \
                           WHERE query LIKE 'INSERT INTO `wire`.`streamed`%'";

    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let (tcp, http) = (server.tcp(), server.http());
    tcp.execute("CREATE DATABASE IF NOT EXISTS wire")
        .await
        .unwrap();
    tcp.execute(
        "CREATE OR REPLACE TABLE wire.streamed (n UInt32, s String) \
         ENGINE = MergeTree ORDER BY n",
    )
    .await
    .unwrap();

    // 64 rows of 64 KiB: 4 MiB, several flushes past any sane buffer.
    let wide = "x".repeat(64 * 1024);
    let cache = DynamicSchemaCache::new(Duration::from_secs(60));
    let mut insert = http
        .dynamic_insert("wire", "streamed", cache)
        .await
        .unwrap();
    for n in 0..ROWS {
        let mut row = Map::new();
        row.insert("n".to_string(), Value::from(n));
        row.insert("s".to_string(), Value::from(wide.as_str()));
        insert.write_map(&row).await.unwrap();
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut running = 0;
    while running == 0 && tokio::time::Instant::now() < deadline {
        running = tcp
            .fetch_columns(RUNNING)
            .await
            .and_then(|c| c.get::<u64>("c"))
            .unwrap()[0];
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let written = insert.end().await;
    let stored = tcp
        .fetch_columns("SELECT count() AS c FROM wire.streamed")
        .await
        .and_then(|c| c.get::<u64>("c"));
    server.stop().await;

    assert!(
        running > 0,
        "no INSERT reached the server before end(): the whole batch sat in memory"
    );
    assert_eq!(written.unwrap(), u64::from(ROWS));
    assert_eq!(stored.unwrap(), [u64::from(ROWS)]);
}

/// A transient server rejection stays retriable on both transports. A table
/// capped at one part answers its second insert with code 252, `TOO_MANY_PARTS`.
#[tokio::test]
async fn a_transient_server_rejection_is_retriable_on_both_transports() {
    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let (tcp, http) = (server.tcp(), server.http());
    tcp.execute("CREATE DATABASE IF NOT EXISTS wire")
        .await
        .unwrap();

    let mut failures = Vec::new();
    for (name, client) in [("http", &http), ("tcp", &tcp)] {
        let table = format!("one_part_{name}");
        tcp.execute(&format!(
            "CREATE OR REPLACE TABLE wire.{table} (n UInt32, d String) \
             ENGINE = MergeTree ORDER BY n \
             SETTINGS parts_to_delay_insert = 1, parts_to_throw_insert = 1"
        ))
        .await
        .unwrap();
        if let Err(e) = write_n_d(client, &table, &[Some("first")]).await {
            failures.push(format!("{name} first insert: {e}"));
            continue;
        }
        match write_n_d(client, &table, &[Some("second")]).await {
            Err(e) if e.is_retriable() && e.to_string().contains("252") => {}
            other => failures.push(format!("{name} second insert: {other:?}")),
        }
    }

    server.stop().await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A failover list must verify each endpoint against its own host. The
/// certificate is valid for `localhost` alone, and `127.0.0.1` stands in for
/// a second host that the certificate does not name.
#[cfg(feature = "tls")]
#[tokio::test]
async fn tls_failover_verifies_each_endpoint_against_its_own_host() {
    use dfe_clickhouse::tls::TlsTrust;

    require_docker!();
    let _one = ONE_SERVER.acquire().await.unwrap();
    let server = server().await;
    let secure = server.secure_port;
    let down = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let ca = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(ca.path(), &server.tls_ca).unwrap();
    let client = |addr: String, name: &str| {
        let mut trust = TlsTrust::default();
        trust.extra_roots = vec![ca.path().to_path_buf()];
        trust.exclusive = true;
        TcpClient::new_tls(addr, name)
            .with_tls_trust(trust)
            .with_pool_create_timeout(Some(Duration::from_secs(10)))
    };

    let first_down = client(format!("127.0.0.1:{down}"), "127.0.0.1")
        .with_addrs([format!("127.0.0.1:{down}"), format!("localhost:{secure}")])
        .ping()
        .await;
    let by_address = client(format!("127.0.0.1:{secure}"), "127.0.0.1")
        .ping()
        .await;
    let overridden = client(format!("127.0.0.1:{secure}"), "localhost")
        .ping()
        .await;
    let wrong_override = client(format!("localhost:{secure}"), "wrong.example")
        .ping()
        .await;
    server.stop().await;

    first_down.expect("the second endpoint verifies against its own name");
    by_address.expect_err("a name the certificate does not carry is refused");
    overridden.expect("an explicit name holds for an endpoint given by address");
    wrong_override.expect_err("an explicit name holds even when it is wrong");
}
