// Project:   dfe-clickhouse
// File:      src/ext.rs
// Purpose:   Extension traits on clickhouse::Client
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Extensions to [`clickhouse::Client`], built only on its public API.
//! Upstream flattens every rejection into [`Error::BadResponse`] text, so
//! branching on an error code means reading it back out.

use std::future::Future;

use clickhouse::Client;
use clickhouse::error::{Error, Result};

/// A `Code: N. DB::Exception: ...` rejection out of [`Error::BadResponse`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerException {
    /// Signed on the wire; small positives in practice.
    pub code: i32,
    /// The trailing `(UPPERCASE_NAME)` tag; older and proxied errors omit it.
    pub name: Option<String>,
    /// Body without the code prefix, the marker, the name tag and the version.
    pub message: String,
    /// The `Stack trace:` section, when the server included one.
    pub stack_trace: Option<String>,
}

impl ServerException {
    /// `None` for any other [`Error`] variant, and for a body that does not open
    /// with `Code: N` -- a proxy page or a bare status line. A `Code: N` with
    /// nothing after it parses with an empty `message`.
    #[must_use]
    pub fn parse(error: &Error) -> Option<Self> {
        let Error::BadResponse(body) = error else {
            return None;
        };
        let body = body.trim();

        // Upstream's `collect_response` in `response.rs` writes the
        // `X-ClickHouse-Exception-Code` header as `Code: {code}` for a body that
        // is empty, not UTF-8, or fails mid-read.
        if let Some(code) = body
            .strip_prefix("Code: ")
            .and_then(|c| c.parse::<i32>().ok())
        {
            return Some(Self {
                code,
                name: None,
                message: String::new(),
                stack_trace: None,
            });
        }

        let code: i32 = body
            .strip_prefix("Code: ")?
            .split_once('.')
            .map(|(code, _)| code.trim())?
            .parse()
            .ok()?;

        // `DB::Exception`, `DB::NetException` and the rest share this marker.
        let after_marker = body
            .split_once("Exception: ")
            .map_or(body.trim(), |(_, rest)| rest.trim());

        let without_version = match after_marker.rfind("(version ") {
            Some(i) => after_marker[..i].trim_end_matches([' ', '.', ',']),
            None => after_marker,
        };

        // Only a SHOUTY trailing paren run is the name tag; see the test below.
        let mut without_name = without_version;
        let mut name = None;
        if let Some(open) = without_version.strip_suffix(')').and_then(|s| s.rfind('(')) {
            let candidate = &without_version[open + 1..without_version.len() - 1];
            let shouty = candidate
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_');
            if !candidate.is_empty() && shouty {
                without_name = without_version[..open].trim_end_matches([':', ' ', '.', ',']);
                name = Some(candidate.to_owned());
            }
        }

        let (message, stack_trace) = match without_name.find("Stack trace:") {
            Some(i) => (
                without_name[..i].trim_end().to_owned(),
                Some(without_name[i + "Stack trace:".len()..].trim().to_owned()),
            ),
            None => (without_name.trim().to_owned(), None),
        };

        (!message.is_empty()).then_some(Self {
            code,
            name,
            message,
            stack_trace,
        })
    }

    /// Off the same table as [`crate::Error::is_retriable_code`].
    #[must_use]
    pub fn is_retriable(&self) -> bool {
        crate::Error::is_retriable_code(self.code)
    }
}

/// Statement-level controls upstream's `Client` carries only as raw settings.
/// `Sized` because the `with_*` methods consume and return the client.
pub trait ClientExt: Sized {
    /// `SELECT 1`; errors on a transport failure or a bad credential.
    fn ping(&self) -> impl Future<Output = Result<()>> + Send;

    /// Stop `query_id` and wait; matching nothing is not an error.
    fn kill_query(&self, query_id: &str) -> impl Future<Output = Result<()>> + Send;

    /// What `system.query_log` records and [`Self::kill_query`] matches on.
    #[must_use]
    fn with_query_id(self, query_id: impl Into<String>) -> Self;

    /// Bind statements to a session, so `SET` and temp tables survive between.
    #[must_use]
    fn with_session_id(self, session_id: impl Into<String>) -> Self;

    /// Run statements under `role`. `Client::with_default_roles` clears it.
    #[must_use]
    fn with_role(self, role: impl Into<String>) -> Self;
}

impl ClientExt for Client {
    async fn ping(&self) -> Result<()> {
        self.query("SELECT 1").execute().await
    }

    async fn kill_query(&self, query_id: &str) -> Result<()> {
        self.query("KILL QUERY WHERE query_id = ? SYNC")
            .bind(query_id)
            .execute()
            .await
    }

    fn with_query_id(self, query_id: impl Into<String>) -> Self {
        self.with_setting("query_id", query_id)
    }

    fn with_session_id(self, session_id: impl Into<String>) -> Self {
        self.with_setting("session_id", session_id)
    }

    fn with_role(self, role: impl Into<String>) -> Self {
        // Upstream's `clear_roles` removes this exact key, so the clear works.
        self.with_setting("role", role)
    }
}

#[cfg(test)]
mod tests {
    use clickhouse::test::{Mock, handlers};

    use super::*;

    /// The shape a modern server sends: code, marker, message, name, version.
    #[test]
    fn parse_reads_code_name_and_message() {
        let err = Error::BadResponse(
            "Code: 469. DB::Exception: Constraint `x_lt_10` for table t is violated at row 5000. \
             (VIOLATED_CONSTRAINT) (version 26.2.4.23 (official build))"
                .into(),
        );
        let exc = ServerException::parse(&err).expect("a well-formed exception body parses");
        assert_eq!(exc.code, 469);
        assert_eq!(exc.name.as_deref(), Some("VIOLATED_CONSTRAINT"));
        assert_eq!(
            exc.message,
            "Constraint `x_lt_10` for table t is violated at row 5000"
        );
        assert!(exc.stack_trace.is_none());
    }

    /// The socket pair a `NetException` ends in is not a name tag.
    #[test]
    fn parse_keeps_trailing_parens_that_are_not_a_name_tag() {
        let err = Error::BadResponse(
            "Code: 210. DB::NetException: I/O error: Broken pipe, while writing to socket \
             (127.0.0.1:9000 -> 127.0.0.1:54646). (NETWORK_ERROR) (version 23.8.8.20)"
                .into(),
        );
        let exc = ServerException::parse(&err).expect("a NetException body parses");
        assert_eq!(exc.code, 210);
        assert_eq!(exc.name.as_deref(), Some("NETWORK_ERROR"));
        assert_eq!(
            exc.message,
            "I/O error: Broken pipe, while writing to socket (127.0.0.1:9000 -> 127.0.0.1:54646)"
        );
    }

    #[test]
    fn parse_splits_the_stack_trace_out_of_the_message() {
        let err = Error::BadResponse(
            "Code: 36. DB::Exception: Bad arguments. Stack trace:\n0. ./Common/Exception.cpp:99\n\
             1. ./Functions/foo.cpp:42\n (BAD_ARGUMENTS) (version 26.2.4.23 (official build))"
                .into(),
        );
        let exc = ServerException::parse(&err).expect("a body with a trace parses");
        assert_eq!(exc.code, 36);
        assert_eq!(exc.message, "Bad arguments.");
        let trace = exc.stack_trace.expect("the trace section is captured");
        assert!(trace.contains("Exception.cpp:99"), "got {trace:?}");
        assert!(trace.contains("foo.cpp:42"), "got {trace:?}");
    }

    #[test]
    fn parse_tolerates_a_missing_name_tag() {
        let err = Error::BadResponse(
            "Code: 999. DB::Exception: Generic problem (version 26.2.4.23 (official build))".into(),
        );
        let exc = ServerException::parse(&err).expect("a body without a name tag parses");
        assert_eq!(exc.code, 999);
        assert!(exc.name.is_none());
        assert_eq!(exc.message, "Generic problem");
    }

    #[test]
    fn parse_declines_anything_that_is_not_an_exception_body() {
        for body in [
            "404 Not Found",
            "",
            "DB::Exception: oops",
            // A code but no message left after stripping.
            "Code: 60. DB::Exception: (UNKNOWN_TABLE)",
            // A bare number is a proxy or status body, because upstream always
            // writes the `Code: ` prefix when it substitutes the header.
            "117",
            "Code: ",
            "Code: x",
        ] {
            let err = Error::BadResponse(body.into());
            assert!(
                ServerException::parse(&err).is_none(),
                "{body:?} should not parse"
            );
        }
        assert!(ServerException::parse(&Error::TimedOut).is_none());
    }

    /// Upstream's `collect_response` writes `Code: {header}` with no message
    /// when the body is empty or unreadable, and declining it loses the only
    /// field a caller classifies on.
    #[test]
    fn parse_accepts_the_code_only_body_upstream_substitutes() {
        let exc = ServerException::parse(&Error::BadResponse("Code: 117".into()))
            .expect("a code-only body is a rejection, not junk");
        assert_eq!(exc.code, 117);
        assert_eq!(exc.name, None);
        assert!(exc.message.is_empty(), "no message is on the wire to read");
        assert!(exc.stack_trace.is_none());
        assert!(
            !exc.is_retriable(),
            "117 is bad data, not a transient fault"
        );

        let exc = ServerException::parse(&Error::BadResponse(" Code: 202 ".into()))
            .expect("a padded code-only body still parses");
        assert_eq!(exc.code, 202);
        assert!(exc.is_retriable());
    }

    #[test]
    fn is_retriable_shares_the_error_code_table() {
        let exc = |code| ServerException {
            code,
            name: None,
            message: "x".into(),
            stack_trace: None,
        };
        // 159 = TIMEOUT_EXCEEDED, 60 = UNKNOWN_TABLE.
        assert!(exc(159).is_retriable());
        assert!(!exc(60).is_retriable());
    }

    #[tokio::test]
    async fn ping_sends_select_1() {
        let mock = Mock::new();
        let recorded = mock.add(handlers::record_ddl());
        let client = Client::default().with_url(mock.url());

        client.ping().await.expect("the mock answers 200");

        assert_eq!(recorded.query().await, "SELECT 1");
    }

    #[tokio::test]
    async fn kill_query_binds_and_quotes_the_query_id() {
        let mock = Mock::new();
        let recorded = mock.add(handlers::record_ddl());
        let client = Client::default().with_url(mock.url());

        client
            .kill_query("q-1")
            .await
            .expect("the mock answers 200");

        assert_eq!(
            recorded.query().await,
            "KILL QUERY WHERE query_id = 'q-1' SYNC"
        );
    }

    #[test]
    fn the_statement_knobs_land_as_settings() {
        let client = Client::default()
            .with_query_id("q-1")
            .with_session_id("s-1")
            .with_role("reader");

        assert_eq!(client.get_setting("query_id"), Some("q-1"));
        assert_eq!(client.get_setting("session_id"), Some("s-1"));
        assert_eq!(client.get_setting("role"), Some("reader"));
        assert_eq!(client.with_default_roles().get_setting("role"), None);
    }
}
