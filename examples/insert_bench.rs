// Project:   dfe-clickhouse
// File:      examples/insert_bench.rs
// Purpose:   Measure dynamic inserts over HTTP RowBinary against native TCP
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Insert the same DFE-shaped rows through `UnifiedClient::dynamic_insert`
//! over HTTP (RowBinary, LZ4 on and off) and native TCP (Native columnar), and
//! report the median client CPU, server CPU, bytes received and elapsed time
//! per batch. The results are in `docs/insert-formats.md`.
//!
//! ```text
//! cargo run --release --features full --example insert_bench
//! ```
//!
//! `BENCH_HOST` (127.0.0.1), `BENCH_HTTP_PORT` (8123), `BENCH_NATIVE_PORT`
//! (9000), `BENCH_RUNS` (7) and `BENCH_ROWS` (10000,100000) override the
//! defaults. It creates and drops the `dfe_bench` database. Client CPU is read
//! from `/proc`, so it prints `n/a` off Linux.

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clickhouse::{Client, Compression};
use dfe_clickhouse::dynamic::DynamicSchemaCache;
use dfe_clickhouse::{TcpClient, UnifiedClient};
use serde_json::{Map, Value, json};

type Res<T> = Result<T, Box<dyn Error>>;

const CREATE: &str = "CREATE OR REPLACE TABLE dfe_bench.events (
    ts DateTime64(3, 'UTC'),
    host LowCardinality(String),
    message String,
    status UInt16,
    bytes UInt64,
    delta Int32,
    user Nullable(String),
    labels Map(LowCardinality(String), String),
    tags Array(String),
    doc JSON(max_dynamic_paths=2048)
) ENGINE = MergeTree ORDER BY ts";

/// One way of sending the batch.
#[derive(Clone, Copy)]
enum Path {
    HttpLz4,
    HttpPlain,
    Native,
}

impl Path {
    const ALL: [Self; 3] = [Self::HttpLz4, Self::HttpPlain, Self::Native];

    fn label(self) -> &'static str {
        match self {
            Self::HttpLz4 => "HTTP RowBinary, LZ4",
            Self::HttpPlain => "HTTP RowBinary, no compression",
            Self::Native => "native TCP, Native (no compression)",
        }
    }
}

struct Target {
    host: String,
    http_port: u16,
    native_port: u16,
}

impl Target {
    /// A client for `path` whose queries carry `tag` in `system.query_log`.
    fn client(&self, path: Path, tag: &str) -> UnifiedClient {
        let http = |compression| {
            UnifiedClient::Http(
                Client::default()
                    .with_url(format!("http://{}:{}", self.host, self.http_port))
                    .with_compression(compression)
                    .with_setting("async_insert", "0")
                    .with_setting("log_comment", tag),
            )
        };
        match path {
            Path::HttpLz4 => http(Compression::Lz4),
            Path::HttpPlain => http(Compression::None),
            Path::Native => UnifiedClient::Tcp(
                TcpClient::new(format!("{}:{}", self.host, self.native_port))
                    .with_setting("async_insert", "0")
                    .with_setting("log_comment", tag),
            ),
        }
    }
}

/// CPU time of every thread in this process, in nanoseconds.
fn process_cpu_ns() -> Option<u64> {
    let tasks = std::fs::read_dir("/proc/self/task").ok()?;
    let mut total = 0;
    for task in tasks.flatten() {
        let stat = std::fs::read_to_string(task.path().join("schedstat")).ok()?;
        total += stat.split_whitespace().next()?.parse::<u64>().ok()?;
    }
    Some(total)
}

/// Deterministic rows shaped like a DFE web-access event.
fn rows(n: u64) -> Vec<Map<String, Value>> {
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        state >> 33
    };
    (0..n)
        .map(|i| {
            let r = next();
            let ms = i % 3_600_000;
            let status = [200, 200, 200, 404, 500][usize::try_from(r % 5).unwrap_or(0)];
            let bytes = r % 1_000_000;
            let user = if i % 3 == 0 {
                Value::Null
            } else {
                json!(format!("user-{}", r % 500))
            };
            let row = json!({
                "ts": format!("2026-11-01T08:{:02}:{:02}.{:03}Z", ms / 60_000, ms / 1_000 % 60, ms % 1_000),
                "host": format!("host-{:02}", r % 16),
                "message": format!("GET /api/v1/items/{i} HTTP/1.1 {status} {bytes} \"Mozilla/5.0 (X11; Linux x86_64)\" rid={r:016x}"),
                "status": status,
                "bytes": bytes,
                "delta": i64::try_from(r % 2001).unwrap_or(0) - 1000,
                "user": user,
                "labels": {"env": "prod", "region": "ap-southeast-2", "team": format!("t{}", r % 8)},
                "tags": ["beats", "filebeat", format!("t{}", r % 4)],
                "doc": {
                    "event": {"kind": "event", "dataset": "nginx.access"},
                    "http": {"method": "GET", "status": status},
                    "source": {"ip": format!("10.0.{}.{}", r % 256, (r >> 8) % 256)},
                    "seq": i,
                },
            });
            match row {
                Value::Object(map) => map,
                _ => Map::new(),
            }
        })
        .collect()
}

/// What the client saw for one batch.
struct ClientSide {
    tag: String,
    cpu_ns: Option<u64>,
    elapsed: Duration,
}

async fn one_batch(
    client: &UnifiedClient,
    batch: &[Map<String, Value>],
    cache: &Arc<DynamicSchemaCache>,
    tag: String,
) -> Res<ClientSide> {
    let (cpu0, t0) = (process_cpu_ns(), Instant::now());
    let mut insert = client
        .dynamic_insert("dfe_bench", "events", Arc::clone(cache))
        .await?;
    for row in batch {
        insert.write_map(row).await?;
    }
    insert.end().await?;
    let elapsed = t0.elapsed();
    let cpu_ns = process_cpu_ns()
        .zip(cpu0)
        .map(|(after, before)| after - before);
    Ok(ClientSide {
        tag,
        cpu_ns,
        elapsed,
    })
}

/// One measured batch, client and server sides together.
struct Sample {
    client_cpu_ms: Option<f64>,
    elapsed_ms: f64,
    server_user_ms: f64,
    server_sys_ms: f64,
    server_os_cpu_ms: f64,
    received_bytes: f64,
}

/// The server's account of the insert `side` tagged.
#[allow(clippy::cast_precision_loss)]
async fn server_side(admin: &UnifiedClient, side: &ClientSide) -> Res<Sample> {
    let sql = format!(
        "SELECT ProfileEvents['UserTimeMicroseconds'] AS user_us, \
         ProfileEvents['SystemTimeMicroseconds'] AS sys_us, \
         ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS os_us, \
         ProfileEvents['NetworkReceiveBytes'] AS recv \
         FROM system.query_log \
         WHERE log_comment = '{}' AND type = 'QueryFinish' AND query_kind = 'Insert'",
        side.tag
    );
    let got = admin.fetch_columns(&sql).await?;
    let one = |name: &str| -> Res<f64> {
        match got.get::<u64>(name)?.as_slice() {
            [v] => Ok(*v as f64),
            other => Err(format!("{}: {} query_log rows", side.tag, other.len()).into()),
        }
    };
    Ok(Sample {
        client_cpu_ms: side.cpu_ns.map(|ns| ns as f64 / 1e6),
        elapsed_ms: side.elapsed.as_secs_f64() * 1e3,
        server_user_ms: one("user_us")? / 1e3,
        server_sys_ms: one("sys_us")? / 1e3,
        server_os_cpu_ms: one("os_us")? / 1e3,
        received_bytes: one("recv")?,
    })
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    let mid = v.len() / 2;
    match v.len() {
        0 => f64::NAN,
        n if n % 2 == 0 => f64::midpoint(v[mid - 1], v[mid]),
        _ => v[mid],
    }
}

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> Res<T> {
    match std::env::var(key) {
        Ok(raw) => raw
            .parse()
            .map_err(|_| format!("{key}={raw:?} does not parse").into()),
        Err(_) => Ok(default),
    }
}

fn ms(v: Option<f64>) -> String {
    v.map_or_else(|| "n/a".to_string(), |v| format!("{v:.1}"))
}

#[tokio::main]
async fn main() -> Res<()> {
    let target = Target {
        host: env_or("BENCH_HOST", "127.0.0.1".to_string())?,
        http_port: env_or("BENCH_HTTP_PORT", 8123)?,
        native_port: env_or("BENCH_NATIVE_PORT", 9000)?,
    };
    let runs: usize = env_or("BENCH_RUNS", 7)?;
    let batch_rows: Vec<u64> = env_or("BENCH_ROWS", "10000,100000".to_string())?
        .split(',')
        .map(str::parse)
        .collect::<Result<_, _>>()?;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    let admin = target.client(Path::HttpPlain, "bench-admin");
    admin
        .execute("CREATE DATABASE IF NOT EXISTS dfe_bench")
        .await?;
    let version = admin
        .fetch_columns("SELECT version() AS v")
        .await?
        .get::<String>("v")?
        .concat();
    println!("server {version}, {runs} measured runs per cell after one warm-up, median shown");
    println!(
        "raw: path,rows,run,client_cpu_ms,elapsed_ms,server_user_ms,server_sys_ms,server_os_cpu_ms,received_bytes"
    );

    let mut table = Vec::new();
    for &n in &batch_rows {
        let batch = rows(n);
        for path in Path::ALL {
            admin.execute(CREATE).await?;
            let cache = DynamicSchemaCache::new(Duration::from_secs(3600));
            let mut sides = Vec::new();
            for run in 0..=runs {
                let tag = format!("bench-{nonce}-{n}-{}-{run}", path as u8);
                let client = target.client(path, &tag);
                let side = one_batch(&client, &batch, &cache, tag).await?;
                // Run 0 warms the schema cache and the server, and is discarded.
                if run > 0 {
                    sides.push(side);
                }
            }
            admin.execute("SYSTEM FLUSH LOGS").await?;
            let mut samples = Vec::new();
            for (run, side) in (1..).zip(&sides) {
                let s = server_side(&admin, side).await?;
                println!(
                    "raw: {},{n},{run},{},{:.2},{:.2},{:.2},{:.2},{}",
                    path.label(),
                    ms(s.client_cpu_ms),
                    s.elapsed_ms,
                    s.server_user_ms,
                    s.server_sys_ms,
                    s.server_os_cpu_ms,
                    s.received_bytes
                );
                samples.push(s);
            }
            let col = |f: fn(&Sample) -> f64| median(samples.iter().map(f).collect());
            let client_cpu = samples
                .iter()
                .map(|s| s.client_cpu_ms)
                .collect::<Option<Vec<f64>>>()
                .map(median);
            table.push(format!(
                "| {} | {n} | {} | {:.1} | {:.1} | {:.0} | {:.1} |",
                path.label(),
                ms(client_cpu),
                col(|s| s.server_user_ms + s.server_sys_ms),
                col(|s| s.server_os_cpu_ms),
                col(|s| s.received_bytes),
                col(|s| s.elapsed_ms),
            ));
        }
    }
    admin.execute("DROP DATABASE dfe_bench").await?;

    println!();
    println!(
        "| Path | Rows | Client CPU ms | Server user+sys ms | Server OSCPUVirtual ms | Bytes received | Elapsed ms |"
    );
    println!("|---|---|---|---|---|---|---|");
    for line in table {
        println!("{line}");
    }
    Ok(())
}
