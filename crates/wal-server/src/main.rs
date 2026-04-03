//! `wal-server` — standalone deployable WAL node.
//!
//! Runs a single Raft node that participates in a cluster and exposes:
//!   - gRPC port  (Raft + WAL RPCs)
//!   - HTTP port  (health / metrics / status)
//!
//! # Example: 3-node cluster on localhost
//!
//! ```sh
//! # Terminal 1
//! wal-server \
//!   --id node-1 \
//!   --addr http://127.0.0.1:7001 \
//!   --grpc-port 7001 \
//!   --peers http://127.0.0.1:7002,http://127.0.0.1:7003 \
//!   --peer-ids node-2,node-3 \
//!   --data-dir /tmp/wal-node-1
//!
//! # Terminal 2
//! wal-server --id node-2 --addr http://127.0.0.1:7002 --grpc-port 7002 \
//!   --peers http://127.0.0.1:7001,http://127.0.0.1:7003 \
//!   --peer-ids node-1,node-3 --data-dir /tmp/wal-node-2
//!
//! # Terminal 3
//! wal-server --id node-3 --addr http://127.0.0.1:7003 --grpc-port 7003 \
//!   --peers http://127.0.0.1:7001,http://127.0.0.1:7002 \
//!   --peer-ids node-1,node-2 --data-dir /tmp/wal-node-3
//! ```

use std::net::SocketAddr;

use clap::Parser;
use http_body_util::Full;
use hyper::{body::Bytes, server::conn::http1, service::service_fn, Request, Response};
use hyper_util::rt::TokioIo;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::net::TcpListener;
use tracing::{info, warn};
use wal_replication::{ClusterConfig, NodeInfo, RaftNode, start_server};

// ── CLI ────────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "wal-server", about = "Standalone WAL node with Raft replication")]
struct Args {
    /// Unique node identifier (e.g. "node-1")
    #[arg(long)]
    id: String,

    /// Advertised address of *this* node, used by peers for gRPC connections
    /// (e.g. "http://127.0.0.1:7001")
    #[arg(long)]
    addr: String,

    /// Port the gRPC server listens on
    #[arg(long, default_value_t = 7001)]
    grpc_port: u16,

    /// Comma-separated HTTP addresses of *all other* nodes
    /// (e.g. "http://127.0.0.1:7002,http://127.0.0.1:7003")
    #[arg(long, value_delimiter = ',')]
    peers: Vec<String>,

    /// Comma-separated IDs matching --peers (same order)
    #[arg(long, value_delimiter = ',')]
    peer_ids: Vec<String>,

    /// Directory for WAL segments and persistent Raft state
    #[arg(long, default_value = "/tmp/wal-server")]
    data_dir: String,

    /// Port the HTTP health/metrics server listens on
    #[arg(long, default_value_t = 8080)]
    http_port: u16,
}

// ── Entry point ────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Structured logging — controlled via RUST_LOG env var
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "wal_server=info,wal_replication=info".into()),
        )
        .init();

    let args = Args::parse();
    validate_args(&args)?;

    // ── Prometheus metrics ──
    let recorder = PrometheusBuilder::new().build_recorder();
    let prometheus_handle = recorder.handle();
    metrics::set_global_recorder(recorder).expect("metrics recorder already installed");

    // ── Build cluster config ──
    let peers: Vec<NodeInfo> = args
        .peer_ids
        .iter()
        .zip(args.peers.iter())
        .map(|(id, addr)| NodeInfo { id: id.clone(), addr: addr.clone() })
        .collect();

    let this_node = NodeInfo { id: args.id.clone(), addr: args.addr.clone() };

    let config = ClusterConfig::new(this_node, peers, &args.data_dir);

    // ── Start Raft node ──
    let handle = RaftNode::start(config)?;
    info!(node_id = %args.id, grpc_port = args.grpc_port, "Raft node started");

    // ── HTTP health server ──
    let http_addr: SocketAddr = format!("0.0.0.0:{}", args.http_port).parse()?;
    let handle_clone = handle.clone();
    tokio::spawn(async move {
        if let Err(e) = run_http_server(http_addr, handle_clone, prometheus_handle).await {
            warn!("HTTP server error: {e}");
        }
    });

    // ── gRPC server (blocks until shutdown) ──
    let grpc_addr: SocketAddr = format!("0.0.0.0:{}", args.grpc_port).parse()?;
    info!(%grpc_addr, "gRPC server listening");
    start_server(handle, grpc_addr).await?;

    Ok(())
}

// ── HTTP server ────────────────────────────────────────────────────────────────

async fn run_http_server(
    addr: SocketAddr,
    handle: wal_replication::RaftHandle,
    prometheus: metrics_exporter_prometheus::PrometheusHandle,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "HTTP health server listening");

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let h = handle.clone();
        let p = prometheus.clone();

        tokio::spawn(async move {
            if let Err(e) = http1::Builder::new()
                .serve_connection(io, service_fn(move |req| handle_http(req, h.clone(), p.clone())))
                .await
            {
                warn!("HTTP connection error: {e}");
            }
        });
    }
}

async fn handle_http(
    req: Request<hyper::body::Incoming>,
    _handle: wal_replication::RaftHandle,
    prometheus: metrics_exporter_prometheus::PrometheusHandle,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let path = req.uri().path().to_owned();

    let (status, body) = match path.as_str() {
        "/health" | "/healthz" => {
            let body = serde_json::json!({ "status": "ok" }).to_string();
            (200u16, body)
        }
        "/metrics" => {
            let body = prometheus.render();
            (200, body)
        }
        "/status" => {
            let body = serde_json::json!({
                "status": "ok",
                "service": "wal-server",
            })
            .to_string();
            (200, body)
        }
        _ => {
            let body = serde_json::json!({
                "error": "not found",
                "available": ["/health", "/metrics", "/status"],
            })
            .to_string();
            (404, body)
        }
    };

    Ok(Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .unwrap())
}

// ── Validation ─────────────────────────────────────────────────────────────────

fn validate_args(args: &Args) -> anyhow::Result<()> {
    if args.id.is_empty() {
        anyhow::bail!("--id must not be empty");
    }
    if args.peer_ids.len() != args.peers.len() {
        anyhow::bail!(
            "--peer-ids and --peers must have the same number of entries (got {} and {})",
            args.peer_ids.len(),
            args.peers.len()
        );
    }
    Ok(())
}
