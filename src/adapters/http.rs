//! Ops endpoints: `/metrics` (Prometheus), `/healthz` (process alive),
//! `/readyz` (streaming from the slot and able to publish).

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};

use crate::app::Health;

/// Installs the process-wide metrics recorder. Call once, before anything records.
pub fn install_metrics() -> anyhow::Result<PrometheusHandle> {
    let latency_buckets = [
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
    ];
    Ok(PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full("pg_outbox_publish_latency_seconds".into()),
            &latency_buckets,
        )?
        .install_recorder()?)
}

pub async fn serve(
    listen: SocketAddr,
    metrics: PrometheusHandle,
    health: Arc<Health>,
) -> anyhow::Result<()> {
    let app = Router::new()
        .route(
            "/metrics",
            get(move || std::future::ready(metrics.render())),
        )
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/readyz",
            get(move || {
                let ready = health.is_ready();
                async move {
                    if ready {
                        (StatusCode::OK, "ready")
                    } else {
                        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
                    }
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("cannot listen on {listen}"))?;
    tracing::info!(%listen, "serving /metrics, /healthz and /readyz");
    axum::serve(listener, app).await?;
    Ok(())
}
