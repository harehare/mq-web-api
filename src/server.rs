use std::{future::IntoFuture, net::SocketAddr, sync::Arc, sync::atomic::Ordering};
use tokio::signal;
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[cfg(feature = "otel")]
static TRACER_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> =
    std::sync::OnceLock::new();

use crate::{
    auth::ApiKeyStore,
    cleanup::CleanupService,
    config::{Config, LogFormat},
    handlers::SHUTTING_DOWN,
    metrics::metrics,
    rate_limiter::RateLimiter,
    routes::create_router,
};

/// Builds and installs a global tracing subscriber.
///
/// When the `otel` feature is enabled and `config.otel_endpoint` is set,
/// an OTLP span exporter is attached so traces are forwarded to the
/// configured OpenTelemetry collector.
pub fn init_tracing(config: &Config) {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| config.log_level.clone().into());

    #[cfg(feature = "otel")]
    if let Some(ref endpoint) = config.otel_endpoint {
        use opentelemetry::KeyValue;
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_otlp::WithExportConfig;
        use opentelemetry_sdk::{Resource, trace::SdkTracerProvider};

        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .expect("Failed to build OTLP span exporter");

        let resource = Resource::builder()
            .with_attribute(KeyValue::new(
                "service.name",
                config.otel_service_name.clone(),
            ))
            .build();

        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource)
            .build();

        opentelemetry::global::set_tracer_provider(provider.clone());

        // Store for graceful shutdown on server exit.
        let _ = TRACER_PROVIDER.set(provider.clone());

        // Create an otel layer per format branch to avoid type-inference conflicts
        // between JsonFields and DefaultFields formatter types.
        match config.log_format {
            LogFormat::Json => {
                tracing_subscriber::registry()
                    .with(env_filter)
                    .with(fmt::layer().json())
                    .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("mq-web-api")))
                    .init();
            }
            LogFormat::Text => {
                tracing_subscriber::registry()
                    .with(env_filter)
                    .with(fmt::layer())
                    .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("mq-web-api")))
                    .init();
            }
        }
        return;
    }

    match config.log_format {
        LogFormat::Json => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer().json())
                .init();
        }
        LogFormat::Text => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer())
                .init();
        }
    }
}

/// Routes panics into the structured log and the panic counter instead of
/// plain stderr. Handlers and blocking tasks are isolated from the process by
/// `CatchPanicLayer` and `JoinError`; this hook makes those panics visible.
fn install_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        metrics().record_panic();

        let payload = info.payload();
        let message = payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("<non-string panic payload>");
        let location = info
            .location()
            .map_or_else(String::new, |l| format!("{}:{}", l.file(), l.line()));
        error!(location = %location, "panic: {}", message);
    }));
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

/// Runs the server until a shutdown signal is received.
pub async fn start_server(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    install_panic_hook();
    info!("Starting mq-web-api server with config: {:?}", config);

    // Initialize rate limiter
    let rate_limiter = Arc::new(RateLimiter::new(config.rate_limit.clone()));
    info!("Rate limiter initialized successfully");

    let api_key_store = ApiKeyStore::from_config(&config.auth).map(Arc::new);
    match &api_key_store {
        Some(store) => info!("API key authentication enabled with {} key(s)", store.len()),
        None => info!("API key authentication disabled"),
    }

    let app = create_router(&config, rate_limiter.clone(), api_key_store);

    let bind_address = config.bind_address();
    let listener = tokio::net::TcpListener::bind(&bind_address)
        .await
        .unwrap_or_else(|_| panic!("Failed to bind to address {}", bind_address));

    let server_url = config.server_url();
    info!("Server running on {}", server_url);
    info!("OpenAPI docs available at {}/openapi.json", server_url);

    // Print available environment variables for configuration
    info!("Configuration options:");
    info!("  HOST: Host to bind to (default: 0.0.0.0)");
    info!("  PORT: Port to bind to (default: 8080)");
    info!("  RUST_LOG or MQ_LOG_LEVEL: Log level (default: mq_web_api=debug,tower_http=debug)");
    info!("  LOG_FORMAT: Log format - 'json' or 'text' (default: json)");
    info!("  CORS_ORIGINS: Comma-separated CORS origins (default: *)");
    info!("  RATE_LIMIT_REQUESTS_PER_WINDOW: Requests per window (default: 100)");
    info!("  RATE_LIMIT_WINDOW_SIZE_SECONDS: Window size in seconds (default: 3600)");
    info!("  RATE_LIMIT_CLEANUP_INTERVAL_SECONDS: Cleanup interval in seconds (default: 3600)");
    info!(
        "  QUERY_TIMEOUT_SECONDS: Max seconds a single query may run before it's aborted (default: 10)"
    );
    info!(
        "  MAX_REQUEST_BODY_SIZE: Max accepted HTTP request body size in bytes (default: 10485760)"
    );
    info!(
        "  REQUEST_TIMEOUT_SECONDS: Max seconds a single HTTP request may take end-to-end (default: 30)"
    );
    info!("  QUERY_CACHE_ENABLED: Cache repeated query/input combinations (default: true)");
    info!("  QUERY_CACHE_TTL_SECONDS: How long a cached result stays fresh (default: 30)");
    info!("  QUERY_CACHE_MAX_ENTRIES: Max number of cached query results (default: 1000)");
    info!(
        "  QUERY_CACHE_MAX_ENTRY_BYTES: Max bytes retained by one cache entry (default: 1048576)"
    );
    info!(
        "  QUERY_CACHE_MAX_TOTAL_BYTES: Max bytes retained by all cache entries (default: 33554432)"
    );
    info!(
        "  TRUSTED_PROXY_COUNT: Reverse proxies appending to X-Forwarded-For, 0 to ignore it (default: 1)"
    );
    info!("  MAX_CONCURRENT_QUERIES: Queries evaluated at the same time (default: 2 x CPU cores)");
    info!("  BATCH_TIMEOUT_SECONDS: Wall-clock budget of one batch request (default: 20)");
    info!("  METRICS_ENABLED: Serve Prometheus metrics at /metrics (default: false)");
    info!(
        "  SHUTDOWN_DRAIN_SECONDS: Time /ready reports 503 before the listener closes (default: 0)"
    );
    info!("  SHUTDOWN_TIMEOUT_SECONDS: Max wait for in-flight requests on shutdown (default: 30)");
    info!("  AUTH_ENABLED: Require an API key on protected endpoints (default: false)");
    info!(
        "  API_KEYS: Comma-separated API keys (all get read+query scope, no rate limit override)"
    );
    info!(
        "  API_KEYS_FILE: Path to a JSON file of {{key, name, scopes, rate_limit_per_window}} entries"
    );

    // Start cleanup service
    let mut cleanup_service = CleanupService::new(
        Arc::clone(&rate_limiter),
        config.rate_limit.cleanup_interval_seconds as u64,
    );
    cleanup_service.start();

    let (signalled_tx, signalled_rx) = tokio::sync::oneshot::channel::<()>();
    let drain = config.shutdown_drain;
    let graceful = async move {
        shutdown_signal().await;
        SHUTTING_DOWN.store(true, Ordering::Relaxed);
        info!("Shutdown signal received, draining for {:?}", drain);
        let _ = signalled_tx.send(());
        tokio::time::sleep(drain).await;
    };

    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(graceful)
    .into_future();
    tokio::pin!(server);

    // Graceful shutdown waits for every open connection; bound that wait so a
    // stuck request can't keep the process alive forever.
    let force_after = drain + config.shutdown_timeout;
    tokio::select! {
        result = &mut server => result?,
        _ = async {
            let _ = signalled_rx.await;
            tokio::time::sleep(force_after).await;
        } => warn!("In-flight requests did not finish within {:?}, shutting down anyway", force_after),
    }

    info!("Shutting down mq-web-api server");
    cleanup_service.stop();

    #[cfg(feature = "otel")]
    if let Some(provider) = TRACER_PROVIDER.get() {
        provider.shutdown().ok();
    }

    Ok(())
}
