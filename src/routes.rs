use axum::{
    Router,
    extract::Request,
    http::{HeaderName, Method, StatusCode},
    middleware,
    response::{Redirect, Response},
    routing::{get, post},
};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tower::ServiceBuilder;
use tower_http::{
    catch_panic::CatchPanicLayer,
    compression::CompressionLayer,
    cors::{Any, CorsLayer},
    limit::RequestBodyLimitLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    timeout::TimeoutLayer,
    trace::TraceLayer,
};
use tracing::info;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    auth::ApiKeyStore,
    config::Config,
    handlers::{
        ApiDoc, AppState, get_function_api, get_functions_api, get_query_api, get_selector_api,
        get_selectors_api, health_check, metrics_endpoint, post_batch_api, post_check_api,
        post_format_api, post_lint_api, post_query_api, post_shorthand_query_api, ready_check,
    },
    middleware::{
        AuthState, RateLimitState, auth_middleware, metrics_middleware, panic_response,
        problem_json_middleware, rate_limit_middleware,
    },
    query_cache::QueryCache,
    rate_limiter::RateLimiter,
};

const REQUEST_ID_HEADER: &str = "x-request-id";

/// Builds the router with all routes and middleware for `config`.
///
/// Outermost to innermost: request id and tracing, metrics, panic catching,
/// per-IP rate limiting, API key auth, then body limit / CORS / timeout /
/// error shaping / compression around the handlers.
pub fn create_router(
    config: &Config,
    rate_limiter: Arc<RateLimiter>,
    api_key_store: Option<Arc<ApiKeyStore>>,
) -> Router {
    let state = AppState {
        query_timeout: config.query_timeout,
        batch_timeout: config.batch_timeout,
        query_cache: Arc::new(QueryCache::new(config.query_cache.clone())),
        query_slots: Arc::new(Semaphore::new(config.max_concurrent_queries.max(1))),
    };

    let cors = if config.cors_origins.contains(&"*".to_string()) {
        CorsLayer::new()
            .allow_methods([Method::GET, Method::POST])
            .allow_headers(Any)
            .allow_origin(Any)
    } else {
        let origins: Result<Vec<_>, _> = config
            .cors_origins
            .iter()
            .map(|origin| origin.parse())
            .collect();

        match origins {
            Ok(origins) => CorsLayer::new()
                .allow_methods([Method::GET, Method::POST])
                .allow_headers(Any)
                .allow_origin(origins),
            Err(_) => {
                eprintln!("Warning: Invalid CORS origins, falling back to allow all");
                CorsLayer::new()
                    .allow_methods([Method::GET, Method::POST])
                    .allow_headers(Any)
                    .allow_origin(Any)
            }
        }
    };

    let v1_routes = Router::new()
        .route("/query", get(get_query_api).post(post_query_api))
        .route("/batch", post(post_batch_api))
        .route("/check", post(post_check_api))
        .route("/format", post(post_format_api))
        .route("/functions", get(get_functions_api))
        .route("/functions/{name}", get(get_function_api))
        .route("/selectors", get(get_selectors_api))
        .route("/selectors/{name}", get(get_selector_api))
        .route("/lint", post(post_lint_api));

    let mut router = Router::new()
        .merge(SwaggerUi::new("/docs").url("/api/v1/openapi.json", ApiDoc::openapi()))
        .route("/health", get(health_check))
        .route("/ready", get(ready_check))
        .nest("/api/v1", v1_routes)
        // Legacy paths — kept for backward compatibility, redirects to v1
        .route(
            "/api/query",
            get(|| async { Redirect::permanent("/api/v1/query") })
                .post(|| async { Redirect::permanent("/api/v1/query") }),
        )
        .route(
            "/api/check",
            post(|| async { Redirect::permanent("/api/v1/check") }),
        )
        .route(
            "/api/format",
            post(|| async { Redirect::permanent("/api/v1/format") }),
        )
        .route(
            "/openapi.json",
            get(|| async { Redirect::permanent("/api/v1/openapi.json") }),
        )
        // Curl-friendly shortcut: query in the path, raw Markdown body.
        // Static routes above (e.g. /health, /docs, /api/v1/*) take precedence.
        .route("/{query}", post(post_shorthand_query_api));

    if config.metrics_enabled {
        router = router.route("/metrics", get(metrics_endpoint));
    }

    let router = router.layer(
        ServiceBuilder::new()
            .layer(CompressionLayer::new())
            .layer(middleware::from_fn(problem_json_middleware))
            .layer(RequestBodyLimitLayer::new(config.max_request_body_size))
            .layer(cors)
            .layer(TimeoutLayer::with_status_code(
                StatusCode::REQUEST_TIMEOUT,
                config.request_timeout,
            )),
    );

    let router = if let Some(store) = api_key_store {
        router.layer(middleware::from_fn_with_state(
            AuthState {
                store,
                rate_limiter: rate_limiter.clone(),
            },
            auth_middleware,
        ))
    } else {
        router
    };

    let request_id_header = HeaderName::from_static(REQUEST_ID_HEADER);

    router
        .layer(middleware::from_fn_with_state(
            RateLimitState {
                rate_limiter,
                trusted_proxy_count: config.trusted_proxy_count,
            },
            rate_limit_middleware,
        ))
        .layer(CatchPanicLayer::custom(panic_response))
        .layer(middleware::from_fn(metrics_middleware))
        .layer(
            ServiceBuilder::new()
                .layer(SetRequestIdLayer::new(
                    request_id_header.clone(),
                    MakeRequestUuid,
                ))
                .layer(
                    TraceLayer::new_for_http()
                        // The default span records the full URI; that would put the query string,
                        // and with it GET /query inputs, into every log line.
                        .make_span_with(|request: &Request| {
                            let request_id = request
                                .headers()
                                .get(REQUEST_ID_HEADER)
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or("-");
                            tracing::info_span!(
                                "request",
                                method = %request.method(),
                                path = %request.uri().path(),
                                request_id = %request_id,
                            )
                        })
                        .on_response(
                            |response: &Response, latency: Duration, _span: &tracing::Span| {
                                info!(
                                    "response latency: {:.2}ms, status: {}",
                                    latency.as_secs_f64() * 1000.0,
                                    response.status()
                                );
                            },
                        ),
                )
                .layer(PropagateRequestIdLayer::new(request_id_header)),
        )
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AuthConfig;
    use crate::rate_limiter::{RateLimitConfig, RateLimiter};
    use axum::body::Body;
    use axum::http::{Request, header};
    use tower::ServiceExt;

    fn router_with(config: &Config) -> Router {
        let rate_limiter = Arc::new(RateLimiter::new(config.rate_limit.clone()));
        let api_key_store = crate::ApiKeyStore::from_config(&config.auth).map(Arc::new);
        create_router(config, rate_limiter, api_key_store)
    }

    async fn send(
        router: &Router,
        request: Request<Body>,
    ) -> (StatusCode, axum::http::HeaderMap, String) {
        let response = router.clone().oneshot(request).await.unwrap();
        let (parts, body) = response.into_parts();
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        (
            parts.status,
            parts.headers,
            String::from_utf8_lossy(&bytes).into_owned(),
        )
    }

    fn json_post(uri: &str, body: &str) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    fn assert_problem(
        headers: &axum::http::HeaderMap,
        body: &str,
        status: StatusCode,
    ) -> serde_json::Value {
        assert_eq!(
            headers[header::CONTENT_TYPE],
            "application/problem+json",
            "{body}"
        );
        let json: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(json["status"], status.as_u16(), "{body}");
        assert!(json["title"].is_string(), "{body}");
        json
    }

    #[tokio::test]
    async fn test_oversized_request_body_is_rejected() {
        let config = Config {
            max_request_body_size: 16,
            ..Config::default()
        };
        let rate_limiter = Arc::new(RateLimiter::new(config.rate_limit.clone()));
        let router = create_router(&config, rate_limiter, None);

        let body = "x".repeat(64);
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/format")
            .header("content-length", body.len())
            .body(Body::from(body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "application/problem+json"
        );
    }

    #[tokio::test]
    async fn test_unknown_route_and_wrong_method_are_problem_json() {
        let router = router_with(&Config::default());

        let (status, headers, body) = send(
            &router,
            Request::builder()
                .uri("/no/such/route")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_problem(&headers, &body, status);

        let (status, headers, body) = send(
            &router,
            Request::builder().uri("/.h1").body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
        assert!(
            headers.contains_key(header::ALLOW),
            "Allow header must survive"
        );
        assert_problem(&headers, &body, status);
    }

    #[tokio::test]
    async fn test_malformed_json_body_is_problem_json() {
        let router = router_with(&Config::default());

        let (status, headers, body) = send(&router, json_post("/api/v1/query", "{not json")).await;
        assert!(status.is_client_error(), "{status}");
        let json = assert_problem(&headers, &body, status);
        assert!(
            json["error"].as_str().is_some_and(|e| !e.is_empty()),
            "{body}"
        );
    }

    #[tokio::test]
    async fn test_query_roundtrip_sets_request_id() {
        let router = router_with(&Config::default());

        let (status, headers, body) = send(
            &router,
            json_post("/api/v1/query", r##"{"query": ".h1", "input": "# Hi"}"##),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(headers.contains_key("x-request-id"));
        assert!(body.contains("# Hi"), "{body}");

        let mut request = json_post("/api/v1/query", r##"{"query": ".h1", "input": "# Hi"}"##);
        request
            .headers_mut()
            .insert("x-request-id", "client-supplied-id".parse().unwrap());
        let (_, headers, _) = send(&router, request).await;
        assert_eq!(headers["x-request-id"], "client-supplied-id");
    }

    #[tokio::test]
    async fn test_unknown_formats_are_rejected_not_ignored() {
        let router = router_with(&Config::default());

        let (status, headers, body) = send(
            &router,
            Request::builder()
                .uri("/api/v1/query?query=.h1&input=%23%20Hi&input_format=csvv")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let json = assert_problem(&headers, &body, status);
        assert!(json["error"].as_str().unwrap().contains("csvv"), "{body}");
        assert!(
            json["error"].as_str().unwrap().contains("markdown"),
            "{body}"
        );

        let (status, headers, body) = send(
            &router,
            Request::builder()
                .method("POST")
                .uri("/.h1?output_format=yaml")
                .body(Body::from("# Hi"))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_problem(&headers, &body, status);

        // Empty values mean "not given".
        let (status, _, body) = send(
            &router,
            Request::builder()
                .method("POST")
                .uri("/.h1?input_format=&output_format=")
                .body(Body::from("# Hi"))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }

    #[tokio::test]
    async fn test_invalid_query_is_a_400_problem() {
        let router = router_with(&Config::default());

        let (status, headers, body) = send(
            &router,
            json_post(
                "/api/v1/query",
                r#"{"query": "invalid query", "input": "x"}"#,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_problem(&headers, &body, status);
    }

    #[tokio::test]
    async fn test_oversized_batch_is_a_400_problem() {
        let router = router_with(&Config::default());
        let inputs = vec!["# t"; crate::api::MAX_BATCH_SIZE + 1];
        let body = serde_json::json!({ "query": ".h1", "inputs": inputs }).to_string();

        let (status, headers, body) = send(&router, json_post("/api/v1/batch", &body)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_problem(&headers, &body, status);
    }

    #[tokio::test]
    async fn test_saturated_server_answers_503_with_retry_after() {
        let config = Config {
            max_concurrent_queries: 1,
            query_timeout: Duration::from_millis(50),
            ..Config::default()
        };

        // Build the state by hand so the test can hold the only slot itself.
        let state = AppState {
            query_timeout: config.query_timeout,
            batch_timeout: config.batch_timeout,
            query_cache: Arc::new(QueryCache::new(config.query_cache.clone())),
            query_slots: Arc::new(Semaphore::new(config.max_concurrent_queries)),
        };
        let _held = state.query_slots.clone().acquire_owned().await.unwrap();
        let app = Router::new()
            .route("/api/v1/query", post(post_query_api))
            .with_state(state);

        let (status, headers, body) =
            send(&app, json_post("/api/v1/query", r#"{"query": ".h1"}"#)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        assert_eq!(headers[header::RETRY_AFTER], "1");
        assert_problem(&headers, &body, status);
    }

    #[tokio::test]
    async fn test_rate_limit_exceeded_is_problem_json_with_retry_after() {
        let config = Config {
            rate_limit: RateLimitConfig {
                requests_per_window: 1,
                ..RateLimitConfig::default()
            },
            ..Config::default()
        };
        let router = router_with(&config);
        let request = || {
            Request::builder()
                .uri("/api/v1/functions")
                .body(Body::empty())
                .unwrap()
        };

        let (status, _, _) = send(&router, request()).await;
        assert_eq!(status, StatusCode::OK);

        let (status, headers, body) = send(&router, request()).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        let json = assert_problem(&headers, &body, status);
        assert_eq!(json["limit"], 1);
        assert!(headers.contains_key(header::RETRY_AFTER));
        assert!(headers.contains_key("x-ratelimit-limit"));
    }

    #[tokio::test]
    async fn test_probes_are_not_rate_limited() {
        let config = Config {
            rate_limit: RateLimitConfig {
                requests_per_window: 1,
                ..RateLimitConfig::default()
            },
            metrics_enabled: true,
            ..Config::default()
        };
        let router = router_with(&config);

        for path in ["/health", "/ready", "/metrics"] {
            for _ in 0..3 {
                let (status, _, body) = send(
                    &router,
                    Request::builder().uri(path).body(Body::empty()).unwrap(),
                )
                .await;
                assert_eq!(status, StatusCode::OK, "{path}: {body}");
            }
        }
    }

    #[tokio::test]
    async fn test_health_reports_version() {
        let router = router_with(&Config::default());
        let (status, _, body) = send(
            &router,
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["status"], "ok");
        assert_eq!(json["version"], env!("CARGO_PKG_VERSION"));
    }

    // The only test that touches SHUTTING_DOWN, so it can't race with others.
    #[tokio::test]
    async fn test_ready_turns_503_while_shutting_down() {
        use std::sync::atomic::Ordering;

        let router = router_with(&Config::default());
        let ready = || {
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .unwrap()
        };

        let (status, _, _) = send(&router, ready()).await;
        assert_eq!(status, StatusCode::OK);

        crate::handlers::SHUTTING_DOWN.store(true, Ordering::Relaxed);
        let (status, headers, body) = send(&router, ready()).await;
        crate::handlers::SHUTTING_DOWN.store(false, Ordering::Relaxed);

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_problem(&headers, &body, status);
    }

    #[tokio::test]
    async fn test_metrics_endpoint_is_opt_in() {
        let disabled = router_with(&Config::default());
        let (status, _, _) = send(
            &disabled,
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_ne!(status, StatusCode::OK);

        let enabled = router_with(&Config {
            metrics_enabled: true,
            ..Config::default()
        });
        send(
            &enabled,
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        let (status, headers, body) = send(
            &enabled,
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            headers[header::CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("text/plain")
        );
        assert!(
            body.contains("mq_http_requests_total{status=\"200\"}"),
            "{body}"
        );
        assert!(
            body.contains("mq_http_request_duration_seconds_count"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn test_metrics_requires_a_key_when_auth_is_enabled() {
        let router = router_with(&Config {
            metrics_enabled: true,
            auth: AuthConfig {
                enabled: true,
                keys: vec!["secret".to_string()],
                keys_file: None,
            },
            ..Config::default()
        });

        let (status, headers, body) = send(
            &router,
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_problem(&headers, &body, status);
        assert_eq!(headers[header::WWW_AUTHENTICATE], "Bearer");

        let (status, _, _) = send(
            &router,
            Request::builder()
                .uri("/metrics")
                .header(header::AUTHORIZATION, "Bearer secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn test_panicking_handler_becomes_500_and_server_keeps_serving() {
        async fn boom() -> &'static str {
            panic!("handler exploded");
        }

        let app = Router::new()
            .route("/boom", get(boom))
            .route("/ok", get(|| async { "fine" }))
            .layer(CatchPanicLayer::custom(panic_response));

        let (status, headers, body) = send(
            &app,
            Request::builder().uri("/boom").body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        let json = assert_problem(&headers, &body, status);
        assert!(
            !body.contains("exploded"),
            "panic message must not leak: {json}"
        );

        let (status, _, _) = send(
            &app,
            Request::builder().uri("/ok").body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn test_request_within_body_limit_is_not_rejected() {
        let config = Config::default();
        let rate_limiter = Arc::new(RateLimiter::new(config.rate_limit.clone()));
        let router = create_router(&config, rate_limiter, None);

        let request = Request::builder()
            .method("GET")
            .uri("/health")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
