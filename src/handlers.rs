use axum::{
    extract::rejection::QueryRejection,
    extract::{FromRequestParts, Path, Query, State},
    http::{StatusCode, header::CONTENT_TYPE, request::Parts},
    response::{IntoResponse, Json},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned, de::IntoDeserializer};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, error};
use utoipa::{
    Modify, OpenApi, ToSchema,
    openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme},
};

use crate::{
    api::{
        ApiRequest, BatchApiRequest, BatchApiResponse, BatchItemResult, CheckApiRequest,
        CheckApiResponse, CheckError, ExampleDoc, FormatApiRequest, FormatApiResponse, FunctionDoc,
        FunctionsApiResponse, InputFormat, LintApiRequest, LintApiResponse, LintDiagnostic,
        OutputFormat, QueryApiResponse, SelectorDoc, SelectorsApiResponse,
    },
    metrics::metrics,
    problem::ProblemDetails,
    query_cache::{self, QueryCache},
};

/// Set once a shutdown signal arrived; makes `GET /ready` answer 503.
pub static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);

/// State shared by all handlers.
#[derive(Clone)]
pub struct AppState {
    /// Maximum duration a single query evaluation may run before it's aborted.
    pub query_timeout: Duration,
    /// Wall-clock budget of one batch request.
    pub batch_timeout: Duration,
    /// Short-lived cache of `execute_query` results.
    pub query_cache: Arc<QueryCache>,
    /// One permit per query that may be evaluated at the same time.
    pub query_slots: Arc<Semaphore>,
}

/// Waits up to `query_timeout` for a free evaluation slot. The permit is meant
/// to be moved into the blocking task, so a slot stays taken for as long as the
/// work really runs, even after the HTTP request that started it timed out.
async fn acquire_query_slot(state: &AppState) -> Result<OwnedSemaphorePermit, ProblemDetails> {
    match tokio::time::timeout(
        state.query_timeout,
        Arc::clone(&state.query_slots).acquire_owned(),
    )
    .await
    {
        Ok(Ok(permit)) => Ok(permit),
        _ => {
            metrics().record_query_rejected();
            Err(ProblemDetails::new(StatusCode::SERVICE_UNAVAILABLE)
                .with_title("Server busy")
                .with_detail(
                    "error",
                    "Too many queries are being evaluated, retry shortly",
                )
                .with_retry_after(1))
        }
    }
}

/// Maps a failed blocking task (a panic or cancellation) to a 500 without
/// leaking its message to the caller.
fn task_failed(context: &str, e: tokio::task::JoinError) -> ProblemDetails {
    error!("{} task failed: {}", context, e);
    ProblemDetails::new(StatusCode::INTERNAL_SERVER_ERROR)
        .with_title("Internal error")
        .with_detail(
            "error",
            "The request could not be completed because of an internal error",
        )
}

/// Parses an optional `?input_format=` / `?output_format=` value. An empty
/// value counts as absent; an unknown one is a 400 listing the valid names.
fn parse_format<T: DeserializeOwned>(
    param: &str,
    value: Option<String>,
    expected: &[&str],
) -> Result<Option<T>, ProblemDetails> {
    let Some(value) = value.filter(|v| !v.is_empty()) else {
        return Ok(None);
    };

    T::deserialize(value.as_str().into_deserializer())
        .map(Some)
        .map_err(|_: serde::de::value::Error| {
            ProblemDetails::new(StatusCode::BAD_REQUEST)
                .with_title("Invalid query parameters")
                .with_detail(
                    "error",
                    &format!(
                        "Invalid {} '{}', expected one of: {}",
                        param,
                        value,
                        expected.join(", ")
                    ),
                )
        })
}

/// Serves `request` from the cache when fresh, otherwise evaluates it on the
/// blocking pool and caches a successful result.
///
/// Maps every failure to the response the caller should send: an invalid query
/// is a 400, a saturated server a 503, a panicking evaluation a 500.
async fn run_query(
    state: &AppState,
    request: ApiRequest,
) -> Result<QueryApiResponse, ProblemDetails> {
    let cache_key = query_cache::cache_key(&request);

    if let Some(key) = &cache_key {
        if let Some(cached) = state.query_cache.get(key).await {
            debug!("Query cache hit");
            metrics().record_cache_hit();
            return Ok(cached);
        }
        metrics().record_cache_miss();
    }

    let permit = acquire_query_slot(state).await?;
    let timeout = state.query_timeout;
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        crate::api::query(request, timeout)
    })
    .await
    .map_err(|e| task_failed("Query", e))?;
    metrics().record_query(started.elapsed().as_secs_f64());

    match result {
        Ok(response) => {
            if let Some(key) = cache_key {
                state.query_cache.insert(key, response.clone()).await;
            }
            Ok(response)
        }
        Err(e) => {
            debug!("Query failed: {}", e);
            Err(ProblemDetails::new(StatusCode::BAD_REQUEST)
                .with_title("Invalid query")
                .with_detail("error", &e.to_string()))
        }
    }
}

/// Query parameters of `GET /api/v1/query`.
#[derive(Deserialize)]
pub struct QueryParams {
    /// The mq query to run.
    pub query: String,
    /// Input document.
    pub input: Option<String>,
    /// Format of `input`.
    pub input_format: Option<String>,
}

/// Query parameters of the shorthand query endpoint.
#[derive(Deserialize)]
pub struct ShorthandQueryParams {
    /// Format of the request body.
    pub input_format: Option<String>,
    /// Format of the response.
    pub output_format: Option<String>,
}

/// `Query` extractor that rejects malformed parameters with a problem details response.
pub struct ValidatedQuery<T>(pub T);

impl<T, S> FromRequestParts<S> for ValidatedQuery<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ProblemDetails;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Query::<T>::from_request_parts(parts, state)
            .await
            .map(|Query(params)| ValidatedQuery(params))
            .map_err(|e: QueryRejection| {
                ProblemDetails::new(StatusCode::BAD_REQUEST)
                    .with_title("Invalid query parameters")
                    .with_detail("error", &e.body_text())
            })
    }
}

/// Response body of the health and readiness endpoints.
#[derive(Serialize, ToSchema)]
pub struct HealthResponse {
    /// `"ok"` for liveness, `"ready"` for readiness.
    pub status: &'static str,
    /// Version of the running server.
    pub version: &'static str,
}

/// Returns 200 OK while the process is alive.
pub async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
    })
}

/// Returns 200 OK while the server accepts traffic, 503 once it's shutting down.
pub async fn ready_check() -> Result<Json<HealthResponse>, ProblemDetails> {
    if SHUTTING_DOWN.load(Ordering::Relaxed) {
        return Err(ProblemDetails::new(StatusCode::SERVICE_UNAVAILABLE)
            .with_title("Shutting down")
            .with_detail(
                "error",
                "The server is draining and no longer accepts new work",
            )
            .with_retry_after(1));
    }
    Ok(Json(HealthResponse {
        status: "ready",
        version: env!("CARGO_PKG_VERSION"),
    }))
}

/// Serves the metrics in the Prometheus text exposition format.
pub async fn metrics_endpoint() -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        metrics().render(),
    )
}

#[derive(OpenApi)]
#[openapi(
    paths(
        get_query_api,
        post_query_api,
        post_shorthand_query_api,
        post_batch_api,
        post_check_api,
        post_format_api,
        get_functions_api,
        get_function_api,
        get_selectors_api,
        get_selector_api,
        post_lint_api,
        openapi_json
    ),
    components(
        schemas(ApiRequest),
        schemas(InputFormat),
        schemas(OutputFormat),
        schemas(QueryApiResponse),
        schemas(BatchApiRequest),
        schemas(BatchApiResponse),
        schemas(BatchItemResult),
        schemas(CheckApiRequest),
        schemas(CheckApiResponse),
        schemas(CheckError),
        schemas(FormatApiRequest),
        schemas(FormatApiResponse),
        schemas(ExampleDoc),
        schemas(FunctionDoc),
        schemas(FunctionsApiResponse),
        schemas(SelectorDoc),
        schemas(SelectorsApiResponse),
        schemas(LintApiRequest),
        schemas(LintApiResponse),
        schemas(LintDiagnostic),
    ),
    tags(
        (name = "mq-api", description = "Markdown Query API")
    ),
    modifiers(&SecurityAddon)
)]
/// OpenAPI document of the API.
pub struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "api_key",
                SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Bearer).build()),
            );
        }
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/query",
    responses(
        (status = 200, description = "Query executed successfully", body = QueryApiResponse),
        (status = 400, description = "Invalid request parameters"),
    ),
    params(
        ("query" = String, Query, description = "mq query string to execute"),
        ("input" = String, Query, description = "Input content to process"),
        ("input_format" = Option<String>, Query, description = "Input format: markdown, mdx, text, html, raw, null, csv, tsv, psv, json, yaml, toml, xml, or toon")
    ),
    security(("api_key" = []))
)]
/// Runs a query given as query parameters.
pub async fn get_query_api(
    ValidatedQuery(params): ValidatedQuery<QueryParams>,
    State(state): State<AppState>,
) -> Result<Json<QueryApiResponse>, ProblemDetails> {
    debug!("GET /query called with query: {}", params.query);

    let input_format =
        parse_format::<InputFormat>("input_format", params.input_format, InputFormat::NAMES)?;
    debug!("Processing request with input_format: {:?}", input_format);

    let request = ApiRequest {
        query: params.query,
        input: params.input,
        input_format,
        modules: None,
        args: None,
        output_format: None,
        aggregate: None,
    };

    let response = run_query(&state, request).await?;
    debug!("Query succeeded with {} results", response.results.len());
    Ok(Json(response))
}

#[utoipa::path(
    post,
    path = "/api/v1/query",
    responses(
        (status = 200, description = "Query executed successfully", body = QueryApiResponse),
        (status = 400, description = "Invalid request parameters"),
    ),
    request_body = ApiRequest,
    security(("api_key" = []))
)]
/// Runs a query given in the request body.
pub async fn post_query_api(
    State(state): State<AppState>,
    Json(request): Json<ApiRequest>,
) -> Result<Json<QueryApiResponse>, ProblemDetails> {
    debug!("POST /query called with query: {}", request.query);
    debug!(
        "Processing request with input_format: {:?}",
        request.input_format
    );

    let response = run_query(&state, request).await?;
    debug!("Query succeeded with {} results", response.results.len());
    Ok(Json(response))
}

#[utoipa::path(
    post,
    path = "/api/v1/batch",
    responses(
        (status = 200, description = "Batch processed (see per-item `error` fields for per-document failures)", body = BatchApiResponse),
        (status = 400, description = "Invalid request parameters, or `inputs` exceeds the batch size limit"),
    ),
    request_body = BatchApiRequest,
    security(("api_key" = []))
)]
/// Runs one query against many input documents.
pub async fn post_batch_api(
    State(state): State<AppState>,
    Json(request): Json<BatchApiRequest>,
) -> Result<Json<BatchApiResponse>, ProblemDetails> {
    debug!(
        "POST /batch called with query: {}, {} documents",
        request.query,
        request.inputs.len()
    );

    let permit = acquire_query_slot(&state).await?;
    let timeout = state.query_timeout;
    let batch_timeout = state.batch_timeout;
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        crate::api::batch_query(request, timeout, batch_timeout)
    })
    .await
    .map_err(|e| task_failed("Batch", e))?;
    metrics().record_query(started.elapsed().as_secs_f64());

    match result {
        Ok(response) => {
            debug!("Batch succeeded with {} documents", response.items.len());
            Ok(Json(response))
        }
        Err(e) => {
            debug!("Batch rejected: {}", e);
            Err(ProblemDetails::new(StatusCode::BAD_REQUEST)
                .with_title("Invalid batch request")
                .with_detail("error", &e.to_string()))
        }
    }
}

/// Guesses `body`'s format from its leading bytes, ignoring leading
/// whitespace and a UTF-8 BOM. Used by the shorthand endpoint to auto-select
/// `input_format` when the caller doesn't pass `?input_format=`.
///
/// Only formats with an unambiguous leading marker are detected here
/// (HTML, XML, JSON). Formats like CSV/TSV/PSV/YAML/TOML/TOON have no
/// reliable content signature and are indistinguishable from plain
/// Markdown/text without an explicit `?input_format=`.
fn sniff_input_format(body: &str) -> Option<InputFormat> {
    let trimmed = body.trim_start_matches('\u{feff}').trim_start();
    let lower_prefix: String = trimmed
        .chars()
        .take(14)
        .collect::<String>()
        .to_ascii_lowercase();

    if lower_prefix.starts_with("<!doctype html") || lower_prefix.starts_with("<html") {
        return Some(InputFormat::Html);
    }
    if lower_prefix.starts_with("<?xml") {
        return Some(InputFormat::Xml);
    }
    match trimmed.chars().next() {
        Some('{') | Some('[') => Some(InputFormat::Json),
        _ => None,
    }
}

#[utoipa::path(
    post,
    path = "/{query}",
    tag = "mq-api",
    summary = "Run an mq query against raw Markdown/HTML/XML/JSON/CSV/... (curl-friendly shortcut)",
    description = "Curl-friendly shortcut that reads the mq query from the URL path and the input content from the raw request body, e.g. `curl --data-binary @doc.md https://api.mqlang.org/.h1`. Reserved characters in the query (`|`, `?`, `#`) must be percent-encoded.",
    params(
        ("query" = String, Path, description = "mq query expression", example = ".h1"),
        ("input_format" = Option<String>, Query, description = "Input format: markdown, mdx, text, html, raw, null, csv, tsv, psv, json, yaml, toml, xml, or toon. If omitted, html/xml/json are auto-detected from the body's leading bytes; csv/tsv/psv/yaml/toml/toon have no reliable signature and require this parameter."),
        ("output_format" = Option<String>, Query, description = "Output format: markdown, html, text, json, or none"),
    ),
    request_body(content = String, content_type = "text/markdown", description = "Raw content to query: Markdown/MDX/HTML/text natively, or CSV/TSV/PSV/JSON/YAML/TOML/XML/TOON via the matching `input_format`"),
    responses(
        (status = 200, description = "Query executed successfully", body = QueryApiResponse),
        (status = 400, description = "Invalid query or request"),
    ),
    security(("api_key" = []))
)]
/// Runs the query given in the path against the request body.
pub async fn post_shorthand_query_api(
    State(state): State<AppState>,
    Path(query): Path<String>,
    ValidatedQuery(params): ValidatedQuery<ShorthandQueryParams>,
    body: String,
) -> Result<Json<QueryApiResponse>, ProblemDetails> {
    debug!("POST /{{query}} called with query: {}", query);

    let input_format =
        parse_format::<InputFormat>("input_format", params.input_format, InputFormat::NAMES)?
            .or_else(|| sniff_input_format(&body));
    let output_format =
        parse_format::<OutputFormat>("output_format", params.output_format, OutputFormat::NAMES)?;

    let request = ApiRequest {
        query,
        input: Some(body),
        input_format,
        modules: None,
        args: None,
        output_format,
        aggregate: None,
    };

    let response = run_query(&state, request).await?;
    debug!(
        "Shorthand query succeeded with {} results",
        response.results.len()
    );
    Ok(Json(response))
}

#[utoipa::path(
    post,
    path = "/api/v1/check",
    responses(
        (status = 200, description = "Type check completed", body = CheckApiResponse),
        (status = 500, description = "The type checker failed unexpectedly"),
    ),
    request_body = CheckApiRequest
)]
/// Type-checks a query.
pub async fn post_check_api(
    State(_state): State<AppState>,
    Json(request): Json<CheckApiRequest>,
) -> Result<Json<CheckApiResponse>, ProblemDetails> {
    debug!("POST /check called with query: {}", request.query);

    let response = tokio::task::spawn_blocking(move || crate::api::check(request))
        .await
        .map_err(|e| task_failed("Check", e))?;
    debug!("Type check found {} errors", response.errors.len());

    Ok(Json(response))
}

#[utoipa::path(
    post,
    path = "/api/v1/format",
    responses(
        (status = 200, description = "Format completed", body = FormatApiResponse),
        (status = 400, description = "Invalid query syntax"),
    ),
    request_body = FormatApiRequest
)]
/// Formats a query.
pub async fn post_format_api(
    State(_state): State<AppState>,
    Json(request): Json<FormatApiRequest>,
) -> Result<Json<FormatApiResponse>, ProblemDetails> {
    debug!("POST /format called");

    match tokio::task::spawn_blocking(move || crate::api::format_query(request))
        .await
        .map_err(|e| task_failed("Format", e))?
    {
        Ok(response) => Ok(Json(response)),
        Err(e) => {
            debug!("Format failed: {}", e);
            Err(ProblemDetails::new(StatusCode::BAD_REQUEST)
                .with_title("Format error")
                .with_detail("error", &e.to_string()))
        }
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/functions",
    responses(
        (status = 200, description = "List of builtin mq functions", body = FunctionsApiResponse),
    )
)]
/// Lists builtin functions.
pub async fn get_functions_api(State(_state): State<AppState>) -> Json<FunctionsApiResponse> {
    debug!("GET /functions called");
    Json(crate::api::list_functions())
}

#[utoipa::path(
    get,
    path = "/api/v1/selectors",
    responses(
        (status = 200, description = "List of builtin mq selectors", body = SelectorsApiResponse),
    )
)]
/// Lists builtin selectors.
pub async fn get_selectors_api(State(_state): State<AppState>) -> Json<SelectorsApiResponse> {
    debug!("GET /selectors called");
    Json(crate::api::list_selectors())
}

#[utoipa::path(
    get,
    path = "/api/v1/functions/{name}",
    params(("name" = String, Path, description = "Function name, e.g. `map`")),
    responses(
        (status = 200, description = "Documentation for a single builtin function", body = FunctionDoc),
        (status = 404, description = "No function with that name"),
    )
)]
/// Returns the documentation of one builtin function.
pub async fn get_function_api(
    State(_state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<FunctionDoc>, ProblemDetails> {
    debug!("GET /functions/{} called", name);
    crate::api::get_function(&name).map(Json).ok_or_else(|| {
        ProblemDetails::new(StatusCode::NOT_FOUND)
            .with_title("Function not found")
            .with_detail("name", &name)
    })
}

#[utoipa::path(
    get,
    path = "/api/v1/selectors/{name}",
    params(("name" = String, Path, description = "Selector name, with or without a leading `.`, e.g. `h1` or `.h1`")),
    responses(
        (status = 200, description = "Documentation for a single builtin selector", body = SelectorDoc),
        (status = 404, description = "No selector with that name"),
    )
)]
/// Returns the documentation of one builtin selector.
pub async fn get_selector_api(
    State(_state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<SelectorDoc>, ProblemDetails> {
    debug!("GET /selectors/{} called", name);
    crate::api::get_selector(&name).map(Json).ok_or_else(|| {
        ProblemDetails::new(StatusCode::NOT_FOUND)
            .with_title("Selector not found")
            .with_detail("name", &name)
    })
}

#[utoipa::path(
    post,
    path = "/api/v1/lint",
    responses(
        (status = 200, description = "Lint completed", body = LintApiResponse),
        (status = 500, description = "The linter failed unexpectedly"),
    ),
    request_body = LintApiRequest
)]
/// Lints a query.
pub async fn post_lint_api(
    State(_state): State<AppState>,
    Json(request): Json<LintApiRequest>,
) -> Result<Json<LintApiResponse>, ProblemDetails> {
    debug!("POST /lint called with query: {}", request.query);

    let response = tokio::task::spawn_blocking(move || crate::api::lint(request))
        .await
        .map_err(|e| task_failed("Lint", e))?;
    debug!("Lint found {} diagnostics", response.diagnostics.len());

    Ok(Json(response))
}

static OPENAPI_SPEC: OnceLock<utoipa::openapi::OpenApi> = OnceLock::new();

#[utoipa::path(
    get,
    path = "/api/v1/openapi.json",
    responses(
        (status = 200, description = "OpenAPI specification", body = String),
    )
)]
/// Serves the OpenAPI specification.
pub async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    debug!("GET /openapi.json called");
    Json(OPENAPI_SPEC.get_or_init(ApiDoc::openapi).clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("<html><body>Hi</body></html>", Some(InputFormat::Html))]
    #[case("<HTML><body>Hi</body></html>", Some(InputFormat::Html))]
    #[case("<!DOCTYPE html><html></html>", Some(InputFormat::Html))]
    #[case("  \n<!doctype html>\n<html></html>", Some(InputFormat::Html))]
    #[case("\u{feff}<html></html>", Some(InputFormat::Html))]
    #[case(r#"<?xml version="1.0"?><root/>"#, Some(InputFormat::Xml))]
    #[case(r#"{"a": 1}"#, Some(InputFormat::Json))]
    #[case("[1, 2, 3]", Some(InputFormat::Json))]
    #[case("  \n[1, 2, 3]", Some(InputFormat::Json))]
    #[case("# Title\n\nBody text.", None)]
    #[case("<div>fragment</div>", None)]
    #[case("a,b,c\n1,2,3", None)]
    #[case("", None)]
    fn test_sniff_input_format(#[case] body: &str, #[case] expected: Option<InputFormat>) {
        assert_eq!(sniff_input_format(body), expected, "body: {:?}", body);
    }
}
