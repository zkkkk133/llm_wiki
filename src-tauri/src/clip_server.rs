use std::sync::{Condvar, Mutex};
use std::sync::atomic::{AtomicU8, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use tiny_http::{Header, Method, Response, Server};

static CURRENT_PROJECT: Mutex<String> = Mutex::new(String::new());
static ALL_PROJECTS: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new()); // (name, path)
static PENDING_CLIPS: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new()); // (projectPath, filePath)
static PENDING_ASKS: Mutex<Vec<PendingAsk>> = Mutex::new(Vec::new());
static ASK_RESULTS: Mutex<Vec<AskResult>> = Mutex::new(Vec::new());
static ASK_RESULTS_CVAR: Condvar = Condvar::new();
static ASK_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Daemon status: 0=starting, 1=running, 2=port_conflict, 3=error
static DAEMON_STATUS: AtomicU8 = AtomicU8::new(0);

const BIND_ADDR: &str = "0.0.0.0";
const PORT: u16 = 19827;
const MAX_BIND_RETRIES: u32 = 3;
const MAX_RESTART_RETRIES: u32 = 10;
const BIND_RETRY_DELAY_SECS: u64 = 2;
const RESTART_DELAY_SECS: u64 = 5;
const DEFAULT_ASK_TIMEOUT_MS: u64 = 300_000;
const MAX_ASK_TIMEOUT_MS: u64 = 1_800_000;

#[derive(Clone)]
struct PendingAsk {
    id: String,
    project_path: String,
    question: String,
}

struct AskResult {
    id: String,
    status: u16,
    body: String,
}

/// Get current daemon status as a string
pub fn get_daemon_status() -> &'static str {
    match DAEMON_STATUS.load(Ordering::Relaxed) {
        0 => "starting",
        1 => "running",
        2 => "port_conflict",
        _ => "error",
    }
}

pub fn start_clip_server() {
    thread::spawn(|| {
        let mut restart_count: u32 = 0;

        loop {
            // Try to bind the port with retries
            let server = {
                let mut last_err = String::new();
                let mut bound = None;
                for attempt in 1..=MAX_BIND_RETRIES {
                    match Server::http(format!("{}:{}", BIND_ADDR, PORT)) {
                        Ok(s) => {
                            bound = Some(s);
                            break;
                        }
                        Err(e) => {
                            last_err = format!("{}", e);
                            eprintln!(
                                "[Clip Server] Bind attempt {}/{} failed: {}",
                                attempt, MAX_BIND_RETRIES, e
                            );
                            if attempt < MAX_BIND_RETRIES {
                                thread::sleep(std::time::Duration::from_secs(BIND_RETRY_DELAY_SECS));
                            }
                        }
                    }
                }
                match bound {
                    Some(s) => s,
                    None => {
                        eprintln!(
                            "[Clip Server] Port {} unavailable after {} attempts: {}",
                            PORT, MAX_BIND_RETRIES, last_err
                        );
                        DAEMON_STATUS.store(2, Ordering::Relaxed); // port_conflict
                        return; // Don't retry on port conflict — needs user action
                    }
                }
            };

            DAEMON_STATUS.store(1, Ordering::Relaxed); // running
            restart_count = 0; // Reset on successful bind
            println!("[Clip Server] Listening on http://{}:{}", BIND_ADDR, PORT);

        for mut request in server.incoming_requests() {
            let cors_headers = vec![
                Header::from_bytes("Access-Control-Allow-Origin", "*").unwrap(),
                Header::from_bytes("Access-Control-Allow-Methods", "GET, POST, OPTIONS").unwrap(),
                Header::from_bytes("Access-Control-Allow-Headers", "Content-Type").unwrap(),
                Header::from_bytes("Content-Type", "application/json").unwrap(),
            ];

            // Handle CORS preflight
            if request.method() == &Method::Options {
                let mut response = Response::from_string("").with_status_code(204);
                for h in &cors_headers {
                    response.add_header(h.clone());
                }
                let _ = request.respond(response);
                continue;
            }

            let url = request.url().to_string();

            match (request.method(), url.as_str()) {
                (&Method::Get, "/status") => {
                    let body = r#"{"ok":true,"version":"0.1.0"}"#;
                    let mut response = Response::from_string(body);
                    for h in &cors_headers {
                        response.add_header(h.clone());
                    }
                    let _ = request.respond(response);
                }
                (&Method::Get, "/project") => {
                    let path = CURRENT_PROJECT.lock().unwrap().clone();
                    // serde_json handles backslash escaping so a Windows
                    // path that somehow still contains `\` won't break
                    // the JSON parser on the client.
                    let body = serde_json::json!({
                        "ok": true,
                        "path": path,
                    }).to_string();
                    let mut response = Response::from_string(body);
                    for h in &cors_headers {
                        response.add_header(h.clone());
                    }
                    let _ = request.respond(response);
                }
                (&Method::Post, "/project") => {
                    let mut body = String::new();
                    if let Err(e) = request.as_reader().read_to_string(&mut body) {
                        let err =
                            format!(r#"{{"ok":false,"error":"Failed to read body: {}"}}"#, e);
                        let mut response = Response::from_string(err).with_status_code(400);
                        for h in &cors_headers {
                            response.add_header(h.clone());
                        }
                        let _ = request.respond(response);
                        continue;
                    }

                    let result = handle_set_project(&body);
                    let status = if result.contains(r#""ok":true"#) {
                        200
                    } else {
                        400
                    };
                    let mut response = Response::from_string(result).with_status_code(status);
                    for h in &cors_headers {
                        response.add_header(h.clone());
                    }
                    let _ = request.respond(response);
                }
                (&Method::Get, "/projects") => {
                    let projects = ALL_PROJECTS.lock().unwrap().clone();
                    let current = CURRENT_PROJECT.lock().unwrap().clone();
                    // serde_json for proper escaping of `\`, `"`, and any
                    // other characters that might appear in a project name
                    // or path. Previously only `"` was escaped by hand,
                    // which broke on Windows paths containing backslashes.
                    let items: Vec<serde_json::Value> = projects.iter()
                        .map(|(name, path)| serde_json::json!({
                            "name": name,
                            "path": path,
                            "current": path == &current,
                        }))
                        .collect();
                    let body = serde_json::json!({
                        "ok": true,
                        "projects": items,
                    }).to_string();
                    let mut response = Response::from_string(body);
                    for h in &cors_headers { response.add_header(h.clone()); }
                    let _ = request.respond(response);
                }
                (&Method::Post, "/projects") => {
                    let mut body = String::new();
                    if request.as_reader().read_to_string(&mut body).is_ok() {
                        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&body) {
                            if let Some(arr) = parsed["projects"].as_array() {
                                let mut projects = ALL_PROJECTS.lock().unwrap();
                                projects.clear();
                                for item in arr {
                                    let name = item["name"].as_str().unwrap_or("").to_string();
                                    let path = item["path"].as_str().unwrap_or("").to_string();
                                    if !path.is_empty() {
                                        projects.push((name, path));
                                    }
                                }
                            }
                        }
                    }
                    let mut response = Response::from_string(r#"{"ok":true}"#);
                    for h in &cors_headers { response.add_header(h.clone()); }
                    let _ = request.respond(response);
                }
                (&Method::Get, "/clips/pending") => {
                    let mut pending = PENDING_CLIPS.lock().unwrap();
                    // Use serde_json for proper escaping of both quotes
                    // and backslashes — hand-rolled escaping previously
                    // produced invalid JSON on Windows paths containing
                    // \r, \s, etc.
                    let clips_json: Vec<serde_json::Value> = pending.iter()
                        .map(|(proj, file)| serde_json::json!({
                            "projectPath": proj,
                            "filePath": file,
                        }))
                        .collect();
                    let body = serde_json::json!({
                        "ok": true,
                        "clips": clips_json,
                    }).to_string();
                    pending.clear();
                    let mut response = Response::from_string(body);
                    for h in &cors_headers { response.add_header(h.clone()); }
                    let _ = request.respond(response);
                }
                (&Method::Get, "/asks/pending") => {
                    let mut pending = PENDING_ASKS.lock().unwrap();
                    let asks_json: Vec<serde_json::Value> = pending.iter()
                        .map(|ask| serde_json::json!({
                            "id": ask.id,
                            "projectPath": ask.project_path,
                            "question": ask.question,
                        }))
                        .collect();
                    let body = serde_json::json!({
                        "ok": true,
                        "asks": asks_json,
                    }).to_string();
                    pending.clear();
                    let mut response = Response::from_string(body);
                    for h in &cors_headers { response.add_header(h.clone()); }
                    let _ = request.respond(response);
                }
                (&Method::Post, "/asks/answer") => {
                    let mut body = String::new();
                    if let Err(e) = request.as_reader().read_to_string(&mut body) {
                        let err =
                            format!(r#"{{"ok":false,"error":"Failed to read body: {}"}}"#, e);
                        let mut response = Response::from_string(err).with_status_code(400);
                        for h in &cors_headers {
                            response.add_header(h.clone());
                        }
                        let _ = request.respond(response);
                        continue;
                    }

                    let result = handle_ask_answer(&body);
                    let status = if result.contains(r#""ok":true"#) {
                        200
                    } else {
                        400
                    };
                    let mut response = Response::from_string(result).with_status_code(status);
                    for h in &cors_headers {
                        response.add_header(h.clone());
                    }
                    let _ = request.respond(response);
                }
                (&Method::Post, "/ask") => {
                    let mut body = String::new();
                    if let Err(e) = request.as_reader().read_to_string(&mut body) {
                        let err =
                            format!(r#"{{"ok":false,"error":"Failed to read body: {}"}}"#, e);
                        let mut response = Response::from_string(err).with_status_code(400);
                        for h in &cors_headers {
                            response.add_header(h.clone());
                        }
                        let _ = request.respond(response);
                        continue;
                    }

                    // Keep the tiny_http accept loop free while a question is
                    // being answered. The frontend has to poll /asks/pending
                    // and POST /asks/answer on this same server, so blocking
                    // in the accept loop would deadlock the request.
                    let response_headers = cors_headers.clone();
                    thread::spawn(move || {
                        let (result, status) = handle_ask(&body);
                        let mut response = Response::from_string(result).with_status_code(status);
                        for h in &response_headers {
                            response.add_header(h.clone());
                        }
                        let _ = request.respond(response);
                    });
                }
                (&Method::Post, "/clip") => {
                    let mut body = String::new();
                    if let Err(e) = request.as_reader().read_to_string(&mut body) {
                        let err =
                            format!(r#"{{"ok":false,"error":"Failed to read body: {}"}}"#, e);
                        let mut response = Response::from_string(err).with_status_code(400);
                        for h in &cors_headers {
                            response.add_header(h.clone());
                        }
                        let _ = request.respond(response);
                        continue;
                    }

                    let result = handle_clip(&body);
                    let status = if result.contains(r#""ok":true"#) {
                        200
                    } else {
                        500
                    };
                    let mut response = Response::from_string(result).with_status_code(status);
                    for h in &cors_headers {
                        response.add_header(h.clone());
                    }
                    let _ = request.respond(response);
                }
                _ => {
                    let body = r#"{"ok":false,"error":"Not found"}"#;
                    let mut response = Response::from_string(body).with_status_code(404);
                    for h in &cors_headers {
                        response.add_header(h.clone());
                    }
                    let _ = request.respond(response);
                }
            }
        }

            // Server loop exited (shouldn't happen normally)
            DAEMON_STATUS.store(3, Ordering::Relaxed); // error
            restart_count += 1;

            if restart_count >= MAX_RESTART_RETRIES {
                eprintln!(
                    "[Clip Server] Exceeded max restarts ({}). Giving up.",
                    MAX_RESTART_RETRIES
                );
                return;
            }

            eprintln!(
                "[Clip Server] Crashed. Restarting in {}s (attempt {}/{})",
                RESTART_DELAY_SECS, restart_count, MAX_RESTART_RETRIES
            );
            thread::sleep(std::time::Duration::from_secs(RESTART_DELAY_SECS));
        }
    });
}

fn handle_set_project(body: &str) -> String {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return format!(r#"{{"ok":false,"error":"Invalid JSON: {}"}}"#, e),
    };

    let path = match parsed["path"].as_str() {
        // Normalize to forward slashes on ingress so downstream
        // comparisons against frontend-normalized paths succeed.
        Some(p) => p.replace('\\', "/"),
        None => return r#"{"ok":false,"error":"path field is required"}"#.to_string(),
    };

    match CURRENT_PROJECT.lock() {
        Ok(mut guard) => {
            *guard = path;
            r#"{"ok":true}"#.to_string()
        }
        Err(e) => format!(r#"{{"ok":false,"error":"Lock error: {}"}}"#, e),
    }
}

fn handle_ask(body: &str) -> (String, u16) {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => {
            return (
                serde_json::json!({"ok": false, "error": format!("Invalid JSON: {}", e)}).to_string(),
                400,
            )
        }
    };

    let question = match parsed["question"].as_str() {
        Some(q) if !q.trim().is_empty() => q.trim().to_string(),
        _ => {
            return (
                serde_json::json!({"ok": false, "error": "question field is required"}).to_string(),
                400,
            )
        }
    };

    let project_path_from_body = parsed["projectPath"].as_str().unwrap_or("").to_string();
    let project_path = if project_path_from_body.is_empty() {
        match CURRENT_PROJECT.lock() {
            Ok(guard) => guard.clone(),
            Err(e) => {
                return (
                    serde_json::json!({"ok": false, "error": format!("Lock error: {}", e)}).to_string(),
                    500,
                )
            }
        }
    } else {
        project_path_from_body
    };
    let project_path = project_path.replace('\\', "/");

    if project_path.is_empty() {
        return (
            serde_json::json!({
                "ok": false,
                "error": "projectPath is required (set via POST /project or include in request body)",
            })
            .to_string(),
            400,
        );
    }

    let timeout_ms = parsed["timeoutMs"]
        .as_u64()
        .unwrap_or(DEFAULT_ASK_TIMEOUT_MS)
        .clamp(1_000, MAX_ASK_TIMEOUT_MS);

    let id = format!(
        "ask_{}_{}",
        chrono::Local::now().timestamp_millis(),
        ASK_COUNTER.fetch_add(1, Ordering::Relaxed)
    );

    match PENDING_ASKS.lock() {
        Ok(mut pending) => pending.push(PendingAsk {
            id: id.clone(),
            project_path,
            question,
        }),
        Err(e) => {
            return (
                serde_json::json!({"ok": false, "error": format!("Lock error: {}", e)}).to_string(),
                500,
            )
        }
    }

    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let mut results = match ASK_RESULTS.lock() {
        Ok(guard) => guard,
        Err(e) => {
            remove_pending_ask(&id);
            return (
                serde_json::json!({"ok": false, "id": id, "error": format!("Lock error: {}", e)}).to_string(),
                500,
            );
        }
    };

    loop {
        if let Some(pos) = results.iter().position(|result| result.id == id) {
            let result = results.remove(pos);
            return (result.body, result.status);
        }

        let now = Instant::now();
        if now >= deadline {
            remove_pending_ask(&id);
            return (
                serde_json::json!({
                    "ok": false,
                    "id": id,
                    "error": "Timed out waiting for the desktop app to answer. Make sure LLM Wiki is open and an LLM provider is configured.",
                })
                .to_string(),
                504,
            );
        }

        let remaining = deadline.saturating_duration_since(now);
        let waited = ASK_RESULTS_CVAR.wait_timeout(results, remaining);
        match waited {
            Ok((guard, _)) => results = guard,
            Err(e) => {
                remove_pending_ask(&id);
                return (
                    serde_json::json!({"ok": false, "id": id, "error": format!("Lock error: {}", e)}).to_string(),
                    500,
                );
            }
        }
    }
}

fn handle_ask_answer(body: &str) -> String {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return format!(r#"{{"ok":false,"error":"Invalid JSON: {}"}}"#, e),
    };

    let id = match parsed["id"].as_str() {
        Some(id) if !id.trim().is_empty() => id.trim().to_string(),
        _ => return r#"{"ok":false,"error":"id field is required"}"#.to_string(),
    };

    let ok = parsed["ok"].as_bool().unwrap_or(true);
    let (status, body) = if ok {
        let answer = parsed["answer"].as_str().unwrap_or("");
        let references = parsed["references"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        (
            200,
            serde_json::json!({
                "ok": true,
                "id": id.clone(),
                "answer": answer,
                "references": references,
            })
            .to_string(),
        )
    } else {
        let error = parsed["error"].as_str().unwrap_or("Failed to answer question");
        (
            500,
            serde_json::json!({
                "ok": false,
                "id": id.clone(),
                "error": error,
            })
            .to_string(),
        )
    };

    match ASK_RESULTS.lock() {
        Ok(mut results) => {
            results.retain(|result| result.id != id);
            results.push(AskResult { id, status, body });
            while results.len() > 100 {
                results.remove(0);
            }
            ASK_RESULTS_CVAR.notify_all();
            r#"{"ok":true}"#.to_string()
        }
        Err(e) => format!(r#"{{"ok":false,"error":"Lock error: {}"}}"#, e),
    }
}

fn remove_pending_ask(id: &str) {
    if let Ok(mut pending) = PENDING_ASKS.lock() {
        pending.retain(|ask| ask.id != id);
    }
}

fn handle_clip(body: &str) -> String {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return format!(r#"{{"ok":false,"error":"Invalid JSON: {}"}}"#, e),
    };

    let title = parsed["title"].as_str().unwrap_or("Untitled");
    let url = parsed["url"].as_str().unwrap_or("");
    let content = parsed["content"].as_str().unwrap_or("");

    // Use projectPath from request body, or fall back to globally-set project path
    let project_path_from_body = parsed["projectPath"].as_str().unwrap_or("").to_string();
    let project_path = if project_path_from_body.is_empty() {
        match CURRENT_PROJECT.lock() {
            Ok(guard) => guard.clone(),
            Err(e) => return format!(r#"{{"ok":false,"error":"Lock error: {}"}}"#, e),
        }
    } else {
        project_path_from_body
    };
    // Normalize to forward slashes so string comparisons against the
    // frontend-side project path (already normalized) succeed on Windows.
    let project_path = project_path.replace('\\', "/");

    if project_path.is_empty() {
        return r#"{"ok":false,"error":"projectPath is required (set via POST /project or include in request body)"}"#
            .to_string();
    }

    if content.is_empty() {
        return r#"{"ok":false,"error":"content is required"}"#.to_string();
    }

    let date = chrono::Local::now().format("%Y-%m-%d").to_string();
    let date_compact = chrono::Local::now().format("%Y%m%d").to_string();

    // Generate slug from title
    let slug_raw: String = title
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == ' ' || c == '-' {
                c
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join("-")
        .to_lowercase();
    let slug: String = slug_raw.chars().take(50).collect();

    let base_name = format!("{}-{}", slug, date_compact);
    // Use PathBuf for cross-platform path construction
    let dir_path = std::path::Path::new(&project_path).join("raw").join("sources");

    // Ensure directory exists
    if let Err(e) = std::fs::create_dir_all(&dir_path) {
        return format!(
            r#"{{"ok":false,"error":"Failed to create directory: {}"}}"#,
            e
        );
    }

    // Find unique filename
    let mut file_path = dir_path.join(format!("{}.md", base_name));
    let mut counter = 2u32;
    while file_path.exists() {
        file_path = dir_path.join(format!("{}-{}.md", base_name, counter));
        counter += 1;
    }
    // Normalize to forward slashes so the string compares cleanly against
    // frontend-side project paths (already normalized) and survives JSON
    // serialization (the hand-rolled serializer below doesn't escape
    // backslashes; a Windows path like `...\raw\sources\foo.md` would
    // produce invalid JSON escape sequences for `\r` / `\s` / etc).
    let file_path = file_path.to_string_lossy().replace('\\', "/");

    // Build markdown content with web-clip origin
    let markdown = format!(
        "---\ntype: clip\ntitle: \"{}\"\nurl: \"{}\"\nclipped: {}\norigin: web-clip\nsources: []\ntags: [web-clip]\n---\n\n# {}\n\nSource: {}\n\n{}\n",
        title.replace('"', r#"\""#),
        url.replace('"', r#"\""#),
        date,
        title,
        url,
        content,
    );

    if let Err(e) = std::fs::write(&file_path, &markdown) {
        return format!(
            r#"{{"ok":false,"error":"Failed to write file: {}"}}"#,
            e
        );
    }

    // Compute relative path using Path for cross-platform separator handling
    let relative_path = {
        let full = std::path::Path::new(&file_path);
        let base = std::path::Path::new(&project_path);
        full.strip_prefix(base)
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .unwrap_or_else(|_| file_path.replace('\\', "/"))
    };

    // Add to pending clips for frontend to pick up and auto-ingest
    if let Ok(mut pending) = PENDING_CLIPS.lock() {
        pending.push((project_path, file_path.clone()));
    }

    serde_json::json!({
        "ok": true,
        "path": relative_path,
    }).to_string()
}
