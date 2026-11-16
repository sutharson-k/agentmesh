//! AgentMesh load generator.
//!
//! Submits a large batch of tasks to a running control plane, samples the
//! stats endpoint while they drain, then verifies completion and per-agent
//! sandbox uniqueness. Exits non-zero when the run did not fully succeed.
//!
//!     agentmesh-loadgen --url http://127.0.0.1:8080 --tasks 1000
//!     agentmesh-loadgen --tasks 20000 --tenants 8 --batch 500 --json

use std::collections::HashSet;
use std::io::Write;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::Parser;
use futures::{stream, StreamExt};
use reqwest::{Client, Method};
use serde_json::{json, Value};

const TERMINAL: [&str; 4] = ["succeeded", "failed", "canceled", "timed_out"];

#[derive(Parser, Debug)]
#[command(
    name = "agentmesh-loadgen",
    version,
    about = "Load generator and benchmark for the AgentMesh control plane"
)]
struct Args {
    /// Control-plane base URL.
    #[arg(long, env = "AGENTMESH_URL", default_value = "http://127.0.0.1:8080")]
    url: String,

    /// Bearer token, when the control plane has auth enabled. Prefer the
    /// AGENTMESH_TOKEN environment variable so it never lands in shell history.
    #[arg(long, env = "AGENTMESH_TOKEN", hide_env_values = true)]
    token: Option<String>,

    /// Read the bearer token from a file instead of argv.
    #[arg(long, value_name = "PATH")]
    token_file: Option<std::path::PathBuf>,

    /// Number of tasks (agents) to submit.
    #[arg(long, default_value_t = 1000)]
    tasks: usize,

    /// Number of tenants to spread tasks across (exercises WFQ).
    #[arg(long, default_value_t = 3)]
    tenants: usize,

    /// Tasks per HTTP batch (max 1000).
    #[arg(long, default_value_t = 250)]
    batch: usize,

    /// Concurrent in-flight batch submissions.
    #[arg(long, default_value_t = 4)]
    submit_concurrency: usize,

    /// Per-task timeout recorded in the task spec.
    #[arg(long, default_value_t = 900)]
    timeout_s: u64,

    /// Give up after this many seconds of waiting for terminal states.
    #[arg(long, default_value_t = 300)]
    wait_timeout_s: u64,

    /// Stats poll interval in milliseconds.
    #[arg(long, default_value_t = 250)]
    poll_ms: u64,

    /// Mock-agent sleep in milliseconds (requires the fake backend).
    #[arg(long, default_value_t = 0)]
    sleep_ms: u64,

    /// Emit a single JSON summary instead of human-readable output.
    #[arg(long)]
    json: bool,

    /// Exit 0 even when some tasks failed.
    #[arg(long)]
    allow_failures: bool,

    /// Submit even when the fleet has no online workers.
    #[arg(long)]
    allow_empty_fleet: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    match run(args).await {
        Ok(code) => std::process::exit(code),
        Err(err) => {
            eprintln!("loadgen: {err:#}");
            std::process::exit(2);
        }
    }
}

async fn run(args: Args) -> Result<i32> {
    if args.tasks == 0 {
        bail!("--tasks must be greater than zero");
    }
    if args.tenants == 0 {
        bail!("--tenants must be greater than zero");
    }
    if args.submit_concurrency == 0 {
        bail!("--submit-concurrency must be greater than zero");
    }
    let batch_size = args.batch.clamp(1, 1000);
    let base = args.url.trim_end_matches('/').to_string();
    let token = match &args.token_file {
        Some(path) => Some(
            std::fs::read_to_string(path)
                .with_context(|| format!("reading token file {}", path.display()))?
                .trim()
                .to_string(),
        ),
        None => args.token.clone(),
    };
    if let Some(value) = &token {
        if value.is_empty() {
            bail!("token is empty; unset AGENTMESH_TOKEN or pass --token-file");
        }
    }
    let client = Client::builder()
        .timeout(Duration::from_secs(60))
        .user_agent(concat!("agentmesh-loadgen/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building HTTP client")?;

    let fleet = get_json(&client, &base, token.as_deref(), "/v1/stats").await?;
    let online = fleet
        .pointer("/workers/online")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    // The status counters in /v1/stats are cumulative for the whole database.
    // Remember how many tasks were already terminal so a run against a warm
    // database still waits for *its own* tasks instead of exiting instantly.
    let baseline_terminal: u64 = TERMINAL
        .iter()
        .map(|s| {
            fleet
                .pointer(&format!("/tasks/by_status/{s}"))
                .and_then(Value::as_u64)
                .unwrap_or(0)
        })
        .sum();
    if online == 0 && !args.allow_empty_fleet {
        bail!(
            "no online workers at {base}; start one with `agentmesh worker --backend fake` or pass --allow-empty-fleet"
        );
    }

    if !args.json {
        println!();
        println!(
            "  AgentMesh loadgen · {} tasks · {} tenants · batch {} · {} workers online",
            args.tasks, args.tenants, batch_size, online
        );
        println!();
    }

    let tasks: Vec<Value> = (0..args.tasks)
        .map(|i| {
            json!({
                "name": format!("load-{i:06}"),
                "tenant": format!("load-tenant-{}", i % args.tenants),
                "priority": 50,
                "agent": {
                    "kind": "mock",
                    "prompt": format!("[sleep={}] load test {i}", args.sleep_ms as f64 / 1000.0),
                },
                "cpu": 1,
                "memory_mb": 128,
                "timeout_s": args.timeout_s,
                "labels": {"loadgen": "true"},
            })
        })
        .collect();

    let started = Instant::now();
    let batches: Vec<Vec<Value>> = tasks.chunks(batch_size).map(|c| c.to_vec()).collect();
    let mut created = 0usize;
    let mut duplicates = 0usize;
    let mut submissions = stream::iter(batches.into_iter().map(|batch| {
        let client = client.clone();
        let base = base.clone();
        let token = token.clone();
        async move {
            let response = post_json(
                &client,
                &base,
                token.as_deref(),
                "/v1/tasks/batch",
                &Value::Array(batch),
            )
            .await?;
            let created = response
                .get("created")
                .and_then(Value::as_u64)
                .unwrap_or(0) as usize;
            let total = response
                .get("tasks")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or(0);
            Ok::<_, anyhow::Error>((created, total.saturating_sub(created)))
        }
    }))
    .buffer_unordered(args.submit_concurrency);
    while let Some(result) = submissions.next().await {
        let (c, d) = result?;
        created += c;
        duplicates += d;
    }
    let submit_secs = started.elapsed().as_secs_f64();

    let mut peak_active = 0u64;
    let mut peak_queued = 0u64;
    let mut peak_oldest_wait = 0u64;
    let mut samples = 0u64;
    let deadline = Instant::now() + Duration::from_secs(args.wait_timeout_s);
    let poll = Duration::from_millis(args.poll_ms.max(50));

    let workers_online: u64 = loop {
        let stats = get_json(&client, &base, token.as_deref(), "/v1/stats").await?;
        let active = stats.pointer("/tasks/active").and_then(Value::as_u64).unwrap_or(0);
        let queued = stats.pointer("/tasks/queued").and_then(Value::as_u64).unwrap_or(0);
        let oldest = stats
            .pointer("/tasks/oldest_queued_age_s")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let terminal = TERMINAL
            .iter()
            .map(|s| {
                stats
                    .pointer(&format!("/tasks/by_status/{s}"))
                    .and_then(Value::as_u64)
                    .unwrap_or(0)
            })
            .sum::<u64>()
            .saturating_sub(baseline_terminal);
        peak_active = peak_active.max(active);
        peak_queued = peak_queued.max(queued);
        peak_oldest_wait = peak_oldest_wait.max(oldest);
        samples += 1;
        if !args.json {
            let line = format!(
                "  active {:>5}  ·  queued {:>5}  ·  done {:>5}/{:<5}  ·  peak {:>5}",
                active, queued, terminal, args.tasks, peak_active
            );
            print!("\r{line}");
            let _ = std::io::stdout().flush();
        }
        if terminal >= args.tasks as u64 {
            break stats
                .pointer("/workers/online")
                .and_then(Value::as_u64)
                .unwrap_or(online);
        }
        if Instant::now() >= deadline {
            if !args.json {
                println!();
            }
            bail!(
                "timed out after {}s: {} of {} tasks reached a terminal state",
                args.wait_timeout_s,
                terminal,
                args.tasks
            );
        }
        tokio::time::sleep(poll).await;
    };
    let total_secs = started.elapsed().as_secs_f64();
    if !args.json {
        println!("\n");
    }

    // Full listing: used for isolation checks, percentiles and failure samples.
    let mut listing: Vec<Value> = Vec::with_capacity(args.tasks);
    let mut offset = 0usize;
    while listing.len() < args.tasks {
        let page: Value = get_json(
            &client,
            &base,
            token.as_deref(),
            &format!("/v1/tasks?limit=1000&offset={offset}"),
        )
        .await?;
        let items = page
            .get("tasks")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if items.is_empty() {
            break;
        }
        offset += items.len();
        listing.extend(items);
    }

    let mut by_status: std::collections::BTreeMap<String, u64> = Default::default();
    let mut workspaces = HashSet::new();
    let mut sandbox_keys = HashSet::new();
    let mut durations = Vec::new();
    let mut waits = Vec::new();
    let mut failures: Vec<Value> = Vec::new();
    for task in &listing {
        let status = task
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        *by_status.entry(status.to_string()).or_insert(0) += 1;
        if let Some(result) = task.get("result") {
            if let Some(ws) = result.get("workspace").and_then(Value::as_str) {
                workspaces.insert(ws.to_string());
            }
            if let Some(key) = result.get("sandbox_key").and_then(Value::as_str) {
                sandbox_keys.insert(key.to_string());
            }
        }
        if let Some(d) = task.get("duration_s").and_then(Value::as_f64) {
            durations.push(d);
        }
        if let Some(w) = task.get("queue_wait_s").and_then(Value::as_f64) {
            waits.push(w);
        }
        if status != "succeeded" && failures.len() < 5 {
            failures.push(json!({
                "name": task.get("name"),
                "status": status,
                "error": task.get("error"),
            }));
        }
    }

    let succeeded = *by_status.get("succeeded").unwrap_or(&0);
    let failed = args.tasks as u64 - succeeded;
    let isolation = if succeeded > 0 && (!workspaces.is_empty() || !sandbox_keys.is_empty()) {
        Some(
            workspaces.len() as u64 == succeeded && sandbox_keys.len() as u64 == succeeded,
        )
    } else {
        None
    };

    let summary = json!({
        "url": base,
        "submitted": created,
        "duplicates": duplicates,
        "completed": succeeded,
        "by_status": by_status,
        "workers_online": workers_online,
        "submit_seconds": round(submit_secs),
        "total_seconds": round(total_secs),
        "throughput_per_s": round(args.tasks as f64 / total_secs.max(0.001)),
        "peak_active": peak_active,
        "peak_queued": peak_queued,
        "peak_oldest_queued_age_s": peak_oldest_wait,
        "samples": samples,
        "unique_workspaces": workspaces.len(),
        "unique_sandboxes": sandbox_keys.len(),
        "isolation_ok": isolation,
        "queue_wait_s": stats_summary(&mut waits),
        "duration_s": stats_summary(&mut durations),
        "failures": failures,
    });

    if args.json {
        println!("{}", serde_json::to_string_pretty(&summary)?);
    } else {
        println!("  summary");
        println!("    submitted           {created} tasks in {:.2}s", submit_secs);
        println!(
            "    completed           {succeeded}/{} in {:.2}s  ({:.1} agents/s)",
            args.tasks,
            total_secs,
            args.tasks as f64 / total_secs.max(0.001)
        );
        println!(
            "    peak concurrency    {peak_active}   peak queue {peak_queued}"
        );
        if let Some(q) = summary.get("queue_wait_s") {
            println!(
                "    queue wait          avg {}s   p50 {}s   p95 {}s",
                q["avg"], q["p50"], q["p95"]
            );
        }
        if let Some(d) = summary.get("duration_s") {
            println!(
                "    agent duration      avg {}s   p50 {}s   p95 {}s",
                d["avg"], d["p50"], d["p95"]
            );
        }
        match isolation {
            Some(true) => println!(
                "    sandboxes           {} unique workspaces, {} unique sandboxes",
                workspaces.len(),
                sandbox_keys.len()
            ),
            Some(false) => println!(
                "    sandboxes           MISMATCH: {} workspaces, {} sandboxes for {succeeded} successes",
                workspaces.len(),
                sandbox_keys.len()
            ),
            None => println!("    sandboxes           not reported by this backend"),
        }
        if !failures.is_empty() {
            println!("    failures (first {})", failures.len());
            for failure in &failures {
                println!(
                    "      {}  {}  {}",
                    failure["name"].as_str().unwrap_or("?"),
                    failure["status"].as_str().unwrap_or("?"),
                    failure["error"].as_str().unwrap_or("")
                );
            }
        }
        println!();
    }

    let isolation_broken = isolation == Some(false);
    if (failed > 0 && !args.allow_failures) || isolation_broken {
        return Ok(1);
    }
    Ok(0)
}

fn round(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

fn stats_summary(values: &mut [f64]) -> Value {
    if values.is_empty() {
        return json!({"avg": null, "p50": null, "p95": null, "max": null});
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let sum: f64 = values.iter().sum();
    json!({
        "avg": round(sum / values.len() as f64),
        "p50": round(percentile(values, 0.50)),
        "p95": round(percentile(values, 0.95)),
        "max": round(*values.last().unwrap()),
    })
}

fn percentile(values: &[f64], p: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let rank = (p * values.len() as f64).ceil() as usize;
    values[rank.saturating_sub(1).min(values.len() - 1)]
}

async fn get_json(client: &Client, base: &str, token: Option<&str>, path: &str) -> Result<Value> {
    send(client, base, token, Method::GET, path, None).await
}

async fn post_json(
    client: &Client,
    base: &str,
    token: Option<&str>,
    path: &str,
    body: &Value,
) -> Result<Value> {
    send(client, base, token, Method::POST, path, Some(body)).await
}

async fn send(
    client: &Client,
    base: &str,
    token: Option<&str>,
    method: Method,
    path: &str,
    body: Option<&Value>,
) -> Result<Value> {
    let url = format!("{base}{path}");
    let mut request = client.request(method, &url);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    if let Some(body) = body {
        request = request.json(body);
    }
    let response = request.send().await.with_context(|| format!("{url}"))?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        bail!(
            "{} {} -> {}: {}",
            if body.is_some() { "POST" } else { "GET" },
            path,
            status,
            text.chars().take(400).collect::<String>()
        );
    }
    serde_json::from_str(&text).with_context(|| format!("decoding response from {path}"))
}
