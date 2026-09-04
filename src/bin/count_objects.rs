//! How many objects sit under a folder, and how much they weigh.
//!
//!     cargo run --bin count_objects -- 2026/09/
//!     cargo run --bin count_objects -- 2026/09/ --all
//!
//! Counts as the pages arrive without keeping the keys, so it stays flat in
//! memory on a prefix holding millions of objects. The per-folder breakdown is
//! free: it groups on the path segment that follows the prefix while counting.

#[path = "../storage/mod.rs"]
mod storage;

use std::collections::HashMap;
use std::time::Instant;
use storage::client::S3Client;
use storage::config::{self, S3Config};

/// Sub-folders listed before the tail is folded into one line.
const TOP_N: usize = 20;

fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    match unit {
        0 => format!("{} B", n),
        _ => format!("{:.1} {}", value, UNITS[unit]),
    }
}

/// The path segment right after the prefix — the sub-folder a key belongs to.
fn group_of<'a>(key: &'a str, prefix: &str) -> &'a str {
    let rel = key.strip_prefix(prefix).unwrap_or(key);
    match rel.find('/') {
        Some(i) => &rel[..i],
        None => ".",
    }
}

#[derive(Default, Clone, Copy)]
struct Tally {
    objects: u64,
    bytes: u64,
}

impl Tally {
    fn add(&mut self, size: u64) {
        self.objects += 1;
        self.bytes += size;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    config::load_dotenv();
    let cfg = S3Config::from_env()?;

    let args: Vec<String> = std::env::args().skip(1).collect();
    let show_all = args.iter().any(|a| a == "--all");
    let Some(mut prefix) = args
        .into_iter()
        .find(|a| !a.starts_with("--"))
        .or_else(|| config::optional("S3_PREFIX"))
    else {
        eprintln!("usage: count_objects <prefix> [--all]");
        std::process::exit(2);
    };
    if !prefix.ends_with('/') {
        prefix.push('/');
    }

    let bucket = cfg.bucket.clone();
    let client = S3Client::new(cfg).await?;

    println!("Counting '{}' in bucket '{}'...", prefix, bucket);
    let started = Instant::now();

    let mut total = Tally::default();
    let mut folders: HashMap<String, Tally> = HashMap::new();
    let mut reported = 0u64;

    let pages = client
        .for_each_page(&bucket, &prefix, |objects| {
            for obj in objects {
                let Some(key) = obj.key() else { continue };
                // Zero-byte "folder" markers are not objects anyone stored.
                if key.ends_with('/') {
                    continue;
                }
                let size = obj.size().unwrap_or(0).max(0) as u64;
                total.add(size);
                folders
                    .entry(group_of(key, &prefix).to_string())
                    .or_default()
                    .add(size);
            }

            // A wide prefix takes many round-trips; say so while they happen.
            if total.objects >= reported + 10_000 {
                reported = total.objects;
                println!(
                    "  ... {} object(s), {} after {:.1}s",
                    total.objects,
                    human_bytes(total.bytes),
                    started.elapsed().as_secs_f64()
                );
            }
        })
        .await?;

    let elapsed = started.elapsed().as_secs_f64();

    if total.objects == 0 {
        println!(
            "Nothing under '{}' ({} page(s), {:.1}s)",
            prefix, pages, elapsed
        );
        return Ok(());
    }

    let mut rows: Vec<(&String, &Tally)> = folders.iter().collect();
    rows.sort_by(|a, b| b.1.objects.cmp(&a.1.objects).then(a.0.cmp(b.0)));

    let shown = if show_all { rows.len() } else { rows.len().min(TOP_N) };
    let width = rows[..shown].iter().map(|(n, _)| n.len()).max().unwrap_or(1);

    println!();
    for (name, tally) in &rows[..shown] {
        println!(
            "  {:<width$}  {:>9} object(s)  {:>10}",
            name,
            tally.objects,
            human_bytes(tally.bytes),
            width = width
        );
    }
    if rows.len() > shown {
        let rest: u64 = rows[shown..].iter().map(|(_, t)| t.objects).sum();
        println!(
            "  ... and {} more folder(s) holding {} object(s) — pass --all to list them",
            rows.len() - shown,
            rest
        );
    }

    println!();
    println!(
        "{} object(s), {}, across {} folder(s) under '{}'",
        total.objects,
        human_bytes(total.bytes),
        rows.len(),
        prefix
    );
    println!("{} request(s) in {:.1}s", pages, elapsed);
    Ok(())
}
