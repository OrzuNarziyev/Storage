//! Diagnostic: list the object keys under a prefix without downloading anything.
//!
//!     cargo run --bin list_objects_path -- 2026/09/
//!     cargo run --bin list_objects_path -- 2026/09/ --count   # totals only
//!
//! Prints the running total after every 1,000-key page, so a prefix that simply
//! holds a lot of objects is easy to tell apart from one that is stuck.

#[path = "../storage/mod.rs"]
mod storage;

use std::time::Instant;
use storage::client::S3Client;
use storage::config::{self, S3Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    config::load_dotenv();
    let cfg = S3Config::from_env()?;

    let args: Vec<String> = std::env::args().skip(1).collect();
    let count_only = args.iter().any(|a| a == "--count");
    let Some(mut prefix) = args
        .into_iter()
        .find(|a| !a.starts_with("--"))
        .or_else(|| config::optional("S3_PREFIX"))
    else {
        eprintln!("usage: list_objects_path <prefix> [--count]");
        std::process::exit(2);
    };
    if !prefix.ends_with('/') {
        prefix.push('/');
    }

    let bucket = cfg.bucket.clone();
    let client = S3Client::new(cfg).await?;

    println!("Listing '{}' in bucket '{}'...", prefix, bucket);
    let started = Instant::now();

    let keys = client
        .list_objects_with(&bucket, &prefix, |total| {
            let secs = started.elapsed().as_secs_f64();
            println!(
                "  ... {} key(s) after {:.1}s ({:.0} keys/s)",
                total,
                secs,
                total as f64 / secs.max(0.001)
            );
        })
        .await?;

    println!(
        "{} key(s) under '{}' in {:.1}s",
        keys.len(),
        prefix,
        started.elapsed().as_secs_f64()
    );

    if !count_only {
        for key in &keys {
            println!("{}", key);
        }
    }
    Ok(())
}
