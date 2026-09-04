mod storage;

use std::path::{Path, PathBuf};
use storage::client::S3Client;
use storage::config::{self, S3Config};
use tokio::task::JoinSet;

/// Fallbacks for the optional knobs.
const DEFAULT_DOWNLOAD_DIR: &str = "data";
const DEFAULT_CONCURRENCY: usize = 8;

const IMAGE_EXTS: [&str; 8] = [
    "jpg", "jpeg", "png", "gif", "bmp", "webp", "tif", "tiff",
];

fn is_image(key: &str) -> bool {
    Path::new(key)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| IMAGE_EXTS.contains(&e.to_ascii_lowercase().as_str()))
        .unwrap_or(false)
}

/// Local path for a key: everything after the prefix, kept under `out_dir`.
fn local_path(out_dir: &str, prefix: &str, key: &str) -> PathBuf {
    let rel = key.strip_prefix(prefix).unwrap_or(key).trim_start_matches('/');
    Path::new(out_dir).join(rel)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    config::load_dotenv();
    let cfg = S3Config::from_env()?;

    // Folder to download: CLI argument wins over S3_PREFIX.
    let Some(mut prefix) = std::env::args().nth(1).or_else(|| config::optional("S3_PREFIX")) else {
        eprintln!(
            "No folder given. Pass it as an argument or set S3_PREFIX in .env:\n  \
             cargo run -- 2026/09/03/"
        );
        std::process::exit(2);
    };
    if !prefix.ends_with('/') {
        prefix.push('/');
    }

    let out_dir = config::optional("DOWNLOAD_DIR").unwrap_or_else(|| DEFAULT_DOWNLOAD_DIR.into());
    let concurrency = config::optional("DOWNLOAD_CONCURRENCY")
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_CONCURRENCY);

    let bucket = cfg.bucket.clone();
    let client = S3Client::new(cfg).await?;

    let keys = client.list_objects(&bucket, &prefix).await?;
    let (images, skipped): (Vec<String>, Vec<String>) =
        keys.into_iter().partition(|k| is_image(k));

    println!("Found {} image(s) under '{}'", images.len(), prefix);
    if !skipped.is_empty() {
        println!("Skipping {} non-image object(s)", skipped.len());
    }
    if images.is_empty() {
        return Ok(());
    }

    std::fs::create_dir_all(&out_dir)?;
    println!("Downloading {} image(s) into '{}/'...", images.len(), out_dir);

    let mut tasks: JoinSet<Result<(String, usize), (String, String)>> = JoinSet::new();
    let mut queue = images.into_iter();
    let mut downloaded = 0usize;
    let mut failed = 0usize;

    // Keep at most `concurrency` downloads in flight.
    for _ in 0..concurrency {
        if let Some(key) = queue.next() {
            spawn_download(&mut tasks, &client, &bucket, &out_dir, &prefix, key);
        }
    }

    while let Some(joined) = tasks.join_next().await {
        match joined? {
            Ok((key, bytes)) => {
                downloaded += 1;
                println!("  ok   {} ({} bytes)", key, bytes);
            }
            Err((key, msg)) => {
                failed += 1;
                eprintln!("  fail {}: {}", key, msg);
            }
        }

        if let Some(key) = queue.next() {
            spawn_download(&mut tasks, &client, &bucket, &out_dir, &prefix, key);
        }
    }

    println!(
        "Done: {} downloaded, {} failed -> {}/",
        downloaded, failed, out_dir
    );
    Ok(())
}

fn spawn_download(
    tasks: &mut JoinSet<Result<(String, usize), (String, String)>>,
    client: &S3Client,
    bucket: &str,
    out_dir: &str,
    prefix: &str,
    key: String,
) {
    let client = client.clone();
    let bucket = bucket.to_string();
    let path = local_path(out_dir, prefix, &key);

    tasks.spawn(async move {
        let bytes = client
            .get_object(&bucket, &key)
            .await
            .map_err(|e| (key.clone(), e.to_string()))?;

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| (key.clone(), e.to_string()))?;
        }
        tokio::fs::write(&path, &bytes)
            .await
            .map_err(|e| (key.clone(), e.to_string()))?;

        Ok((key, bytes.len()))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_path_keeps_structure_under_prefix() {
        let p = local_path("data", "2026/09/03/", "2026/09/03/uid/a.jpeg");
        assert_eq!(p, Path::new("data/uid/a.jpeg"));
    }

    #[test]
    fn only_images_pass_the_filter() {
        assert!(is_image("a/b/c.JPEG"));
        assert!(is_image("x.png"));
        assert!(!is_image("x.json"));
        assert!(!is_image("noext"));
    }
}
