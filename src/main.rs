mod storage;

use std::collections::HashSet;
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

/// Just the file name, dropping the key's folders.
fn file_name(key: &str) -> &str {
    key.rsplit('/').next().unwrap_or(key)
}

/// Every image lands straight in `out_dir`, no sub-folders. Keys from
/// different S3 folders can share a file name, so a repeat gets a `_2`,
/// `_3`, ... suffix instead of overwriting the file already there.
fn flat_paths(out_dir: &str, keys: &[String]) -> Vec<PathBuf> {
    let mut taken: HashSet<String> = HashSet::with_capacity(keys.len());

    keys.iter()
        .map(|key| {
            let name = file_name(key);
            let stem = Path::new(name)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or(name);
            let ext = Path::new(name).extension().and_then(|e| e.to_str());

            let mut candidate = name.to_string();
            let mut n = 2;
            while !taken.insert(candidate.clone()) {
                candidate = match ext {
                    Some(ext) => format!("{}_{}.{}", stem, n, ext),
                    None => format!("{}_{}", stem, n),
                };
                n += 1;
            }
            Path::new(out_dir).join(candidate)
        })
        .collect()
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

    let paths = flat_paths(&out_dir, &images);
    let mut tasks: JoinSet<Result<(String, usize), (String, String)>> = JoinSet::new();
    let mut queue = images.into_iter().zip(paths);
    let mut downloaded = 0usize;
    let mut failed = 0usize;

    // Keep at most `concurrency` downloads in flight.
    for _ in 0..concurrency {
        if let Some((key, path)) = queue.next() {
            spawn_download(&mut tasks, &client, &bucket, key, path);
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

        if let Some((key, path)) = queue.next() {
            spawn_download(&mut tasks, &client, &bucket, key, path);
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
    key: String,
    path: PathBuf,
) {
    let client = client.clone();
    let bucket = bucket.to_string();

    tasks.spawn(async move {
        let bytes = client
            .get_object(&bucket, &key)
            .await
            .map_err(|e| (key.clone(), e.to_string()))?;

        // out_dir is created once up front, so the write needs no mkdir here.
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
    fn files_land_flat_in_the_output_dir() {
        let keys = vec!["2026/09/03/uid/a.jpeg".to_string()];
        assert_eq!(flat_paths("data", &keys), vec![PathBuf::from("data/a.jpeg")]);
    }

    #[test]
    fn repeated_names_get_a_suffix_instead_of_overwriting() {
        let keys = vec![
            "x/a.jpeg".to_string(),
            "y/a.jpeg".to_string(),
            "z/a.jpeg".to_string(),
            "w/noext".to_string(),
            "v/noext".to_string(),
        ];
        assert_eq!(
            flat_paths("data", &keys),
            vec![
                PathBuf::from("data/a.jpeg"),
                PathBuf::from("data/a_2.jpeg"),
                PathBuf::from("data/a_3.jpeg"),
                PathBuf::from("data/noext"),
                PathBuf::from("data/noext_2"),
            ]
        );
    }

    #[test]
    fn only_images_pass_the_filter() {
        assert!(is_image("a/b/c.JPEG"));
        assert!(is_image("x.png"));
        assert!(!is_image("x.json"));
        assert!(!is_image("noext"));
    }
}
