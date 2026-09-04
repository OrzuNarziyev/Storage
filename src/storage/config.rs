use super::error::{S3Error, S3Result};
use std::env;

/// Configuration for the S3-compatible storage client.
/// Supports custom endpoints (MinIO, RustFS, Ceph, etc.).
#[derive(Debug, Clone)]
pub struct S3Config {
    /// Full URL of the S3-compatible endpoint, e.g. `http://localhost:9000`.
    pub endpoint_url: String,
    /// AWS/S3 region string. MinIO accepts any non-empty value; `"us-east-1"` is safe.
    pub region: String,
    /// Access key ID.
    pub access_key: String,
    /// Secret access key.
    pub secret_key: String,
    /// Bucket name.
    pub bucket: String,

    pub api_key: Option<String>,
}

impl S3Config {
    /// Read the config from the environment. Call [`load_dotenv`] first if you
    /// want values from a `.env` file to be picked up.
    ///
    /// Required: `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`.
    /// Optional: `S3_REGION` (defaults to `us-east-1`), `S3_API_KEY`.
    pub fn from_env() -> S3Result<Self> {
        Ok(Self {
            endpoint_url: required("S3_ENDPOINT_URL")?,
            region: optional("S3_REGION").unwrap_or_else(|| "us-east-1".to_string()),
            access_key: required("S3_ACCESS_KEY")?,
            secret_key: required("S3_SECRET_KEY")?,
            bucket: required("S3_BUCKET")?,
            api_key: optional("S3_API_KEY"),
        })
    }
}

/// Load a `.env` file from the current directory (or any parent), if one exists.
/// A missing file is not an error — real environment variables are enough.
pub fn load_dotenv() {
    match dotenvy::dotenv() {
        Ok(path) => println!("[Storage][env] loaded {}", path.display()),
        Err(e) if e.not_found() => {}
        Err(e) => eprintln!("[Storage][env] could not read .env: {}", e),
    }
}

fn required(key: &str) -> S3Result<String> {
    optional(key).ok_or_else(|| S3Error::MissingEnv(key.to_string()))
}

/// An unset variable and an empty/whitespace one are treated the same.
pub fn optional(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}
