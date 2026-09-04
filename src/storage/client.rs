use super::{
    config::S3Config,
    error::{S3Error, S3Result},
};
use aws_config::BehaviorVersion;
use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use aws_sdk_s3::{
    Client,
    config::{Builder as S3ConfigBuilder, Region},
    error::SdkError,
    operation::{create_bucket::CreateBucketError, delete_objects::DeleteObjectsOutput},
    primitives::ByteStream,
    types::{BucketLocationConstraint, CreateBucketConfiguration, Delete, ObjectIdentifier},
};
use bytes::Bytes;
use log::info;
use reqwest::get;

#[derive(Clone, Debug)]
pub struct S3Client {
    inner: Client,
    region: String,
    endpoint: String,
}

impl S3Client {
    pub async fn new(cfg: S3Config) -> S3Result<Self> {
        let creds = Credentials::new(&cfg.access_key, &cfg.secret_key, None, None, "minio");
        let cred_provider = SharedCredentialsProvider::new(creds);

        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(cfg.region.clone()))
            .credentials_provider(cred_provider)
            .load()
            .await;

        // force_path_style = true is REQUIRED for MinIO / RustFS / Ceph
        let s3_cfg = S3ConfigBuilder::from(&sdk_config)
            .endpoint_url(&cfg.endpoint_url)
            .force_path_style(true)
            .build();

        let client = Client::from_conf(s3_cfg);
        println!(
            "[Storage][init] Storage Client initialized with url: {}",
            &cfg.endpoint_url
        );
        Ok(Self {
            inner: client,
            region: cfg.region,
            endpoint: cfg.endpoint_url,
        })
    }
    /// Ensure bucket
    pub async fn ensure_bucket(&self, name: &str) -> S3Result<()> {
        let get_bucket = self.inner.get_bucket_acl().bucket(name).send().await;
        match get_bucket {
            Ok(bucket) => {
                print!("Bucket '{}' already exists and is accessible", name);
                Ok(())
            }
            Err(e) => {
                println!("Error getting bucket '{}': {}", name, e);
                Ok(())
            }
        }
    }

    /// Create bucket — idempotent (BucketAlreadyExists is silently ignored).
    pub async fn create_bucket(&self, bucket: &str) -> S3Result<()> {
        println!("Ensuring bucket '{}' exists...", bucket);
        let create_config = (self.region != "us-east-1").then(|| {
            CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(self.region.as_str()))
                .build()
        });

        let mut req = self.inner.create_bucket().bucket(bucket);
        if let Some(cfg) = create_config {
            req = req.create_bucket_configuration(cfg);
        }

        match req.send().await {
            Ok(_) => {
                // info!(%bucket, "bucket created");
                println!("bucket created: {}", bucket);
                Ok(())
            }
            Err(SdkError::ServiceError(ref svc))
                if matches!(
                    svc.err(),
                    CreateBucketError::BucketAlreadyExists(_)
                        | CreateBucketError::BucketAlreadyOwnedByYou(_)
                ) =>
            {
                println!("{} already exists", bucket);
                // debug!(%bucket, "already exists");
                Ok(())
            }
            Err(e) => Err(S3Error::from(e)),
        }
    }

    /// Upload an object from an in-memory buffer.
    pub async fn put_object(&self, bucket: &str, key: &str, data: Vec<u8>) -> S3Result<()> {
        self.inner
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(Bytes::from(data)))
            .send()
            .await
            .map_err(S3Error::from)?;
        print!("{}: {} uploaded", bucket, key);
        // info!(%bucket, %key, "uploaded");
        Ok(())
    }

    /// Download an object into memory.
    pub async fn get_object(&self, bucket: &str, key: &str) -> S3Result<Vec<u8>> {
        let resp = self
            .inner
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                let err = S3Error::from(e);
                if matches!(err, S3Error::NoSuchKey { .. }) {
                    S3Error::NoSuchKey {
                        bucket: bucket.into(),
                        key: key.into(),
                    }
                } else {
                    err
                }
            })?;

        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| S3Error::BodyRead(e.to_string()))?
            .into_bytes();

        // info!(%bucket, %key, bytes = bytes.len(), "downloaded");
        println!("{}: {} ({} bytes)", bucket, key, bytes.len());
        Ok(bytes.to_vec())
    }

    /// List every object key under `prefix`, following pagination to the end.
    pub async fn list_objects(&self, bucket: &str, prefix: &str) -> S3Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut pages = self
            .inner
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .into_paginator()
            .send();

        while let Some(page) = pages.next().await {
            let page = page.map_err(S3Error::from)?;
            for obj in page.contents() {
                if let Some(key) = obj.key() {
                    // MinIO/S3 represent "folders" as zero-byte keys ending in '/'.
                    if !key.ends_with('/') {
                        keys.push(key.to_string());
                    }
                }
            }
        }

        println!("{}: {} object(s) under '{}'", bucket, keys.len(), prefix);
        Ok(keys)
    }

    /// Delete a single object (idempotent — missing key is not an error).
    pub async fn delete_object(&self, bucket: &str, key: &str) -> S3Result<()> {
        self.inner
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(S3Error::from)?;
        // info!(%bucket, %key, "deleted");
        println!("Deleted '{}'", bucket);
        Ok(())
    }

    /// Batch-delete up to N objects, automatically chunked at 1,000 per request.
    pub async fn delete_objects(&self, bucket: &str, keys: &[&str]) -> S3Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        const CHUNK: usize = 1_000;
        let mut errors: Vec<(String, String)> = vec![];

        for chunk in keys.chunks(CHUNK) {
            let ids = chunk
                .iter()
                .map(|k| {
                    ObjectIdentifier::builder()
                        .key(*k)
                        .build()
                        .expect("non-empty key")
                })
                .collect::<Vec<_>>();

            let delete = Delete::builder()
                .set_objects(Some(ids))
                .quiet(true)
                .build()
                .map_err(|e| S3Error::Other(e.to_string()))?;

            let resp: DeleteObjectsOutput = self
                .inner
                .delete_objects()
                .bucket(bucket)
                .delete(delete)
                .send()
                .await
                .map_err(S3Error::from)?;

            if let Some(errs) = resp.errors {
                for e in errs {
                    let key = e.key.unwrap_or_default();
                    let msg = e.message.unwrap_or_else(|| "unknown".into());
                    println!("batch delete error");
                    // warn!(%key, %msg, "batch delete error");
                    errors.push((key, msg));
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(S3Error::BatchDeletePartial(errors))
        }
    }

    /// Direct access to the raw `aws_sdk_s3::Client` for advanced operations.
    pub fn raw(&self) -> &Client {
        &self.inner
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}
