use aws_sdk_s3::{
    error::SdkError,
    operation::{
        create_bucket::CreateBucketError, delete_object::DeleteObjectError,
        delete_objects::DeleteObjectsError, get_object::GetObjectError,
        list_objects_v2::ListObjectsV2Error, put_object::PutObjectError,
    },
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum S3Error {
    #[error("object not found: bucket={bucket}, key={key}")]
    NoSuchKey { bucket: String, key: String },

    #[error("bucket not found: {0}")]
    NoSuchBucket(String),

    #[error("access denied: {0}")]
    AccessDenied(String),

    #[error("network error: {0}")]
    Network(String),

    #[error("failed to read object body: {0}")]
    BodyRead(String),

    /// Contains (key, reason) pairs for every key that failed.
    #[error("batch delete had {count} error(s)", count = .0.len())]
    BatchDeletePartial(Vec<(String, String)>),

    #[error("S3 error: {0}")]
    Other(String),

    #[error("failed to get bucket lists")]
    GetBucketLists,

    #[error("missing environment variable: {0} (see example.env)")]
    MissingEnv(String),

    #[error("listing stalled after {pages} page(s) / {keys} key(s): the server \
             repeated its continuation token instead of advancing")]
    ListingStalled { pages: usize, keys: usize },
}

pub type S3Result<T> = Result<T, S3Error>;

fn classify(msg: String) -> S3Error {
    if msg.contains("AccessDenied") || msg.contains("InvalidAccessKeyId") {
        S3Error::AccessDenied(msg)
    } else if msg.contains("NoSuchBucket") {
        S3Error::NoSuchBucket(msg)
    } else if msg.contains("DispatchFailure")
        || msg.contains("timeout")
        || msg.contains("connection refused")
    {
        S3Error::Network(msg)
    } else {
        S3Error::Other(msg)
    }
}

impl From<SdkError<CreateBucketError>> for S3Error {
    fn from(e: SdkError<CreateBucketError>) -> Self {
        classify(e.to_string())
    }
}
impl From<SdkError<PutObjectError>> for S3Error {
    fn from(e: SdkError<PutObjectError>) -> Self {
        classify(e.to_string())
    }
}
impl From<SdkError<DeleteObjectError>> for S3Error {
    fn from(e: SdkError<DeleteObjectError>) -> Self {
        classify(e.to_string())
    }
}
impl From<SdkError<DeleteObjectsError>> for S3Error {
    fn from(e: SdkError<DeleteObjectsError>) -> Self {
        classify(e.to_string())
    }
}

impl From<SdkError<ListObjectsV2Error>> for S3Error {
    fn from(e: SdkError<ListObjectsV2Error>) -> Self {
        classify(e.to_string())
    }
}

impl From<SdkError<GetObjectError>> for S3Error {
    fn from(e: SdkError<GetObjectError>) -> Self {
        if let SdkError::ServiceError(ref svc) = e {
            if matches!(svc.err(), GetObjectError::NoSuchKey(_)) {
                return S3Error::NoSuchKey {
                    bucket: String::new(),
                    key: String::new(),
                };
            }
        }
        classify(e.to_string())
    }
}
