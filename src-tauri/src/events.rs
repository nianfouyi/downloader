use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type TaskId = Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Queued,
    Downloading,
    Paused,
    Completed,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SegmentStatus {
    Pending,
    Downloading,
    Completed,
    Failed,
    Retrying,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EncryptionStatus {
    Unknown,
    Fetching,
    Ready,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskLog {
    pub timestamp: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SegmentSnapshot {
    pub seq: u64,
    pub status: SegmentStatus,
    pub speed_bps: Option<f64>,
    pub size_bytes: Option<u64>,
    pub retries: u32,
    pub last_updated: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EncryptionSnapshot {
    pub method: Option<String>,
    pub key_uri: Option<String>,
    pub status: EncryptionStatus,
    pub iv: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskSnapshot {
    pub id: String,
    pub title: String,
    pub playlist_url: String,
    pub playlist_type: String,
    pub quality: Option<String>,
    pub resolution: Option<String>,
    pub audio: Option<String>,
    pub save_dir: String,
    pub total_segments: u64,
    pub downloaded_segments: u64,
    pub downloaded_bytes: u64,
    pub progress: f32,
    pub speed_bps: f64,
    pub status: TaskStatus,
    pub eta_seconds: Option<u64>,
    pub created_at: String,
    pub last_event: Option<String>,
    pub average_segment_size_bytes: Option<u64>,
    pub retries: u32,
    pub encryption: Option<EncryptionSnapshot>,
    pub segments: Vec<SegmentSnapshot>,
    pub logs: Vec<TaskLog>,
    pub output_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum FrontendEvent {
    Snapshot {
        tasks: Vec<TaskSnapshot>,
    },
    TaskCreated {
        task: TaskSnapshot,
    },
    TaskUpdated {
        task: TaskSnapshot,
    },
    TaskCompleted {
        #[serde(rename = "taskId")]
        task_id: String,
        task: TaskSnapshot,
    },
    TaskStatus {
        #[serde(rename = "taskId")]
        task_id: String,
        status: TaskStatus,
        message: Option<String>,
    },
    TaskError {
        #[serde(rename = "taskId")]
        task_id: String,
        message: String,
    },
    SegmentUpdated {
        #[serde(rename = "taskId")]
        task_id: String,
        segment: SegmentSnapshot,
    },
    LogAppended {
        #[serde(rename = "taskId")]
        task_id: String,
        log: TaskLog,
    },
    KeyStatus {
        #[serde(rename = "taskId")]
        task_id: String,
        encryption: EncryptionSnapshot,
    },
    TaskRemoved {
        #[serde(rename = "taskId")]
        task_id: String,
    },
}
