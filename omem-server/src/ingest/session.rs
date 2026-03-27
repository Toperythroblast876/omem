use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase, Select};
use lancedb::table::Table;
use lancedb::Connection;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::domain::error::OmemError;

const SESSION_TABLE: &str = "sessions";

#[derive(Debug, Clone)]
pub struct SessionMessage {
    pub id: String,
    pub session_id: String,
    pub agent_id: String,
    pub role: String,
    pub content: String,
    pub content_hash: String,
    pub tags: Vec<String>,
    pub created_at: String,
}

impl SessionMessage {
    pub fn new(
        session_id: &str,
        agent_id: &str,
        role: &str,
        content: &str,
        tags: Vec<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            session_id: session_id.to_string(),
            agent_id: agent_id.to_string(),
            role: role.to_string(),
            content: content.to_string(),
            content_hash: compute_content_hash(session_id, role, content),
            tags,
            created_at: chrono::Utc::now().to_rfc3339(),
        }
    }
}

pub struct SessionStore {
    db: Connection,
}

impl SessionStore {
    pub async fn new(uri: &str) -> Result<Self, OmemError> {
        let db = lancedb::connect(uri)
            .execute()
            .await
            .map_err(|e| OmemError::Storage(format!("failed to connect to LanceDB for sessions: {e}")))?;
        Ok(Self { db })
    }

    pub async fn init_table(&self) -> Result<(), OmemError> {
        let existing = self
            .db
            .table_names()
            .execute()
            .await
            .map_err(|e| OmemError::Storage(format!("failed to list tables: {e}")))?;

        if existing.contains(&SESSION_TABLE.to_string()) {
            return Ok(());
        }

        self.db
            .create_empty_table(SESSION_TABLE, Self::schema())
            .execute()
            .await
            .map_err(|e| OmemError::Storage(format!("failed to create sessions table: {e}")))?;

        Ok(())
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("session_id", DataType::Utf8, false),
            Field::new("agent_id", DataType::Utf8, false),
            Field::new("role", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, false),
            Field::new("content_hash", DataType::Utf8, false),
            Field::new("tags", DataType::Utf8, false),
            Field::new("created_at", DataType::Utf8, false),
        ]))
    }

    async fn open_table(&self) -> Result<Table, OmemError> {
        self.db
            .open_table(SESSION_TABLE)
            .execute()
            .await
            .map_err(|e| OmemError::Storage(format!("failed to open sessions table: {e}")))
    }

    /// Stores session messages, skipping any whose content_hash already exists.
    /// Returns the count of newly stored messages.
    pub async fn bulk_create(&self, messages: &[SessionMessage]) -> Result<usize, OmemError> {
        if messages.is_empty() {
            return Ok(0);
        }

        let hashes: Vec<&str> = messages.iter().map(|m| m.content_hash.as_str()).collect();
        let existing_hashes = self.get_existing_hashes(&hashes).await?;

        let new_messages: Vec<&SessionMessage> = messages
            .iter()
            .filter(|m| !existing_hashes.contains(&m.content_hash))
            .collect();

        if new_messages.is_empty() {
            return Ok(0);
        }

        let batch = Self::messages_to_batch(&new_messages)?;
        let table = self.open_table().await?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], Self::schema());
        table
            .add(Box::new(reader) as Box<dyn arrow_array::RecordBatchReader + Send>)
            .execute()
            .await
            .map_err(|e| OmemError::Storage(format!("failed to insert session messages: {e}")))?;

        Ok(new_messages.len())
    }

    pub async fn count_by_session(&self, session_id: &str) -> Result<usize, OmemError> {
        let table = self.open_table().await?;
        let batches: Vec<RecordBatch> = table
            .query()
            .only_if(format!("session_id = '{}'", escape_sql(session_id)))
            .execute()
            .await
            .map_err(|e| OmemError::Storage(format!("session query failed: {e}")))?
            .try_collect()
            .await
            .map_err(|e| OmemError::Storage(format!("collect failed: {e}")))?;

        Ok(batches.iter().map(|b| b.num_rows()).sum())
    }

    async fn get_existing_hashes(&self, hashes: &[&str]) -> Result<HashSet<String>, OmemError> {
        if hashes.is_empty() {
            return Ok(HashSet::new());
        }

        let quoted: Vec<String> = hashes
            .iter()
            .map(|h| format!("'{}'", escape_sql(h)))
            .collect();
        let filter = format!("content_hash IN ({})", quoted.join(", "));

        let table = self.open_table().await?;
        let batches: Vec<RecordBatch> = table
            .query()
            .only_if(filter)
            .select(Select::Columns(vec!["content_hash".to_string()]))
            .execute()
            .await
            .map_err(|e| OmemError::Storage(format!("hash lookup query failed: {e}")))?
            .try_collect()
            .await
            .map_err(|e| OmemError::Storage(format!("collect failed: {e}")))?;

        let mut result = HashSet::new();
        for batch in &batches {
            if let Some(col) = batch.column_by_name("content_hash") {
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            result.insert(arr.value(i).to_string());
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    fn messages_to_batch(messages: &[&SessionMessage]) -> Result<RecordBatch, OmemError> {
        let ids: Vec<&str> = messages.iter().map(|m| m.id.as_str()).collect();
        let session_ids: Vec<&str> = messages.iter().map(|m| m.session_id.as_str()).collect();
        let agent_ids: Vec<&str> = messages.iter().map(|m| m.agent_id.as_str()).collect();
        let roles: Vec<&str> = messages.iter().map(|m| m.role.as_str()).collect();
        let contents: Vec<&str> = messages.iter().map(|m| m.content.as_str()).collect();
        let hashes: Vec<&str> = messages.iter().map(|m| m.content_hash.as_str()).collect();
        let tags: Result<Vec<String>, OmemError> = messages
            .iter()
            .map(|m| {
                serde_json::to_string(&m.tags)
                    .map_err(|e| OmemError::Storage(format!("failed to serialize tags: {e}")))
            })
            .collect();
        let tags = tags?;
        let tags_refs: Vec<&str> = tags.iter().map(|t| t.as_str()).collect();
        let created_ats: Vec<&str> = messages.iter().map(|m| m.created_at.as_str()).collect();

        RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(StringArray::from(session_ids)),
                Arc::new(StringArray::from(agent_ids)),
                Arc::new(StringArray::from(roles)),
                Arc::new(StringArray::from(contents)),
                Arc::new(StringArray::from(hashes)),
                Arc::new(StringArray::from(tags_refs)),
                Arc::new(StringArray::from(created_ats)),
            ],
        )
        .map_err(|e| OmemError::Storage(format!("failed to build session RecordBatch: {e}")))
    }
}

fn compute_content_hash(session_id: &str, role: &str, content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(session_id.as_bytes());
    hasher.update(role.as_bytes());
    hasher.update(content.as_bytes());
    let result = hasher.finalize();
    result.iter().map(|b| format!("{b:02x}")).collect()
}

fn escape_sql(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn setup() -> (SessionStore, TempDir) {
        let dir = TempDir::new().expect("temp dir");
        let store = SessionStore::new(dir.path().to_str().expect("path"))
            .await
            .expect("session store");
        store.init_table().await.expect("init");
        (store, dir)
    }

    #[test]
    fn content_hash_deterministic() {
        let h1 = compute_content_hash("sess-1", "user", "hello");
        let h2 = compute_content_hash("sess-1", "user", "hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn content_hash_varies_by_session() {
        let h1 = compute_content_hash("sess-1", "user", "hello");
        let h2 = compute_content_hash("sess-2", "user", "hello");
        assert_ne!(h1, h2);
    }

    #[test]
    fn content_hash_varies_by_role() {
        let h1 = compute_content_hash("sess-1", "user", "hello");
        let h2 = compute_content_hash("sess-1", "assistant", "hello");
        assert_ne!(h1, h2);
    }

    #[test]
    fn content_hash_varies_by_content() {
        let h1 = compute_content_hash("sess-1", "user", "hello");
        let h2 = compute_content_hash("sess-1", "user", "world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn content_hash_is_64_hex_chars() {
        let h = compute_content_hash("s", "r", "c");
        assert_eq!(h.len(), 64);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn session_message_new_generates_uuid_and_hash() {
        let msg = SessionMessage::new("sess-1", "agent-1", "user", "hello world", vec![]);
        assert!(!msg.id.is_empty());
        assert_eq!(msg.session_id, "sess-1");
        assert_eq!(msg.agent_id, "agent-1");
        assert_eq!(msg.role, "user");
        assert_eq!(msg.content, "hello world");
        assert_eq!(msg.content_hash.len(), 64);
        assert!(!msg.created_at.is_empty());
    }

    #[tokio::test]
    async fn bulk_create_stores_messages() {
        let (store, _dir) = setup().await;

        let messages = vec![
            SessionMessage::new("sess-1", "agent-1", "user", "hello", vec![]),
            SessionMessage::new("sess-1", "agent-1", "assistant", "hi there", vec![]),
        ];

        let count = store.bulk_create(&messages).await.expect("bulk_create");
        assert_eq!(count, 2);

        let total = store.count_by_session("sess-1").await.expect("count");
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn test_content_hash_dedup() {
        let (store, _dir) = setup().await;

        let messages = vec![
            SessionMessage::new("sess-1", "agent-1", "user", "hello", vec![]),
            SessionMessage::new("sess-1", "agent-1", "assistant", "hi", vec![]),
        ];

        let first_count = store.bulk_create(&messages).await.expect("first insert");
        assert_eq!(first_count, 2);

        let dupes = vec![
            SessionMessage::new("sess-1", "agent-1", "user", "hello", vec![]),
            SessionMessage::new("sess-1", "agent-1", "assistant", "hi", vec![]),
        ];
        let second_count = store.bulk_create(&dupes).await.expect("second insert");
        assert_eq!(second_count, 0);

        let total = store.count_by_session("sess-1").await.expect("count");
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn bulk_create_empty_returns_zero() {
        let (store, _dir) = setup().await;
        let count = store.bulk_create(&[]).await.expect("empty bulk_create");
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn different_sessions_isolated() {
        let (store, _dir) = setup().await;

        let msgs_a = vec![
            SessionMessage::new("sess-a", "agent-1", "user", "hello", vec![]),
        ];
        let msgs_b = vec![
            SessionMessage::new("sess-b", "agent-1", "user", "hello", vec![]),
        ];

        store.bulk_create(&msgs_a).await.expect("insert a");
        store.bulk_create(&msgs_b).await.expect("insert b");

        let count_a = store.count_by_session("sess-a").await.expect("count a");
        let count_b = store.count_by_session("sess-b").await.expect("count b");
        assert_eq!(count_a, 1);
        assert_eq!(count_b, 1);
    }

    #[tokio::test]
    async fn partial_dedup_mixed_new_and_existing() {
        let (store, _dir) = setup().await;

        let first = vec![
            SessionMessage::new("sess-1", "agent-1", "user", "existing msg", vec![]),
        ];
        store.bulk_create(&first).await.expect("first insert");

        let mixed = vec![
            SessionMessage::new("sess-1", "agent-1", "user", "existing msg", vec![]),
            SessionMessage::new("sess-1", "agent-1", "user", "brand new msg", vec![]),
        ];
        let count = store.bulk_create(&mixed).await.expect("mixed insert");
        assert_eq!(count, 1);

        let total = store.count_by_session("sess-1").await.expect("count");
        assert_eq!(total, 2);
    }
}
