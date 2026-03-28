use std::sync::Arc;

use tracing::{error, info, warn};

use crate::domain::error::OmemError;
use crate::domain::types::MemoryState;
use crate::embed::EmbedService;
use crate::ingest::extractor::FactExtractor;
use crate::ingest::reconciler::Reconciler;
use crate::ingest::types::IngestMessage;
use crate::llm::LlmService;
use crate::store::{LanceStore, SpaceStore};

const INTELLIGENCE_CHUNK_SIZE: usize = 80_000;

pub struct IntelligenceTask {
    store: Arc<LanceStore>,
    extractor: Arc<FactExtractor>,
    reconciler: Arc<Reconciler>,
    space_store: Arc<SpaceStore>,
    task_id: String,
    tenant_id: String,
}

impl IntelligenceTask {
    pub fn new(
        store: Arc<LanceStore>,
        embed: Arc<dyn EmbedService>,
        llm: Arc<dyn LlmService>,
        space_store: Arc<SpaceStore>,
        task_id: String,
        tenant_id: String,
    ) -> Self {
        let mut extractor = FactExtractor::new(llm.clone());
        extractor.max_input_chars = INTELLIGENCE_CHUNK_SIZE;

        let reconciler = Reconciler::new(llm, store.clone(), embed);

        Self {
            store,
            extractor: Arc::new(extractor),
            reconciler: Arc::new(reconciler),
            space_store,
            task_id,
            tenant_id,
        }
    }

    pub async fn run(&self) {
        if let Err(e) = self.run_inner().await {
            error!(task_id = %self.task_id, error = %e, "intelligence task failed");
            self.set_task_field(|t| {
                t.status = "failed".to_string();
                t.errors.push(format!("{e}"));
                t.completed_at = Some(chrono::Utc::now().to_rfc3339());
            })
            .await;
        }
    }

    async fn run_inner(&self) -> Result<(), OmemError> {
        self.set_task_field(|t| t.extraction_status = "running".to_string())
            .await;

        let memories = self.store.list_all_active().await?;
        let import_memories: Vec<_> = memories
            .into_iter()
            .filter(|m| {
                m.source
                    .as_deref()
                    .map(|s| s.starts_with("import"))
                    .unwrap_or(false)
            })
            .collect();

        if import_memories.is_empty() {
            info!(task_id = %self.task_id, "no import memories found, completing");
            self.set_task_field(|t| {
                t.extraction_status = "completed".to_string();
                t.reconcile_status = "skipped".to_string();
                t.status = "completed".to_string();
                t.completed_at = Some(chrono::Utc::now().to_rfc3339());
            })
            .await;
            return Ok(());
        }

        let full_text = import_memories
            .iter()
            .map(|m| m.content.as_str())
            .collect::<Vec<_>>()
            .join("\n\n");

        let chunks = split_into_chunks(&full_text, INTELLIGENCE_CHUNK_SIZE);

        let mut all_facts = Vec::new();
        for (i, chunk) in chunks.iter().enumerate() {
            let messages = vec![IngestMessage {
                role: "user".to_string(),
                content: chunk.to_string(),
            }];

            match self.extractor.extract(&messages, None).await {
                Ok(facts) => {
                    all_facts.extend(facts);
                    let facts_count = all_facts.len();
                    let total = chunks.len();
                    let progress = i + 1;
                    self.set_task_field(move |t| {
                        t.extraction_chunks = total;
                        t.extraction_facts = facts_count;
                        t.extraction_progress = progress;
                    })
                    .await;
                }
                Err(e) => {
                    warn!(chunk = i, error = %e, task_id = %self.task_id, "chunk extraction failed");
                    let err_msg = format!("chunk {} extraction failed: {}", i, e);
                    self.set_task_field(move |t| t.errors.push(err_msg))
                        .await;
                }
            }
        }

        for old in &import_memories {
            let mut archived = old.clone();
            archived.state = MemoryState::Archived;
            archived.updated_at = chrono::Utc::now().to_rfc3339();
            if let Err(e) = self.store.update(&archived, None).await {
                warn!(memory_id = %old.id, error = %e, "failed to archive import fragment");
            }
        }

        self.set_task_field(|t| t.extraction_status = "completed".to_string())
            .await;

        if all_facts.is_empty() {
            info!(task_id = %self.task_id, "no facts extracted, completing");
            self.set_task_field(|t| {
                t.reconcile_status = "skipped".to_string();
                t.status = "completed".to_string();
                t.completed_at = Some(chrono::Utc::now().to_rfc3339());
            })
            .await;
            return Ok(());
        }

        self.set_task_field(|t| t.reconcile_status = "running".to_string())
            .await;

        match self.reconciler.reconcile(&all_facts, &self.tenant_id).await {
            Ok(memories) => {
                let fact_count = all_facts.len();
                let mem_count = memories.len();
                info!(
                    task_id = %self.task_id,
                    fact_count,
                    memory_count = mem_count,
                    "intelligence reconciliation complete"
                );
                self.set_task_field(move |t| {
                    t.reconcile_relations = fact_count;
                    t.reconcile_merged = mem_count;
                    t.reconcile_progress = fact_count;
                })
                .await;
            }
            Err(e) => {
                error!(error = %e, task_id = %self.task_id, "reconciliation failed");
                let err_msg = format!("reconciliation failed: {e}");
                self.set_task_field(move |t| t.errors.push(err_msg))
                    .await;
            }
        }

        self.set_task_field(|t| {
            t.reconcile_status = "completed".to_string();
            t.status = "completed".to_string();
            t.completed_at = Some(chrono::Utc::now().to_rfc3339());
        })
        .await;

        Ok(())
    }

    async fn set_task_field<F: FnOnce(&mut crate::store::spaces::ImportTaskRecord)>(&self, f: F) {
        if let Ok(Some(mut task)) = self.space_store.get_import_task(&self.task_id).await {
            f(&mut task);
            let _ = self.space_store.update_import_task(&task).await;
        }
    }
}

pub fn split_into_chunks(text: &str, max_chars: usize) -> Vec<String> {
    if text.len() <= max_chars {
        return vec![text.to_string()];
    }

    let mut chunks = Vec::new();
    let mut start = 0;
    while start < text.len() {
        let end = (start + max_chars).min(text.len());
        let boundary = text[start..end]
            .rfind("\n\n")
            .map(|pos| start + pos + 2)
            .unwrap_or(end);
        chunks.push(text[start..boundary].to_string());
        start = boundary;
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_chunk_for_small_text() {
        let chunks = split_into_chunks("hello world", 100);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], "hello world");
    }

    #[test]
    fn splits_at_double_newline() {
        let text = "part one\n\npart two\n\npart three";
        let chunks = split_into_chunks(text, 15);
        assert!(chunks.len() >= 2);
        assert!(chunks[0].ends_with("\n\n") || !chunks[0].contains("part two"));
    }

    #[test]
    fn handles_no_boundary() {
        let text = "a".repeat(200);
        let chunks = split_into_chunks(&text, 100);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 100);
        assert_eq!(chunks[1].len(), 100);
    }

    #[test]
    fn empty_text() {
        let chunks = split_into_chunks("", 100);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], "");
    }

    #[test]
    fn exact_boundary() {
        let text = "abc\n\ndef\n\nghi";
        let chunks = split_into_chunks(text, 5);
        assert!(chunks.len() >= 2);
        for chunk in &chunks {
            assert!(!chunk.is_empty());
        }
    }
}
