use std::sync::Arc;

use axum::extract::{Extension, Path, Query, State};
use axum::Json;
use axum_extra::extract::Multipart;
use serde::Deserialize;
use uuid::Uuid;

use crate::api::server::{personal_space_id, AppState};
use crate::domain::error::OmemError;
use crate::domain::memory::Memory;
use crate::domain::tenant::AuthInfo;
use crate::domain::types::MemoryType;
use crate::ingest::intelligence::IntelligenceTask;
use crate::store::spaces::ImportTaskRecord;

#[derive(Deserialize)]
pub struct ListImportsQuery {
    #[serde(default = "default_import_limit")]
    pub limit: usize,
}

fn default_import_limit() -> usize {
    50
}

pub async fn create_import(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<AuthInfo>,
    mut multipart: Multipart,
) -> Result<Json<ImportTaskRecord>, OmemError> {
    let mut file_data: Option<Vec<u8>> = None;
    let mut filename = String::new();
    let mut file_type = String::from("memory");
    let mut agent_id: Option<String> = None;
    let mut session_id: Option<String> = None;
    let mut space_id: Option<String> = None;
    let mut post_process = true;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| OmemError::Validation(format!("multipart error: {e}")))?
    {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "file" => {
                filename = field.file_name().unwrap_or("unknown").to_string();
                file_data = Some(
                    field
                        .bytes()
                        .await
                        .map_err(|e| OmemError::Validation(format!("read file: {e}")))?
                        .to_vec(),
                );
            }
            "file_type" => {
                file_type = field
                    .text()
                    .await
                    .map_err(|e| OmemError::Validation(format!("{e}")))?;
            }
            "agent_id" => {
                agent_id = Some(
                    field
                        .text()
                        .await
                        .map_err(|e| OmemError::Validation(format!("{e}")))?,
                );
            }
            "session_id" => {
                session_id = Some(
                    field
                        .text()
                        .await
                        .map_err(|e| OmemError::Validation(format!("{e}")))?,
                );
            }
            "space_id" => {
                space_id = Some(
                    field
                        .text()
                        .await
                        .map_err(|e| OmemError::Validation(format!("{e}")))?,
                );
            }
            "post_process" => {
                let val = field
                    .text()
                    .await
                    .map_err(|e| OmemError::Validation(format!("{e}")))?;
                post_process = val != "false" && val != "0";
            }
            _ => {}
        }
    }

    let data = file_data.ok_or_else(|| OmemError::Validation("no 'file' field".to_string()))?;
    let content =
        String::from_utf8(data).map_err(|_| OmemError::Validation("not valid UTF-8".to_string()))?;
    let target_space = space_id.unwrap_or_else(|| personal_space_id(&auth.tenant_id));
    let store = state.store_manager.get_store(&target_space).await?;
    let task_id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();

    let mut imported = 0usize;
    let mut skipped = 0usize;
    let mut errors: Vec<String> = Vec::new();
    let total_items;

    match file_type.as_str() {
        "memory" => {
            let items = parse_memory_json(&content)?;
            total_items = items.len();
            for item in &items {
                let c = item.get("content").and_then(|v| v.as_str()).unwrap_or("");
                if c.is_empty() {
                    skipped += 1;
                    continue;
                }
                let tags: Vec<String> = item
                    .get("tags")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_else(|| vec!["imported".into()]);
                let mut m = Memory::new(
                    c,
                    crate::domain::category::Category::Entities,
                    MemoryType::Insight,
                    &auth.tenant_id,
                );
                m.tags = tags;
                m.source = Some("import".into());
                m.space_id = target_space.clone();
                if let Some(ref a) = agent_id {
                    m.owner_agent_id = a.clone();
                }
                let v = embed(&state, c).await;
                match store.create(&m, v.as_deref()).await {
                    Ok(()) => imported += 1,
                    Err(e) => errors.push(format!("{e}")),
                }
            }
        }
        "session" => {
            let msgs = parse_session(&content);
            total_items = msgs.len();
            for (role, c) in &msgs {
                if c.len() < 20 {
                    skipped += 1;
                    continue;
                }
                let cat = if role == "user" {
                    crate::domain::category::Category::Events
                } else {
                    crate::domain::category::Category::Cases
                };
                let mut m = Memory::new(c, cat, MemoryType::Session, &auth.tenant_id);
                m.source = Some("import-session".into());
                m.space_id = target_space.clone();
                if let Some(ref s) = session_id {
                    m.session_id = Some(s.clone());
                }
                if let Some(ref a) = agent_id {
                    m.owner_agent_id = a.clone();
                }
                let v = embed(&state, c).await;
                match store.create(&m, v.as_deref()).await {
                    Ok(()) => imported += 1,
                    Err(e) => errors.push(format!("{e}")),
                }
            }
        }
        "markdown" => {
            let paras: Vec<&str> = content
                .split("\n\n")
                .map(|p| p.trim())
                .filter(|p| p.len() > 20)
                .collect();
            total_items = paras.len();
            for &p in &paras {
                let mut m = Memory::new(
                    p,
                    crate::domain::category::Category::Entities,
                    MemoryType::Insight,
                    &auth.tenant_id,
                );
                m.tags = vec!["imported".into(), "markdown".into()];
                m.source = Some("import-markdown".into());
                m.space_id = target_space.clone();
                if let Some(ref a) = agent_id {
                    m.owner_agent_id = a.clone();
                }
                let v = embed(&state, p).await;
                match store.create(&m, v.as_deref()).await {
                    Ok(()) => imported += 1,
                    Err(e) => errors.push(format!("{e}")),
                }
            }
        }
        "jsonl" => {
            let lines: Vec<&str> = content.lines().filter(|l| !l.trim().is_empty()).collect();
            total_items = lines.len();
            for line in &lines {
                if let Ok(obj) = serde_json::from_str::<serde_json::Value>(line) {
                    let c = obj.get("content").and_then(|v| v.as_str()).unwrap_or("");
                    if c.is_empty() {
                        skipped += 1;
                        continue;
                    }
                    let mut m = Memory::new(
                        c,
                        crate::domain::category::Category::Entities,
                        MemoryType::Insight,
                        &auth.tenant_id,
                    );
                    m.source = Some("import-jsonl".into());
                    m.space_id = target_space.clone();
                    if let Some(ref a) = agent_id {
                        m.owner_agent_id = a.clone();
                    }
                    let v = embed(&state, c).await;
                    match store.create(&m, v.as_deref()).await {
                        Ok(()) => imported += 1,
                        Err(e) => errors.push(format!("{e}")),
                    }
                } else {
                    skipped += 1;
                }
            }
        }
        _ => {
            return Err(OmemError::Validation(format!(
                "unsupported file_type: {file_type}. Use: memory, session, markdown, jsonl"
            )))
        }
    }

    let storage_status = if errors.is_empty() {
        "completed"
    } else if imported > 0 {
        "partial"
    } else {
        "failed"
    };

    let overall_status = if post_process && storage_status != "failed" {
        "processing"
    } else {
        storage_status
    };

    let task = ImportTaskRecord {
        id: task_id.clone(),
        status: overall_status.to_string(),
        file_type: file_type.clone(),
        filename: filename.clone(),
        agent_id: agent_id.clone(),
        space_id: target_space.clone(),
        post_process,
        storage_total: total_items,
        storage_stored: imported,
        storage_skipped: skipped,
        extraction_status: if post_process && storage_status != "failed" {
            "pending".to_string()
        } else {
            "skipped".to_string()
        },
        extraction_chunks: 0,
        extraction_facts: 0,
        extraction_progress: 0,
        reconcile_status: if post_process && storage_status != "failed" {
            "pending".to_string()
        } else {
            "skipped".to_string()
        },
        reconcile_relations: 0,
        reconcile_merged: 0,
        reconcile_progress: 0,
        errors,
        created_at: now,
        completed_at: if overall_status != "processing" {
            Some(chrono::Utc::now().to_rfc3339())
        } else {
            None
        },
    };

    state.space_store.create_import_task(&task).await?;

    if post_process && storage_status != "failed" {
        let bg_store = store;
        let bg_embed = state.embed.clone();
        let bg_llm = state.llm.clone();
        let bg_space_store = state.space_store.clone();
        let bg_task_id = task_id;
        let bg_tenant_id = auth.tenant_id.clone();

        tokio::spawn(async move {
            let intelligence = IntelligenceTask::new(
                bg_store,
                bg_embed,
                bg_llm,
                bg_space_store,
                bg_task_id.clone(),
                bg_tenant_id,
            );
            intelligence.run().await;
        });
    }

    Ok(Json(task))
}

pub async fn list_imports(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<AuthInfo>,
    Query(params): Query<ListImportsQuery>,
) -> Result<Json<serde_json::Value>, OmemError> {
    let space_id = personal_space_id(&auth.tenant_id);
    let tasks = state
        .space_store
        .list_import_tasks(&space_id, params.limit)
        .await
        .unwrap_or_default();

    Ok(Json(serde_json::json!({
        "imports": tasks,
        "total": tasks.len(),
    })))
}

pub async fn get_import(
    State(state): State<Arc<AppState>>,
    Extension(_auth): Extension<AuthInfo>,
    Path(id): Path<String>,
) -> Result<Json<ImportTaskRecord>, OmemError> {
    state
        .space_store
        .get_import_task(&id)
        .await?
        .map(Json)
        .ok_or_else(|| OmemError::NotFound(format!("import task {id}")))
}

pub async fn trigger_intelligence(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<AuthInfo>,
    Path(id): Path<String>,
) -> Result<Json<ImportTaskRecord>, OmemError> {
    let mut task = state
        .space_store
        .get_import_task(&id)
        .await?
        .ok_or_else(|| OmemError::NotFound(format!("import task {id}")))?;

    if task.status == "processing" {
        return Err(OmemError::Validation(
            "intelligence task already running".to_string(),
        ));
    }

    task.status = "processing".to_string();
    task.extraction_status = "pending".to_string();
    task.reconcile_status = "pending".to_string();
    task.completed_at = None;
    state.space_store.update_import_task(&task).await?;

    let store = state.store_manager.get_store(&task.space_id).await?;
    let bg_embed = state.embed.clone();
    let bg_llm = state.llm.clone();
    let bg_space_store = state.space_store.clone();
    let bg_task_id = task.id.clone();
    let bg_tenant_id = auth.tenant_id.clone();

    tokio::spawn(async move {
        let intelligence = IntelligenceTask::new(
            store,
            bg_embed,
            bg_llm,
            bg_space_store,
            bg_task_id.clone(),
            bg_tenant_id,
        );
        intelligence.run().await;
    });

    Ok(Json(task))
}

async fn embed(state: &AppState, text: &str) -> Option<Vec<f32>> {
    state
        .embed
        .embed(&[text.to_string()])
        .await
        .ok()
        .and_then(|v| v.into_iter().next())
}

fn parse_memory_json(content: &str) -> Result<Vec<serde_json::Value>, OmemError> {
    if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(content) {
        return Ok(arr);
    }
    if let Ok(obj) = serde_json::from_str::<serde_json::Value>(content) {
        if let Some(mems) = obj.get("memories").and_then(|m| m.as_array()) {
            return Ok(mems.clone());
        }
        return Ok(vec![obj]);
    }
    Err(OmemError::Validation(
        "expected JSON array or object with 'memories' field".into(),
    ))
}

fn parse_session(content: &str) -> Vec<(String, String)> {
    if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(content) {
        return arr
            .iter()
            .filter_map(|m| {
                Some((
                    m.get("role")?.as_str()?.into(),
                    m.get("content")?.as_str()?.into(),
                ))
            })
            .collect();
    }
    content
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|m| {
            Some((
                m.get("role")?.as_str()?.into(),
                m.get("content")?.as_str()?.into(),
            ))
        })
        .collect()
}
