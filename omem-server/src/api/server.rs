use std::sync::Arc;

use crate::config::OmemConfig;
use crate::embed::EmbedService;
use crate::llm::LlmService;
use crate::store::{SpaceStore, StoreManager, TenantStore};

pub struct AppState {
    pub store_manager: Arc<StoreManager>,
    pub tenant_store: Arc<TenantStore>,
    pub space_store: Arc<SpaceStore>,
    pub embed: Arc<dyn EmbedService>,
    pub llm: Arc<dyn LlmService>,
    pub config: OmemConfig,
}
