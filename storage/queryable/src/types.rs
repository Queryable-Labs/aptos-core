use aptos_types::block_metadata::BlockMetadata;
use aptos_types::transaction::Version;
use serde::{Serialize, Deserialize};

pub const ENTITY_BLOCKS_NAME: &str = "blocks";
pub const ENTITY_TRANSACTIONS_NAME: &str = "transactions";
pub const ENTITY_EVENTS_NAME: &str = "events";
pub const ENTITY_CALL_TRACES_NAME: &str = "call_traces";

pub const ENTITY_FIELD_ID: &str = "_id";
pub const ENTITY_FIELD_RECORD_VERSION: &str = "record_version";
pub const ENTITY_FIELD_BLOCK_INDEX: &str = "block_index";
pub const ENTITY_FIELD_BLOCK_TX_INDEX: &str = "block_tx_index";
pub const ENTITY_FIELD_TIME_INDEX: &str = "time_index";
pub const ENTITY_FIELD_TX_INDEX: &str = "tx_index";
pub const ENTITY_FIELD_BLOCK_HASH: &str = "block_hash";
pub const ENTITY_FIELD_TX_HASH: &str = "tx_hash";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastExportData {
    pub last_known_tx_version: Version,
    pub last_block_metadata: BlockMetadata,
    pub last_block_tx_version: Version,
    pub last_known_block_index: u64,
}