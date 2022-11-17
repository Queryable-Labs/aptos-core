use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use queryable_core::constant::PARQUET_METADATA_FIELD_NETWORK_ID;
use aptos_types::block_metadata::BlockMetadata;
use aptos_types::chain_id::ChainId;
use aptos_types::contract_event::ContractEvent;
use aptos_types::transaction::{Transaction, TransactionInfo, TransactionPayload, Version};
use aptos_types::write_set::WriteSet;
use aptos_logger::{prelude::*};
use queryable_core::datasource_writer::DatasourceWriter;
use queryable_core::types::entity::{Entity, EntityField, EntityFieldEncoding, EntityFieldType, EntityRelation, EntityRelationType};
use queryable_core::writer_context::WriterContext;
use aptos_crypto::HashValue;
use aptos_vm::data_cache::StorageAdapter;
use aptos_types::account_address::AccountAddress;
use storage_interface::state_view::DbStateView;
use move_deps::move_core_types::language_storage::TypeTag;
use move_deps::move_core_types::trace::{CallTrace, CallType};
use move_deps::move_core_types::transaction_argument::TransactionArgument;
use move_deps::move_resource_viewer::{AnnotatedMoveValue, MoveValueAnnotator};
use crate::types::{ENTITY_BLOCKS_NAME, ENTITY_TRANSACTIONS_NAME, ENTITY_EVENTS_NAME, ENTITY_CALL_TRACES_NAME, ENTITY_FIELD_BLOCK_INDEX, ENTITY_FIELD_TX_INDEX, ENTITY_FIELD_TIME_INDEX, ENTITY_FIELD_BLOCK_TX_INDEX, ENTITY_FIELD_RECORD_VERSION, ENTITY_FIELD_BLOCK_HASH, ENTITY_FIELD_TX_HASH, LastExportData, ENTITY_FIELD_ID};

#[derive(Debug)]
pub struct QueryableExporter {
    previous_block_metadata: Option<BlockMetadata>,
    current_block_metadata: BlockMetadata,
    current_block_tx_version: Version,
    current_block_index: u64,

    current_block_id: u64,
    current_transaction_ids: Vec<Option<u64>>,
    current_event_ids: Vec<Option<u64>>,
    current_call_trace_ids: Vec<Option<u64>>,

    last_tx_version: Version,

    last_successful_export_current_block_metadata: BlockMetadata,
    last_successful_export_current_block_tx_version: Version,
    last_successful_export_current_block_index: u64,
    last_successful_export_tx_version: Version,

    block_already_exported: bool,

    chain_id: Option<ChainId>,

    cached_blocks_count: u32,

    transaction_index: u32,
    block_size: u32,
    block_total_fee: u64,

    datasource_writer: Mutex<DatasourceWriter>,
}

impl QueryableExporter {
    pub fn new(
        config_file_path: PathBuf,
        chain_id: Option<ChainId>,
    ) -> anyhow::Result<Self> {
        let blocks_name = String::from(ENTITY_BLOCKS_NAME);
        let transactions_name = String::from(ENTITY_TRANSACTIONS_NAME);
        let events_name = String::from(ENTITY_EVENTS_NAME);
        let call_traces_name = String::from(ENTITY_CALL_TRACES_NAME);

        let datasource_writer = DatasourceWriter::new(
            WriterContext::read_from_file(config_file_path)?,
            String::from("Aptos"),
            String::from("Aptos Blockchain Datasource"),
            String::from("https://aptos.dev/img/aptos_word_dark.svg"),
            vec![
                Entity::new(
                    blocks_name.clone(),
                    vec![
                        EntityRelation {
                            local_field_name: String::from("relation_transactions"),
                            remote_entity_name: transactions_name.clone(),
                            remote_field_name: String::from(ENTITY_FIELD_ID),
                            relation_type: EntityRelationType::OneToMany,
                            eager_fetch: false,
                            nullable: false,
                        },
                        EntityRelation {
                            local_field_name: String::from("relation_events"),
                            remote_entity_name: events_name.clone(),
                            remote_field_name: String::from(ENTITY_FIELD_ID),
                            relation_type: EntityRelationType::OneToMany,
                            eager_fetch: false,
                            nullable: false,
                        },
                        EntityRelation {
                            local_field_name: String::from("relation_call_traces"),
                            remote_entity_name: call_traces_name.clone(),
                            remote_field_name: String::from(ENTITY_FIELD_ID),
                            relation_type: EntityRelationType::OneToMany,
                            eager_fetch: false,
                            nullable: false,
                        },
                    ],
                    vec![
                        EntityField::create_field(String::from(ENTITY_FIELD_ID), EntityFieldType::Uint64(true)),
                        EntityField::create_list_field(String::from("relation_transactions"), true, EntityFieldType::Uint64(false))?,
                        EntityField::create_list_field(String::from("relation_events"), true, EntityFieldType::Uint64(false))?,
                        EntityField::create_list_field(String::from("relation_call_traces"), true, EntityFieldType::Uint64(false))?,
                        EntityField::create_field(String::from("record_version"), EntityFieldType::Uint8(true)),
                        EntityField::create_field(String::from("block_index"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("time_index"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("block_tx_index"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("epoch"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("round"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("hash"), EntityFieldType::Binary(true)),
                        EntityField::create_field(String::from("parent_hash"), EntityFieldType::Binary(false)),
                        EntityField::create_field(String::from("proposer"), EntityFieldType::Binary(true)),
                        EntityField::create_field(String::from("previous_block_votes"), EntityFieldType::Binary(true)),
                        EntityField::create_field(String::from("time"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("size"), EntityFieldType::Uint32(true)),
                        EntityField::create_field(String::from("transactions_count"), EntityFieldType::Uint32(true)),
                        EntityField::create_field(String::from("total_minted"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("total_fee"), EntityFieldType::Uint64(true)),
                    ],
                    HashMap::from([
                        (String::from(ENTITY_FIELD_ID), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from(ENTITY_FIELD_RECORD_VERSION), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from(ENTITY_FIELD_BLOCK_INDEX), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from(ENTITY_FIELD_TIME_INDEX), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from(ENTITY_FIELD_BLOCK_TX_INDEX), EntityFieldEncoding::DeltaBinaryPacked),

                        (String::from("relation_transactions.list.item"), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from("relation_events.list.item"), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from("relation_call_traces.list.item"), EntityFieldEncoding::DeltaBinaryPacked),

                        (String::from("epoch"), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from("round"), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from("proposer"), EntityFieldEncoding::RLEDictionary),
                        (String::from("time"), EntityFieldEncoding::DeltaBinaryPacked),
                    ])
                )?,
                Entity::new(
                    transactions_name.clone(),
                    vec![
                        EntityRelation {
                            local_field_name: String::from("relation_block"),
                            remote_entity_name: blocks_name.clone(),
                            remote_field_name: String::from(ENTITY_FIELD_ID),
                            relation_type: EntityRelationType::ManyToOne,
                            eager_fetch: true,
                            nullable: false,
                        },
                        EntityRelation {
                            local_field_name: String::from("relation_events"),
                            remote_entity_name: events_name.clone(),
                            remote_field_name: String::from(ENTITY_FIELD_ID),
                            relation_type: EntityRelationType::OneToMany,
                            eager_fetch: false,
                            nullable: false,
                        },
                        EntityRelation {
                            local_field_name: String::from("relation_call_traces"),
                            remote_entity_name: call_traces_name.clone(),
                            remote_field_name: String::from(ENTITY_FIELD_ID),
                            relation_type: EntityRelationType::OneToMany,
                            eager_fetch: false,
                            nullable: false,
                        },
                    ],
                    vec![
                        EntityField::create_field(String::from(ENTITY_FIELD_ID), EntityFieldType::Uint64(true)),

                        EntityField::create_field(String::from("relation_block"), EntityFieldType::Uint64(true)),
                        EntityField::create_list_field(String::from("relation_events"), true, EntityFieldType::Uint64(false))?,
                        EntityField::create_list_field(String::from("relation_call_traces"), true, EntityFieldType::Uint64(false))?,

                        EntityField::create_field(String::from("record_version"), EntityFieldType::Uint8(true)),
                        EntityField::create_field(String::from("tx_index"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("block_index"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("time_index"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("block_tx_index"), EntityFieldType::Uint64(true)),

                        EntityField::create_field(String::from("block_hash"), EntityFieldType::Binary(true)),
                        EntityField::create_field(String::from("tx_hash"), EntityFieldType::Binary(true)),

                        EntityField::create_field(String::from("epoch"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("chain_id"), EntityFieldType::Uint8(true)),

                        EntityField::create_field(String::from("state_change_hash"), EntityFieldType::Binary(true)),
                        EntityField::create_field(String::from("event_root_hash"), EntityFieldType::Binary(true)),
                        EntityField::create_field(String::from("success"), EntityFieldType::Boolean(true)),
                        EntityField::create_field(String::from("detailed_status"), EntityFieldType::Binary(true)),
                        EntityField::create_field(String::from("tx_type"), EntityFieldType::Uint8(true)),

                        EntityField::create_field(String::from("sender"), EntityFieldType::Binary(false)),
                        EntityField::create_field(String::from("sequence_number"), EntityFieldType::Uint64(false)),

                        EntityField::create_field(String::from("gas_limit"), EntityFieldType::Uint64(false)),
                        EntityField::create_field(String::from("gas_price"), EntityFieldType::Uint64(false)),
                        EntityField::create_field(String::from("gas_used"), EntityFieldType::Uint64(true)),

                        EntityField::create_field(String::from("expiration_timestamp_secs"), EntityFieldType::Uint64(false)),

                        EntityField::create_field(String::from("payload_type"), EntityFieldType::Uint8(true)),
                        EntityField::create_field(String::from("payload_code"), EntityFieldType::Binary(false)),
                        EntityField::create_field(String::from("payload_module_address"), EntityFieldType::Binary(false)),
                        EntityField::create_field(String::from("payload_method_name"), EntityFieldType::Binary(false)),
                        EntityField::create_list_field(String::from("payload_ty_args"), false, EntityFieldType::Binary(false))?,
                        EntityField::create_list_field(String::from("payload_arg_types"), false, EntityFieldType::Binary(false))?,
                        EntityField::create_list_field(String::from("payload_arg_values"), false, EntityFieldType::Binary(false))?,

                        EntityField::create_field(String::from("signature"), EntityFieldType::Binary(false)),
                        EntityField::create_field(String::from("state_checkpoint"), EntityFieldType::Binary(false)),
                        EntityField::create_field(String::from("proposer"), EntityFieldType::Binary(false)),

                        EntityField::create_field(String::from("size"), EntityFieldType::Uint32(false)),
                        EntityField::create_field(String::from("time"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("fee"), EntityFieldType::Uint64(false)),
                    ],
                    HashMap::from([
                        (String::from(ENTITY_FIELD_ID), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from(ENTITY_FIELD_RECORD_VERSION), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from(ENTITY_FIELD_BLOCK_INDEX), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from(ENTITY_FIELD_TIME_INDEX), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from(ENTITY_FIELD_BLOCK_TX_INDEX), EntityFieldEncoding::DeltaBinaryPacked),

                        (String::from("relation_block"), EntityFieldEncoding::DeltaBinaryPacked),

                        (String::from("relation_events.list.item"), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from("relation_call_traces.list.item"), EntityFieldEncoding::DeltaBinaryPacked),

                        (String::from("block_hash"), EntityFieldEncoding::RLEDictionary),
                        (String::from("tx_hash"), EntityFieldEncoding::RLEDictionary),

                        (String::from("epoch"), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from("time"), EntityFieldEncoding::DeltaBinaryPacked),
                    ])
                )?,
                Entity::new(
                    events_name,
                    vec![
                        EntityRelation {
                            local_field_name: String::from("relation_block"),
                            remote_entity_name: blocks_name.clone(),
                            remote_field_name: String::from(ENTITY_FIELD_ID),
                            relation_type: EntityRelationType::ManyToOne,
                            eager_fetch: true,
                            nullable: false,
                        },
                        EntityRelation {
                            local_field_name: String::from("relation_transaction"),
                            remote_entity_name: transactions_name.clone(),
                            remote_field_name: String::from(ENTITY_FIELD_ID),
                            relation_type: EntityRelationType::ManyToOne,
                            eager_fetch: true,
                            nullable: false,
                        },
                    ],
                    vec![
                        EntityField::create_field(String::from(ENTITY_FIELD_ID), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("relation_block"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("relation_transaction"), EntityFieldType::Uint64(true)),

                        EntityField::create_field(String::from("record_version"), EntityFieldType::Uint8(true)),
                        EntityField::create_field(String::from("tx_index"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("block_index"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("time_index"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("block_tx_index"), EntityFieldType::Uint64(true)),

                        EntityField::create_field(String::from("block_hash"), EntityFieldType::Binary(true)),
                        EntityField::create_field(String::from("tx_hash"), EntityFieldType::Binary(true)),

                        EntityField::create_field(String::from("index"), EntityFieldType::Uint32(true)),
                        EntityField::create_field(String::from("module_address"), EntityFieldType::Binary(false)),
                        EntityField::create_field(String::from("event_name"), EntityFieldType::Binary(false)),
                        EntityField::create_list_field(String::from("payload_arg_types"), false, EntityFieldType::Binary(false))?,
                        EntityField::create_list_field(String::from("payload_arg_names"), false, EntityFieldType::Binary(false))?,
                        EntityField::create_list_field(String::from("payload_arg_values"), false, EntityFieldType::Binary(false))?,
                        EntityField::create_field(String::from("creation_number"), EntityFieldType::Uint64(true)),
                    ],
                    HashMap::from([
                        (String::from(ENTITY_FIELD_ID), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from(ENTITY_FIELD_RECORD_VERSION), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from(ENTITY_FIELD_BLOCK_INDEX), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from(ENTITY_FIELD_TIME_INDEX), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from(ENTITY_FIELD_BLOCK_TX_INDEX), EntityFieldEncoding::DeltaBinaryPacked),

                        (String::from("relation_block"), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from("relation_transaction"), EntityFieldEncoding::DeltaBinaryPacked),

                        (String::from("block_hash"), EntityFieldEncoding::RLEDictionary),
                        (String::from("tx_hash"), EntityFieldEncoding::RLEDictionary),

                        (String::from("module_address"), EntityFieldEncoding::RLEDictionary),
                        (String::from("event_name"), EntityFieldEncoding::RLEDictionary),
                        (String::from("time"), EntityFieldEncoding::DeltaBinaryPacked),
                    ]),
                )?,
                Entity::new(
                    call_traces_name,
                    vec![
                        EntityRelation {
                            local_field_name: String::from("relation_block"),
                            remote_entity_name: blocks_name.clone(),
                            remote_field_name: String::from(ENTITY_FIELD_ID),
                            relation_type: EntityRelationType::ManyToOne,
                            eager_fetch: true,
                            nullable: false,
                        },
                        EntityRelation {
                            local_field_name: String::from("relation_transaction"),
                            remote_entity_name: transactions_name.clone(),
                            remote_field_name: String::from(ENTITY_FIELD_ID),
                            relation_type: EntityRelationType::ManyToOne,
                            eager_fetch: true,
                            nullable: false,
                        },
                    ],
                    vec![
                        EntityField::create_field(String::from(ENTITY_FIELD_ID), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("relation_block"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("relation_transaction"), EntityFieldType::Uint64(true)),

                        EntityField::create_field(String::from("record_version"), EntityFieldType::Uint8(true)),
                        EntityField::create_field(String::from("tx_index"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("block_index"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("time_index"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("block_tx_index"), EntityFieldType::Uint64(true)),

                        EntityField::create_field(String::from("block_hash"), EntityFieldType::Binary(true)),
                        EntityField::create_field(String::from("tx_hash"), EntityFieldType::Binary(true)),

                        EntityField::create_field(String::from("tx_type"), EntityFieldType::Uint8(true)),
                        EntityField::create_field(String::from("depth"), EntityFieldType::Uint32(true)),
                        EntityField::create_field(String::from("call_type"), EntityFieldType::Uint8(true)),

                        EntityField::create_field(String::from("module_address"), EntityFieldType::Binary(false)),
                        EntityField::create_field(String::from("method_name"), EntityFieldType::Binary(true)),
                        EntityField::create_list_field(String::from("ty_args"), false, EntityFieldType::Binary(false))?,
                        EntityField::create_list_field(String::from("arg_types"), false, EntityFieldType::Binary(false))?,
                        EntityField::create_list_field(String::from("arg_values"), false, EntityFieldType::Binary(false))?,

                        EntityField::create_field(String::from("gas_used"), EntityFieldType::Uint64(true)),
                        EntityField::create_field(String::from("err"), EntityFieldType::Binary(false)),
                    ],
                    HashMap::from([
                        (String::from(ENTITY_FIELD_ID), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from(ENTITY_FIELD_RECORD_VERSION), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from(ENTITY_FIELD_BLOCK_INDEX), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from(ENTITY_FIELD_TIME_INDEX), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from(ENTITY_FIELD_BLOCK_TX_INDEX), EntityFieldEncoding::DeltaBinaryPacked),

                        (String::from("relation_block"), EntityFieldEncoding::DeltaBinaryPacked),
                        (String::from("relation_transaction"), EntityFieldEncoding::DeltaBinaryPacked),

                        (String::from("block_hash"), EntityFieldEncoding::RLEDictionary),
                        (String::from("tx_hash"), EntityFieldEncoding::RLEDictionary),

                        (String::from("module_address"), EntityFieldEncoding::PlainDictionary),
                    ]),
                )?
            ]
        )?;

        let last_exported_data = datasource_writer.get_last_exported_data();

        let last_block_metadata: BlockMetadata;
        let last_block_tx_version: Version;
        let last_tx_version: Version;
        let current_block_index: u64;

        if let Some(last_exported_data) = last_exported_data {
            let last_exported_data: LastExportData =
                serde_json::from_str(&last_exported_data)?;

            last_block_metadata = last_exported_data.last_block_metadata;
            last_block_tx_version = last_exported_data.last_block_tx_version;
            last_tx_version = last_exported_data.last_known_tx_version.clone();
            current_block_index = last_exported_data.last_known_block_index;
        } else {
            last_block_metadata = BlockMetadata::new(
                HashValue::zero(),
                0,
                0,
                AccountAddress::ZERO,
                vec![],
                vec![],
                0,
            );
            last_block_tx_version = 0;
            last_tx_version = 0;
            current_block_index = 0;
        }

        Ok(Self {
            previous_block_metadata: Some(last_block_metadata.clone()),

            current_block_index: current_block_index.clone(),
            current_block_metadata: last_block_metadata.clone(),
            current_block_tx_version: last_block_tx_version.clone(),

            current_block_id: current_block_index.clone(),
            current_transaction_ids: vec![],
            current_event_ids: vec![],
            current_call_trace_ids: vec![],

            last_tx_version,

            last_successful_export_current_block_metadata: last_block_metadata,
            last_successful_export_current_block_tx_version: last_block_tx_version.clone(),
            last_successful_export_current_block_index: current_block_index,
            last_successful_export_tx_version: last_tx_version.clone(),

            block_already_exported: false,

            chain_id,

            cached_blocks_count: 0,
            transaction_index: 0,
            block_size: 0,
            block_total_fee: 0,

            datasource_writer: Mutex::new(datasource_writer),
        })
    }

    pub fn set_chain_id(&mut self, chain_id: ChainId) {
        self.chain_id = Some(chain_id);
    }

    pub fn get_current_block(&self) -> BlockMetadata {
        return self.current_block_metadata.clone();
    }

    pub fn set_block_already_exported(&mut self) {
        self.block_already_exported = true;
    }

    pub fn add_current_block(
        &mut self,
        block_metadata: BlockMetadata,
        version: Version,
    ) -> anyhow::Result<()> {
        self.block_already_exported = false;

        self.previous_block_metadata = Some(self.current_block_metadata.clone());

        self.current_block_metadata = block_metadata;
        self.current_block_tx_version = version;
        self.current_block_index += 1;

        let mut datasource_writer = self.datasource_writer.lock().unwrap();

        self.current_block_id = datasource_writer.get_next_id(String::from(ENTITY_BLOCKS_NAME))?;
        self.current_transaction_ids = vec![];
        self.current_event_ids = vec![];
        self.current_call_trace_ids = vec![];

        self.transaction_index = 0;
        self.block_size = 0;
        self.block_total_fee = 0;

        self.cached_blocks_count += 1;

        Ok(())
    }

    pub fn get_cached_blocks_count(&self) -> u32 {
        return self.cached_blocks_count.clone();
    }

    pub fn get_blocks_per_export(&self) -> u32 {
        let datasource_writer = self.datasource_writer.lock().unwrap();

        datasource_writer.get_blocks_per_export()
    }

    pub fn complete_block(&mut self) -> anyhow::Result<()> {
        if self.block_already_exported {
            return Ok(());
        }

        let current_block = &self.current_block_metadata;
        let current_block_tx_version = &self.current_block_tx_version;
        let current_block_index = &self.current_block_index;

        let epoch = current_block.epoch() as u64;
        let round = current_block.round() as u64;
        let block_hash = current_block.id().to_vec();
        let block_time = current_block.timestamp_usecs() as u64;

        let mut datasource_writer = self.datasource_writer.lock().unwrap();

        datasource_writer.set_block_index(current_block_index.clone());
        datasource_writer.set_block_time(block_time.clone());

        let block_writer = datasource_writer.get_writer(
            String::from(ENTITY_BLOCKS_NAME)
        ).unwrap();

        block_writer.add_value_u64(String::from(ENTITY_FIELD_ID), Some(self.current_block_id.clone()))?;

        block_writer.add_list_value_u64(String::from("relation_transactions"), Some(self.current_transaction_ids.clone()))?;
        block_writer.add_list_value_u64(String::from("relation_events"), Some(self.current_event_ids.clone()))?;
        block_writer.add_list_value_u64(String::from("relation_call_traces"), Some(self.current_call_trace_ids.clone()))?;

        block_writer.add_value_u8(String::from(ENTITY_FIELD_RECORD_VERSION), Some(1))?;

        // incremented block_index
        block_writer.add_value_u64(String::from(ENTITY_FIELD_BLOCK_INDEX), Some(current_block_index.clone() as u64))?;
        block_writer.add_value_u64(String::from(ENTITY_FIELD_TIME_INDEX), Some(block_time.clone() as u64))?;

        block_writer.add_value_u64(String::from(ENTITY_FIELD_BLOCK_TX_INDEX), Some(current_block_tx_version.clone() as u64))?;

        block_writer.add_value_u64(String::from("epoch"), Some(epoch.clone() as u64))?;
        block_writer.add_value_u64(String::from("round"), Some(round.clone() as u64))?;

        block_writer.add_value_binary(String::from("hash"), Some(block_hash.clone()))?;
        block_writer.add_value_binary(
            String::from("parent_hash"),
            if self.previous_block_metadata.is_some() {
                Some(
                    self.previous_block_metadata.as_ref().unwrap().id().to_vec()
                )
            } else {
                None
            }
        )?;

        block_writer.add_value_binary(String::from("proposer"), Some(current_block.proposer().to_vec()))?;
        block_writer.add_value_binary(
            String::from("previous_block_votes"),
            Some(
                current_block.previous_block_votes_bitvec().clone()
            )
        )?;

        block_writer.add_value_u64(String::from("time"), Some(block_time.clone() as u64))?;
        block_writer.add_value_u32(String::from("size"), Some(self.block_size.clone()))?;

        block_writer.add_value_u32(String::from("transactions_count"), Some(self.transaction_index.clone()))?;

        block_writer.add_value_u64(String::from("total_fee"), Some(self.block_total_fee.clone()))?;
        block_writer.add_value_u64(String::from("total_minted"), Some(0))?;

        Ok(())
    }

    pub fn add_transaction(
        &mut self,
        move_value_annotator: &MoveValueAnnotator<StorageAdapter<DbStateView>>,
        version: Version,
        transaction: &Transaction,
        transaction_info: &TransactionInfo,
        _write_set: &WriteSet,
        events: &[ContractEvent],
        call_traces: &Vec<CallTrace>,
    ) -> anyhow::Result<()> {
        self.transaction_index += 1;

        let mut event_ids: Vec<Option<u64>> = vec![];
        let mut call_trace_ids: Vec<Option<u64>> = vec![];

        let block_metadata = &self.current_block_metadata;

        let epoch = block_metadata.epoch() as u64;
        let block_hash = block_metadata.id().to_vec();

        let block_index = self.current_block_index.clone();
        let block_tx_index = self.current_block_tx_version.clone();
        let block_time = block_metadata.timestamp_usecs() as u64;

        let transaction = transaction;
        let transaction_info = match transaction_info {
            TransactionInfo::V0(transaction_info_v0) => transaction_info_v0
        };

        let tx_hash = transaction_info.transaction_hash().to_vec();

        let mut datasource_writer = self.datasource_writer.lock().unwrap();

        let transaction_id = datasource_writer.get_next_id(
            String::from(ENTITY_TRANSACTIONS_NAME)
        )?;

        let transaction_writer = datasource_writer.get_writer(
            String::from(ENTITY_TRANSACTIONS_NAME)
        ).unwrap();

        self.current_transaction_ids.push(Some(transaction_id.clone()));

        transaction_writer.add_value_u64(String::from(ENTITY_FIELD_ID), Some(transaction_id))?;
        transaction_writer.add_value_u8(String::from(ENTITY_FIELD_RECORD_VERSION), Some(1))?;

        transaction_writer.add_value_u64(String::from("relation_block"), Some(self.current_block_index.clone()))?;

        transaction_writer.add_value_u64(String::from(ENTITY_FIELD_TX_INDEX), Some(version.clone() as u64))?;
        transaction_writer.add_value_u64(String::from(ENTITY_FIELD_BLOCK_INDEX), Some(block_index.clone() as u64))?;
        transaction_writer.add_value_u64(String::from(ENTITY_FIELD_BLOCK_TX_INDEX), Some(block_tx_index.clone() as u64))?;
        transaction_writer.add_value_u64(String::from(ENTITY_FIELD_TIME_INDEX), Some(block_time.clone() as u64))?;

        transaction_writer.add_value_binary(String::from(ENTITY_FIELD_BLOCK_HASH), Some(block_hash.to_vec()))?;
        transaction_writer.add_value_binary(String::from(ENTITY_FIELD_TX_HASH), Some(tx_hash.to_vec()))?;

        transaction_writer.add_value_u64(String::from("epoch"), Some(epoch.clone() as u64))?;

        transaction_writer.add_value_u8(String::from("chain_id"), Some(
            if self.chain_id.is_some() {
                self.chain_id.unwrap().id()
            } else {
                0
            }
        ))?;
        transaction_writer.add_value_binary(String::from("state_change_hash"), Some(
            transaction_info.state_change_hash().to_vec()
        ))?;
        transaction_writer.add_value_binary(String::from("event_root_hash"), Some(
            transaction_info.event_root_hash().to_vec()
        ))?;
        transaction_writer.add_value_bool(String::from("success"), Some(
            transaction_info.status().is_success()
        ))?;
        transaction_writer.add_value_binary(String::from("detailed_status"), Some(
            serde_json::to_vec(transaction_info.status())?
        ))?;

        let mut sequence_number: Option<u64> = None;
        let mut sender: Option<Vec<u8>> = None;
        let mut expiration_timestamp: Option<u64> = None;
        let gas_used = transaction_info.gas_used() as u64;
        let mut max_gas: Option<u64> = None;
        let mut gas_unit_price: Option<u64> = None;
        let mut tx_fee: Option<u64> = None;
        let mut tx_size: Option<u32> = None;

        let mut payload_type: u8 = 0;
        let mut payload_code: Option<Vec<u8>> = None;
        let mut payload_module_address: Option<Vec<u8>> = None;
        let mut payload_method_name: Option<Vec<u8>> = None;

        let mut payload_ty_args: Option<Vec<Option<Vec<u8>>>> = None;
        let mut payload_arg_types: Option<Vec<Option<Vec<u8>>>> = None;
        let mut payload_arg_values: Option<Vec<Option<Vec<u8>>>> = None;

        let mut signature: Option<Vec<u8>> = None;
        // @TODO: implement
        let accumulator_root_hash: Vec<u8> = vec![];
        // transaction_info.state_checkpoint_hash().to_string();

        let mut proposer: Option<Vec<u8>> = None;
        let mut state_checkpoint: Option<Vec<u8>> = None;

        let tx_type;

        match transaction {
            Transaction::UserTransaction(tx) => {
                tx_type = 1;
                sequence_number = Some(tx.sequence_number() as u64);
                sender = Some(tx.sender().to_vec());

                let mut payload_arg_types_vec: Vec<Option<Vec<u8>>> = vec![];
                let mut payload_arg_values_vec: Vec<Option<Vec<u8>>> = vec![];

                match tx.payload() {
                    // A transaction that executes code.
                    TransactionPayload::Script(script) => {
                        payload_type = 1;
                        payload_code = Some(
                            Vec::from(script.code().clone())
                        );

                        payload_ty_args = Some(
                            script.ty_args().clone()
                                .into_iter()
                                .map(
                                    |type_layouty|
                                        Some(type_layouty.to_string().into_bytes())
                                )
                                .collect()
                        );

                        script.args().clone()
                            .iter()
                            .for_each(|arg| {
                                match arg {
                                    TransactionArgument::U8(value) => {
                                        payload_arg_types_vec.push(Some(Vec::from("u8")));
                                        payload_arg_values_vec.push(
                                            Some(Vec::from([value.clone()]))
                                        );
                                    },
                                    TransactionArgument::U64(value) => {
                                        payload_arg_types_vec.push(Some(Vec::from("u64")));
                                        payload_arg_values_vec.push(Some(
                                            Vec::from(value.to_be_bytes())
                                        ));
                                    },
                                    TransactionArgument::U128(value) => {
                                        payload_arg_types_vec.push(Some(Vec::from("u128")));
                                        payload_arg_values_vec.push(Some(
                                            Vec::from(value.to_be_bytes())
                                        ));
                                    },
                                    TransactionArgument::Address(value) => {
                                        payload_arg_types_vec.push(Some(Vec::from("address")));
                                        payload_arg_values_vec.push(Some(
                                            Vec::from(value.to_vec())
                                        ));
                                    },
                                    TransactionArgument::U8Vector(value) => {
                                        payload_arg_types_vec.push(Some(Vec::from("vec_u8")));
                                        payload_arg_values_vec.push(Some(
                                            Vec::from(value.clone())
                                        ));
                                    },
                                    TransactionArgument::Bool(value) => {
                                        payload_arg_types_vec.push(Some(Vec::from("bool")));
                                        payload_arg_values_vec.push(Some(
                                            Vec::from(
                                                Vec::from(
                                                    if value.clone() == true {
                                                        [1]
                                                    } else {
                                                        [0]
                                                    }
                                                )
                                            )
                                        ));
                                    }
                                }
                            });

                        payload_arg_types = Some(payload_arg_types_vec);
                        payload_arg_values = Some(payload_arg_values_vec);
                    }
                    // A transaction that publishes multiple modules at the same time.
                    TransactionPayload::ModuleBundle(module_bundle) => {
                        payload_type = 2;
                        // payload_code = Some(ByteArray::from(moduleBundle.codes().code()));

                        // @TODO: define how module id is generated!????!!!!
                    },
                    // A transaction that executes an existing entry function published on-chain.
                    TransactionPayload::EntryFunction(entry_function) => {
                        payload_type = 3;
                        payload_module_address = Some(Vec::from(
                            entry_function.module().address().to_vec()
                        ));
                        payload_method_name = Some(Vec::from(
                            Vec::from(entry_function.function().as_bytes())
                        ));

                        payload_ty_args = Some(
                            entry_function.ty_args().clone()
                                .into_iter()
                                .map(
                                    |arg_type|
                                        Some(Vec::from(arg_type.to_string().into_bytes()))
                                )
                                .collect()
                        );

                        payload_arg_values = Some(
                            entry_function.args().clone()
                                .into_iter()
                                .map(|value| Some(value.clone()))
                                .collect()
                        );
                    }
                }

                expiration_timestamp = Some(tx.expiration_timestamp_secs() as u64);
                max_gas = Some(tx.max_gas_amount() as u64);
                gas_unit_price = Some(tx.gas_unit_price() as u64);
                signature = Some(Vec::from(
                    serde_json::to_vec(
                        &tx.authenticator()
                    )?
                ));

                let tx_fee_variant = tx.gas_unit_price() as u64 * gas_used as u64;

                tx_fee = Some(tx_fee_variant.clone());

                let tx_size_variant = tx.raw_txn_bytes_len() as u32;

                tx_size = Some(tx_size_variant.clone());

                self.block_size += tx_size_variant.clone() as u32;
                self.block_total_fee += tx_fee_variant as u64;
            },
            Transaction::GenesisTransaction(_tx) => {
                tx_type = 2;
            },
            Transaction::BlockMetadata(tx) => {
                tx_type = 3;
                proposer = Some(Vec::from(tx.proposer().to_vec()));
            },
            Transaction::StateCheckpoint(tx) => {
                tx_type = 4;
                state_checkpoint = Some(Vec::from(tx.to_vec()));
            },
        }

        transaction_writer.add_value_u8(String::from("tx_type"), Some(tx_type))?;

        // UserTransaction
        transaction_writer.add_value_binary(String::from("sender"), sender)?;
        transaction_writer.add_value_u64(String::from("sequence_number"), sequence_number)?;
        transaction_writer.add_value_u64(String::from("gas_limit"), max_gas)?;
        transaction_writer.add_value_u64(String::from("gas_price"), gas_unit_price)?;
        transaction_writer.add_value_u64(String::from("gas_used"), Some(gas_used.clone()))?;
        transaction_writer.add_value_u64(String::from("expiration_timestamp_secs"), expiration_timestamp)?;
        transaction_writer.add_value_u8(String::from("payload_type"), Some(payload_type))?;
        transaction_writer.add_value_binary(String::from("payload_code"), payload_code)?;
        transaction_writer.add_value_binary(String::from("payload_module_address"), payload_module_address)?;
        transaction_writer.add_value_binary(String::from("payload_method_name"), payload_method_name)?;
        transaction_writer.add_list_value_binary(String::from("payload_ty_args"), payload_ty_args)?;
        transaction_writer.add_list_value_binary(String::from("payload_arg_types"), payload_arg_types)?;
        transaction_writer.add_list_value_binary(String::from("payload_arg_values"), payload_arg_values)?;

        transaction_writer.add_value_binary(String::from("signature"), signature)?;

        // state
        transaction_writer.add_value_binary(String::from("state_checkpoint"), state_checkpoint)?;

        // block metadata
        transaction_writer.add_value_binary(String::from("proposer"), proposer)?;

        transaction_writer.add_value_u32(String::from("size"), tx_size)?;
        transaction_writer.add_value_u64(String::from("time"), Some(block_time.clone()))?;
        transaction_writer.add_value_u64(String::from("fee"), tx_fee)?;

        ////////////////
        ////////////////
        //////////////// EVENTS
        ////////////////
        ////////////////

        for j in 0..events.len() {
            let tx_event_id = j.clone() as u32;
            let event = &events[j.clone()];

            let event_id = datasource_writer.get_next_id(
                String::from(ENTITY_EVENTS_NAME)
            )?;

            event_ids.push(Some(event_id.clone()));

            let event_writer = datasource_writer.get_writer(
                String::from(ENTITY_EVENTS_NAME)
            ).unwrap();

            event_writer.add_value_u64(String::from(ENTITY_FIELD_ID), Some(event_id))?;

            event_writer.add_value_u64(String::from("relation_block"), Some(self.current_block_index.clone()))?;
            event_writer.add_value_u64(String::from("relation_transaction"), Some(transaction_id.clone()))?;

            event_writer.add_value_u8(String::from(ENTITY_FIELD_RECORD_VERSION), Some(1))?;

            event_writer.add_value_u64(String::from(ENTITY_FIELD_TX_INDEX), Some(version.clone() as u64))?;
            event_writer.add_value_u64(String::from(ENTITY_FIELD_BLOCK_INDEX), Some(block_index.clone() as u64))?;
            event_writer.add_value_u64(String::from(ENTITY_FIELD_BLOCK_TX_INDEX), Some(block_tx_index.clone() as u64))?;
            event_writer.add_value_u64(String::from(ENTITY_FIELD_TIME_INDEX), Some(block_time.clone() as u64))?;

            event_writer.add_value_binary(String::from(ENTITY_FIELD_BLOCK_HASH), Some(block_hash.to_vec()))?;
            event_writer.add_value_binary(String::from(ENTITY_FIELD_TX_HASH), Some(tx_hash.to_vec()))?;

            event_writer.add_value_u32(String::from("index"), Some(tx_event_id.clone()))?;

            match event {
                ContractEvent::V0(contract_event) => {
                    let event_key = contract_event.key();

                    let type_tag = contract_event.type_tag();

                    let (module_id, event_name) = match type_tag {
                        TypeTag::Struct(type_struct_tag) => {
                            (
                                Some(
                                    type_struct_tag.module_id().short_str_lossless().into_bytes()
                                ),
                                Some(
                                    type_struct_tag.name.clone().into_bytes()
                                )
                            )
                        },
                        _ => {
                            warn!("Event type_tag is not struct");
                            (None, None)
                        }
                    };

                    event_writer.add_value_binary(
                        String::from("module_address"),
                        module_id
                    )?;

                    event_writer.add_value_binary(
                        String::from("event_name"),
                        event_name
                    )?;

                    let event_data = move_value_annotator
                        .view_value(event.type_tag(), event.event_data())?;

                    let mut event_payload_types: Vec<Option<Vec<u8>>> = vec![];
                    let mut event_payload_names: Vec<Option<Vec<u8>>> = vec![];
                    let mut event_payload_values: Vec<Option<Vec<u8>>> = vec![];

                    match event_data {
                        AnnotatedMoveValue::U8(value) => {
                            event_payload_types.push(Some(Vec::from("u8")));
                            event_payload_values.push(Some(Vec::from(value.to_be_bytes())));
                        },
                        AnnotatedMoveValue::U64(value) => {
                            event_payload_types.push(Some(Vec::from("u64")));
                            event_payload_values.push(Some(Vec::from(value.to_be_bytes())));
                        },
                        AnnotatedMoveValue::U128(value) => {
                            event_payload_types.push(Some(Vec::from("u128")));
                            event_payload_values.push(Some(Vec::from(value.to_be_bytes())));
                        },
                        AnnotatedMoveValue::Bool(value) => {
                            event_payload_types.push(Some(Vec::from("bool")));
                            event_payload_values.push(Some(
                                Vec::from(
                                    if value.clone() == true {
                                        [1]
                                    } else {
                                        [0]
                                    }
                                )
                            ));
                        },
                        AnnotatedMoveValue::Address(value) => {
                            event_payload_types.push(Some(Vec::from("address")));
                            event_payload_values.push(Some(
                                Vec::from(value.to_vec())
                            ));
                        },
                        AnnotatedMoveValue::Vector(type_tag, values) => {
                            event_payload_types.push(Some(type_tag.to_string().into_bytes()));

                            // @TODO: convert values
                            event_payload_values.push(
                                Some(
                                    serde_json::to_vec(&values)?
                                )
                            );
                        },
                        AnnotatedMoveValue::Bytes(value) => {
                            event_payload_types.push(Some(Vec::from("vec_u8")));
                            event_payload_values.push(Some(
                                Vec::from(value.clone())
                            ));
                        },
                        AnnotatedMoveValue::Struct(value) => {
                            for index in 0..value.value.len() {
                                event_payload_types.push(
                                    Some(
                                        serde_json::to_vec(&value.value[index.clone()].1.get_type())?
                                    )
                                );

                                event_payload_names.push(
                                    Some(
                                        value.value[index.clone()].0.clone().into_bytes()
                                    )
                                );

                                event_payload_values.push(
                                    Some(
                                        serde_json::to_vec(&value.value[index].1)?
                                    )
                                );
                            }
                        },
                    }

                    event_writer.add_list_value_binary(
                        String::from("payload_arg_types"),
                        Some(event_payload_types)
                    )?;

                    event_writer.add_list_value_binary(
                        String::from("payload_arg_names"),
                        Some(event_payload_names)
                    )?;

                    event_writer.add_list_value_binary(
                        String::from("payload_arg_values"),
                        Some(event_payload_values)
                    )?;

                    event_writer.add_value_u64(
                        String::from("creation_number"),
                        Some(event_key.get_creation_number().clone())
                    )?;
                }
            }
        }

        ////////////////
        ////////////////
        //////////////// CALL TRACES
        ////////////////
        ////////////////

        for call_trace in call_traces {
            let call_trace_id = datasource_writer.get_next_id(
                String::from(ENTITY_CALL_TRACES_NAME)
            )?;

            call_trace_ids.push(Some(call_trace_id.clone()));

            let call_trace_writer = datasource_writer.get_writer(
                String::from(ENTITY_CALL_TRACES_NAME)
            ).unwrap();

            call_trace_writer.add_value_u64(String::from(ENTITY_FIELD_ID), Some(call_trace_id))?;

            call_trace_writer.add_value_u64(String::from("relation_block"), Some(self.current_block_index.clone()))?;
            call_trace_writer.add_value_u64(String::from("relation_transaction"), Some(transaction_id.clone()))?;

            call_trace_writer.add_value_u8(String::from(ENTITY_FIELD_RECORD_VERSION), Some(1))?;

            call_trace_writer.add_value_u64(String::from(ENTITY_FIELD_TX_INDEX), Some(version.clone() as u64))?;
            call_trace_writer.add_value_u64(String::from(ENTITY_FIELD_BLOCK_INDEX), Some(block_index.clone() as u64))?;
            call_trace_writer.add_value_u64(String::from(ENTITY_FIELD_BLOCK_TX_INDEX), Some(block_tx_index.clone() as u64))?;
            call_trace_writer.add_value_u64(String::from(ENTITY_FIELD_TIME_INDEX), Some(block_time.clone() as u64))?;

            call_trace_writer.add_value_binary(String::from(ENTITY_FIELD_BLOCK_HASH), Some(block_hash.to_vec()))?;
            call_trace_writer.add_value_binary(String::from(ENTITY_FIELD_TX_HASH), Some(tx_hash.to_vec()))?;

            call_trace_writer.add_value_u8(String::from("tx_type"), Some(tx_type.clone()))?;

            call_trace_writer.add_value_u32(String::from("depth"), Some(call_trace.depth.clone() as u32))?;
            call_trace_writer.add_value_u8(String::from("call_type"), Some(
                match call_trace.call_type {
                    CallType::Call => 1,
                    CallType::CallGeneric => 2,
                }
            ))?;

            call_trace_writer.add_value_binary(String::from("module_address"),
                if call_trace.module_id.is_some() {
                    Some(
                        Vec::from(
                            call_trace.module_id.clone().unwrap().as_bytes()
                        )
                    )
                } else {
                    None
                }
            )?;
            call_trace_writer.add_value_binary(
                String::from("method_name"),
                Some(call_trace.function.clone().into_bytes())
            )?;
            call_trace_writer.add_list_value_binary(
                String::from("ty_args"),
                Some(
                    call_trace.ty_args
                        .iter()
                        .map(|value| Some(value.clone()))
                        .collect()
                )
            )?;
            call_trace_writer.add_list_value_binary(
                String::from("arg_types"),
                Some(
                    call_trace.args_types
                        .iter()
                        .map(|value| Some(value.clone()))
                        .collect()
                )
            )?;
            call_trace_writer.add_list_value_binary(
                String::from("arg_values"),
                Some(
                    call_trace.args_values
                        .iter()
                        .map(|value| Some(value.clone()))
                        .collect()
                )
            )?;

            let gas_used: u64 = call_trace.gas_used.into();

            call_trace_writer.add_value_u64(
                String::from("gas_used"),
                Some(gas_used)
            )?;

            call_trace_writer.add_value_binary(
                String::from("err"),
                if call_trace.err.is_some() {
                    Some(
                        serde_json::to_vec(&call_trace.err.as_ref().unwrap())?
                    )
                } else {
                    None
                }
            )?;
        }

        let transaction_writer = datasource_writer.get_writer(
            String::from(ENTITY_TRANSACTIONS_NAME)
        ).unwrap();

        transaction_writer.add_list_value_u64(String::from("relation_events"), Some(event_ids.clone()))?;
        transaction_writer.add_list_value_u64(String::from("relation_call_traces"), Some(call_trace_ids.clone()))?;

        self.current_transaction_ids.push(Some(version.clone() as u64));
        self.current_event_ids.extend(event_ids.clone());
        self.current_call_trace_ids.extend(call_trace_ids.clone());

        self.last_tx_version = version;

        Ok(())
    }

    pub fn export(&mut self) -> anyhow::Result<()> {
        // Cleaning

        self.cached_blocks_count = 0;

        self.transaction_index = 0;
        self.block_size = 0;
        self.block_total_fee = 0;

        // Exporting
        let exported_data_state = LastExportData{
            last_block_metadata: self.current_block_metadata.clone(),
            last_block_tx_version: self.current_block_tx_version.clone(),
            last_known_tx_version: self.last_tx_version.clone(),
            last_known_block_index: self.current_block_index.clone(),
        };

        let mut datasource_writer = self.datasource_writer.lock().unwrap();

        let exported_state = serde_json::to_string(&exported_data_state)?;

        info!("Exporting Queryable data stream");
        let result = datasource_writer.export(
            Some(exported_state),
            HashMap::from(
                [
                    (
                        String::from(PARQUET_METADATA_FIELD_NETWORK_ID),
                        self.chain_id.unwrap_or(ChainId::test()).to_string()
                    )
                ]
            )
        );

        match result {
            Err(err) => {
                error!("Failed to export, error: {}", err);

                return Err(err);
            },
            _ => {}
        }

        self.last_successful_export_tx_version = self.last_tx_version;

        info!("Exported Queryable data stream");

        Ok(())
    }

    pub fn last_tx_version(&self) -> Version {
        self.last_tx_version
    }

    pub fn reset_cached_data(&mut self) -> anyhow::Result<()> {
        self.current_block_metadata = self.last_successful_export_current_block_metadata.clone();
        self.current_block_tx_version = self.last_successful_export_current_block_tx_version.clone();
        self.current_block_index = self.last_successful_export_current_block_index.clone();

        self.last_tx_version = self.last_successful_export_tx_version.clone();

        self.transaction_index = 0;

        self.block_already_exported = true;

        self.cached_blocks_count = 0;

        self.transaction_index = 0;
        self.block_size = 0;
        self.block_total_fee = 0;

        let mut datasource_writer = self.datasource_writer.lock().unwrap();

        datasource_writer.clean();

        Ok(())
    }
}