// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use crate::common::{Payload, PayloadFilter, Round};
use anyhow::Result;
use futures::channel::oneshot;
use std::{fmt, fmt::Formatter};
use aptos_crypto::HashValue;
use crate::proof_of_store::LogicalTime;

/// Message sent from Consensus to QuorumStore.
pub enum WrapperCommand {
    /// Request to pull block to submit to consensus.
    GetBlockRequest(
        Round,
        // max block size
        u64,
        // max byte size
        u64,
        // block payloads to exclude from the requested block
        PayloadFilter,
        // callback to respond to
        oneshot::Sender<Result<ConsensusResponse>>,
    ),
    /// Request to clean quorum store at commit logical time
    CleanRequest(LogicalTime, Vec<HashValue>),
}

impl fmt::Display for WrapperCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            WrapperCommand::GetBlockRequest(round, max_txns, max_bytes, excluded, _) => {
                write!(
                    f,
                    "GetBlockRequest [round: {}, max_txns: {}, max_bytes: {} excluded: {}]",
                    round, max_txns, max_bytes, excluded
                )
            }
            WrapperCommand::CleanRequest(logical_time, digests) => {
                write!(
                    f,
                    "CleanRequest [epoch: {}, round: {}, digests: {:?}]",
                    logical_time.epoch(),
                    logical_time.round(),
                    digests
                )
            }
        }
    }
}

pub enum ConsensusResponse {
    GetBlockResponse(Payload),
    CleanResponse(),
}
