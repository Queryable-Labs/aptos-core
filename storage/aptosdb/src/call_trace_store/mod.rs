// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

//! This file defines transaction store APIs that are related to committed signed transactions.

use crate::schema::call_traces_by_version::CallTracesByVersionSchema;
use anyhow::{ensure, format_err, Result};
use aptos_types::move_core_types::trace::CallTrace;
use aptos_types::transaction::Version;
use schemadb::{SchemaBatch, DB};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct CallTraceStore {
    db: Arc<DB>,
}

impl CallTraceStore {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db }
    }

    /// Save call trace at `version`
    pub fn put_call_trace(
        &self,
        version: Version,
        call_trace: &Vec<CallTrace>,
        batch: &mut SchemaBatch,
    ) -> Result<()> {
        batch.put::<CallTracesByVersionSchema>(&version, &call_trace)?;

        Ok(())
    }

    /// Prune the call traces schema store between a range of version in [begin, end)
    pub fn prune_call_traces(
        &self,
        begin: Version,
        end: Version,
        db_batch: &mut SchemaBatch,
    ) -> anyhow::Result<()> {
        for i in begin..end {
            db_batch.delete::<CallTracesByVersionSchema>(&i)?;
        }

        Ok(())
    }

    /// Get call traces in `[begin_version, end_version)` half-open range.
    ///
    /// N.b. an empty `Vec` is returned when `begin_version == end_version`
    pub fn get_call_traces(
        &self,
        start_version: Version,
        limit: u64,
        ledger_version: Version,
    ) -> Result<Vec<Vec<CallTrace>>> {
        if limit == 0 || start_version >= ledger_version {
            return Ok(Vec::new());
        }

        let mut iter = self
            .db
            .iter::<CallTracesByVersionSchema>(Default::default())?;
        iter.seek(&start_version)?;

        let limit = std::cmp::min(limit, ledger_version - start_version + 1);

        let mut ret = Vec::with_capacity(limit as usize);
        for current_version in start_version..start_version + limit {
            let (version, call_traces) = iter.next().transpose()?.ok_or_else(|| {
                format_err!("Call traces missing for version {}", current_version)
            })?;
            ensure!(
                version == current_version,
                "Call traces missing for version {}, got version {}",
                current_version,
                version,
            );
            ret.push(call_traces);
        }

        Ok(ret)
    }
}

#[cfg(test)]
mod test;
