// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use crate::schema::{ensure_slice_len_eq, CALL_TRACES_BY_VERSION_CF_NAME};
use anyhow::Result;
use aptos_types::move_core_types::trace::CallTrace;
use aptos_types::transaction::Version;
use byteorder::{BigEndian, ReadBytesExt};
use schemadb::{
    define_schema,
    schema::{KeyCodec, ValueCodec},
};
use std::mem::size_of;

define_schema!(
    CallTracesByVersionSchema,
    Version,
    Vec<CallTrace>,
    CALL_TRACES_BY_VERSION_CF_NAME
);

impl KeyCodec<CallTracesByVersionSchema> for Version {
    fn encode_key(&self) -> Result<Vec<u8>> {
        Ok(self.to_be_bytes().to_vec())
    }

    fn decode_key(mut data: &[u8]) -> Result<Self> {
        ensure_slice_len_eq(data, size_of::<Version>())?;
        Ok(data.read_u64::<BigEndian>()?)
    }
}

impl ValueCodec<CallTracesByVersionSchema> for Vec<CallTrace> {
    fn encode_value(&self) -> Result<Vec<u8>> {
        bcs::to_bytes(self).map_err(Into::into)
    }

    fn decode_value(data: &[u8]) -> Result<Self> {
        bcs::from_bytes(data).map_err(Into::into)
    }
}

#[cfg(test)]
mod test;
