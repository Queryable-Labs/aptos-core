// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::AptosDB;
use aptos_temppath::TempPath;
use move_deps::move_core_types::trace::CallType;

#[test]
fn test_put_get() {
    let tmp_dir = TempPath::new();
    let db = AptosDB::new_for_test(&tmp_dir);
    let store = &db.call_trace_store;

    let call_traces = vec![
        CallTrace {
            depth: 0,
            call_type: CallType::Call,
            module_id: None,
            function: String::from("test"),
            ty_args: vec![],
            args_types: vec![],
            args_values: vec![],
            gas_used: 0,
            err: None,
        }
    ];

    let mut batch = SchemaBatch::new();
    assert_eq!(
        store.put_call_trace(0, &call_traces, &mut batch).unwrap(),
        ()
    );

    db.commit(batch).unwrap();

    let fetched_call_traces = store.get_call_traces(0, 100).unwrap();

    assert_eq!(
        fetched_call_traces.len(),
        1
    );

    assert_eq!(
        fetched_call_traces.get(0).unwrap(),
        &call_traces
    );

    let mut batch = SchemaBatch::new();
    store.prune_call_traces(0, 100, &mut batch).unwrap();

    db.commit(batch).unwrap();

    let fetched_call_traces = store.get_call_traces(0, 100).unwrap();

    assert_eq!(
        fetched_call_traces.len(),
        0
    );
}