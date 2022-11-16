// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use super::*;
use proptest::prelude::*;
use schemadb::{schema::fuzzing::assert_encode_decode, test_no_panic_decoding};
use aptos_types::move_core_types::trace::CallTrace;

proptest! {
    #[test]
    fn test_encode_decode(
        version in any::<Version>(),
        call_trace in any::<CallTrace>(),
    ) {
        assert_encode_decode::<CallTracesByVersionSchema>(&version, &vec![call_trace]);
    }
}

test_no_panic_decoding!(CallTracesByVersionSchema);
