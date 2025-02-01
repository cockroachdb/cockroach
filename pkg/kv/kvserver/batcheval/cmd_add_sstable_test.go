// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval_test

import (
	"bytes"
	"context"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestEvalAddSSTable tests EvalAddSSTable directly, using only an in-memory
// Pebble engine. This allows precise manipulation of timestamps.
func TestEvalAddSSTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	storage.DisableMetamorphicSimpleValueEncoding(t)

	const intentTS = 100 // values with this timestamp are written as intents

	// These are run with IngestAsWrites both disabled and enabled, and
	// kv.bulk_io_write.sst_rewrite_concurrency.per_call of 0 and 8.
	testcases := map[string]struct {
		data           kvs
		sst            kvs
		reqTS          int64
		toReqTS        int64 // SSTTimestampToRequestTimestamp with given SST timestamp
		noConflict     bool  // DisallowConflicts
		noShadowBelow  int64 // DisallowShadowingBelow
		requireReqTS   bool  // AddSSTableRequireAtRequestTimestamp
		expect         kvs
		ignoreExpect   bool
		expectErr      interface{} // error type, substring, substring slice, or true (any)
		expectErrRace  interface{}
		expectStatsEst bool // expect MVCCStats.ContainsEstimates, don't check stats
	}{
		// Blind writes.
		"blind writes below existing": {
			data: kvs{pointKV("a", 5, "a5"), pointKV("b", 7, ""), pointKV("c", 6, "c6")},
			sst:  kvs{pointKV("a", 3, "sst"), pointKV("b", 2, "sst"), pointKV("c", 3, "")},
			expect: kvs{
				pointKV("a", 5, "a5"), pointKV("a", 3, "sst"), pointKV("b", 7, ""), pointKV("b", 2, "sst"), pointKV("c", 6, "c6"), pointKV("c", 3, ""),
			},
			expectStatsEst: true,
		},
		"blind replaces existing": {
			data:           kvs{pointKV("a", 2, "a2"), pointKV("b", 2, "b2")},
			sst:            kvs{pointKV("a", 2, "sst"), pointKV("b", 2, "")},
			expect:         kvs{pointKV("a", 2, "sst"), pointKV("b", 2, "")},
			expectStatsEst: true,
		},
		"blind errors on AddSSTableRequireAtRequestTimestamp": {
			data:         kvs{pointKV("a", 5, "a5"), pointKV("b", 7, "")},
			sst:          kvs{pointKV("a", 3, "sst"), pointKV("b", 2, "sst")},
			requireReqTS: true,
			expectErr:    "AddSSTable requests must set SSTTimestampToRequestTimestamp",
		},
		"blind returns LockConflictError on conflict": {
			data:      kvs{pointKV("b", intentTS, "b0")},
			sst:       kvs{pointKV("b", 1, "sst")},
			expectErr: &kvpb.LockConflictError{},
		},
		"blind returns LockConflictError in span": {
			data:      kvs{pointKV("b", intentTS, "b0")},
			sst:       kvs{pointKV("a", 1, "sst"), pointKV("c", 1, "sst")},
			expectErr: &kvpb.LockConflictError{},
		},
		"blind ignores intent outside span": {
			data:           kvs{pointKV("b", intentTS, "b0")},
			sst:            kvs{pointKV("c", 1, "sst"), pointKV("d", 1, "sst")},
			expect:         kvs{pointKV("b", intentTS, "b0"), pointKV("c", 1, "sst"), pointKV("d", 1, "sst")},
			expectStatsEst: true,
		},
		"blind writes tombstones": {
			sst:            kvs{pointKV("a", 1, "")},
			expect:         kvs{pointKV("a", 1, "")},
			expectStatsEst: true,
		},
		"blind writes range tombstones": {
			sst:            kvs{rangeKV("a", "d", 1, "")},
			expect:         kvs{rangeKV("a", "d", 1, "")},
			expectStatsEst: true,
		},
		"blind replaces range tombstone": {
			data:           kvs{rangeKV("b", "c", 1, "")},
			sst:            kvs{rangeKV("a", "d", 1, "")},
			expect:         kvs{rangeKV("a", "d", 1, "")},
			expectStatsEst: true,
		},
		"blind extends overlapping range tombstone": {
			data:           kvs{rangeKV("c", "e", 1, "")},
			sst:            kvs{rangeKV("d", "f", 1, "")},
			expect:         kvs{rangeKV("c", "f", 1, "")},
			expectStatsEst: true,
		},
		"blind rejects SST inline values under race only": { // unfortunately, for performance
			sst:            kvs{pointKV("a", 0, "inline")},
			expect:         kvs{pointKV("a", 0, "inline")},
			expectStatsEst: true,
			expectErrRace:  `SST contains inline value or intent for key "a"/0,0`,
		},
		"blind writes above existing inline values": { // unfortunately, for performance
			data:           kvs{pointKV("a", 0, "inline")},
			sst:            kvs{pointKV("a", 2, "sst")},
			expect:         kvs{pointKV("a", 0, "inline"), pointKV("a", 2, "sst")},
			expectStatsEst: true,
		},
		"blind rejects local timestamp under race only": { // unfortunately, for performance
			sst:            kvs{pointKVWithLocalTS("a", 2, 1, "a2")},
			expect:         kvs{pointKVWithLocalTS("a", 2, 1, "a2")},
			expectStatsEst: true,
			expectErrRace:  `SST contains non-empty Local Timestamp in the MVCC value header for key "a"/2.000000000,0`,
		},
		"blind rejects local timestamp on range key under race only": { // unfortunately, for performance
			sst:            kvs{rangeKVWithLocalTS("a", "d", 2, 1, "")},
			expect:         kvs{rangeKVWithLocalTS("a", "d", 2, 1, "")},
			expectStatsEst: true,
			expectErrRace:  `SST contains non-empty MVCC value header for range key {a-d}/2.000000000,0`,
		},

		// SSTTimestampToRequestTimestamp
		"SSTTimestampToRequestTimestamp rewrites timestamp": {
			reqTS:          10,
			toReqTS:        1,
			sst:            kvs{pointKV("a", 1, "a1"), pointKV("b", 1, "b1"), rangeKV("d", "f", 1, "")},
			expect:         kvs{pointKV("a", 10, "a1"), pointKV("b", 10, "b1"), rangeKV("d", "f", 10, "")},
			expectStatsEst: true,
		},
		"SSTTimestampToRequestTimestamp succeeds on AddSSTableRequireAtRequestTimestamp": {
			reqTS:          10,
			toReqTS:        1,
			requireReqTS:   true,
			sst:            kvs{pointKV("a", 1, "a1"), pointKV("b", 1, "b1")},
			expect:         kvs{pointKV("a", 10, "a1"), pointKV("b", 10, "b1")},
			expectStatsEst: true,
		},
		"SSTTimestampToRequestTimestamp writes tombstones": {
			reqTS:          10,
			toReqTS:        1,
			sst:            kvs{pointKV("a", 1, "")},
			expect:         kvs{pointKV("a", 10, "")},
			expectStatsEst: true,
		},
		"SSTTimestampToRequestTimestamp rejects incorrect SST timestamp": {
			reqTS:   10,
			toReqTS: 1,
			sst:     kvs{pointKV("a", 1, "a1"), pointKV("b", 1, "b1"), pointKV("c", 2, "c2")},
			expectErr: []string{
				`unexpected timestamp 2.000000000,0 (expected 1.000000000,0) for key "c"`,
				`key has suffix "\x00\x00\x00\x00w5\x94\x00\t", expected "\x00\x00\x00\x00;\x9a\xca\x00\t"`,
				`has suffix 0x000000007735940009; require 0x000000003b9aca0009`,
			},
		},
		"SSTTimestampToRequestTimestamp rejects incorrect SST timestamp for range keys": {
			reqTS:   10,
			toReqTS: 1,
			sst:     kvs{pointKV("a", 1, "a1"), rangeKV("c", "d", 2, "")},
			expectErr: []string{
				`unexpected timestamp 2.000000000,0 (expected 1.000000000,0) for range key {c-d}`,
				`key has suffix "\x00\x00\x00\x00w5\x94\x00\t", expected "\x00\x00\x00\x00;\x9a\xca\x00\t"`,
				`has suffix 0x000000007735940009; require 0x000000003b9aca0009`,
			},
		},
		"SSTTimestampToRequestTimestamp rejects incorrect 0 SST timestamp": {
			reqTS:   10,
			toReqTS: 1,
			sst:     kvs{pointKV("a", 1, "a1"), pointKV("b", 1, "b1"), pointKV("c", 0, "c0")},
			expectErr: []string{
				`unexpected timestamp 0,0 (expected 1.000000000,0) for key "c"`,
				`key has suffix "", expected "\x00\x00\x00\x00;\x9a\xca\x00\t"`,
				`has suffix 0x; require 0x000000003b9aca0009`,
			},
			expectErrRace: `SST contains inline value or intent for key "c"/0,0`,
		},
		"SSTTimestampToRequestTimestamp writes below and replaces": {
			reqTS:          5,
			toReqTS:        1,
			data:           kvs{pointKV("a", 5, "a5"), pointKV("b", 7, "b7")},
			sst:            kvs{pointKV("a", 1, "sst"), pointKV("b", 1, "sst")},
			expect:         kvs{pointKV("a", 5, "sst"), pointKV("b", 7, "b7"), pointKV("b", 5, "sst")},
			expectStatsEst: true,
		},
		"SSTTimestampToRequestTimestamp returns LockConflictError for intents": {
			reqTS:     10,
			toReqTS:   1,
			data:      kvs{pointKV("a", intentTS, "intent")},
			sst:       kvs{pointKV("a", 1, "a@1")},
			expectErr: &kvpb.LockConflictError{},
		},
		"SSTTimestampToRequestTimestamp errors with DisallowConflicts below existing": {
			reqTS:      5,
			toReqTS:    10,
			noConflict: true,
			data:       kvs{pointKV("a", 5, "a5"), pointKV("b", 7, "b7")},
			sst:        kvs{pointKV("a", 10, "sst"), pointKV("b", 10, "sst")},
			expectErr:  &kvpb.WriteTooOldError{},
		},
		"SSTTimestampToRequestTimestamp succeeds with DisallowConflicts above existing": {
			reqTS:      8,
			toReqTS:    1,
			noConflict: true,
			data:       kvs{pointKV("a", 5, "a5"), pointKV("b", 7, "b7")},
			sst:        kvs{pointKV("a", 1, "sst"), pointKV("b", 1, "sst")},
			expect:     kvs{pointKV("a", 8, "sst"), pointKV("a", 5, "a5"), pointKV("b", 8, "sst"), pointKV("b", 7, "b7")},
		},
		"SSTTimestampToRequestTimestamp errors with DisallowShadowingBelow equal value above existing below limit": {
			reqTS:         7,
			toReqTS:       10,
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 3, "a3")},
			sst:           kvs{pointKV("a", 10, "a3")},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"SSTTimestampToRequestTimestamp errors with DisallowShadowingBelow errors above existing above limit": {
			reqTS:         7,
			toReqTS:       10,
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 6, "a6")},
			sst:           kvs{pointKV("a", 10, "sst")},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"SSTTimestampToRequestTimestamp allows DisallowShadowingBelow equal value above existing above limit": {
			reqTS:         7,
			toReqTS:       10,
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 6, "a6")},
			sst:           kvs{pointKV("a", 10, "a6")},
			expect:        kvs{pointKV("a", 7, "a6"), pointKV("a", 6, "a6")},
		},
		"SSTTimestampToRequestTimestamp ignores local timestamp unless under race": { // unfortunately, for performance
			reqTS:          10,
			toReqTS:        2,
			sst:            kvs{pointKVWithLocalTS("a", 2, 1, "a2")},
			expect:         kvs{pointKVWithLocalTS("a", 10, 1, "a2")},
			expectStatsEst: true,
			expectErrRace:  `SST contains non-empty Local Timestamp in the MVCC value header for key "a"/2.000000000,0`,
		},
		"SSTTimestampToRequestTimestamp with DisallowConflicts causes estimated stats with range key masking": {
			reqTS:          5,
			toReqTS:        2, // NB: 2 is below range key at 3, to test range key masking timestamp
			noConflict:     true,
			data:           kvs{rangeKV("a", "c", 3, ""), pointKV("b", 1, "b1")},
			sst:            kvs{pointKV("b", 2, "sst")},
			expect:         kvs{rangeKV("a", "c", 3, ""), pointKV("b", 5, "sst"), pointKV("b", 1, "b1")},
			expectStatsEst: !storage.DisableCheckSSTRangeKeyMasking, // assert correct without masking
		},

		// DisallowConflicts
		"DisallowConflicts allows above and beside": {
			noConflict: true,
			data:       kvs{pointKV("a", 3, "a3"), pointKV("b", 1, "")},
			sst:        kvs{pointKV("a", 4, "sst"), pointKV("b", 3, "sst"), pointKV("c", 1, "sst")},
			expect: kvs{
				pointKV("a", 4, "sst"), pointKV("a", 3, "a3"), pointKV("b", 3, "sst"), pointKV("b", 1, ""), pointKV("c", 1, "sst"),
			},
		},
		"DisallowConflicts returns WriteTooOldError below existing": {
			noConflict: true,
			data:       kvs{pointKV("a", 3, "a3")},
			sst:        kvs{pointKV("a", 2, "sst")},
			expectErr:  &kvpb.WriteTooOldError{},
		},
		"DisallowConflicts returns WriteTooOldError at existing": {
			noConflict: true,
			data:       kvs{pointKV("a", 3, "a3")},
			sst:        kvs{pointKV("a", 3, "sst")},
			expectErr:  &kvpb.WriteTooOldError{},
		},
		"DisallowConflicts returns WriteTooOldError at existing tombstone": {
			noConflict: true,
			data:       kvs{pointKV("a", 3, "")},
			sst:        kvs{pointKV("a", 3, "sst")},
			expectErr:  &kvpb.WriteTooOldError{},
		},
		"DisallowConflicts returns WriteTooOldError at existing range tombstone": {
			noConflict: true,
			data:       kvs{rangeKV("a", "c", 3, "")},
			sst:        kvs{pointKV("b", 3, "sst")},
			expectErr:  &kvpb.WriteTooOldError{},
		},
		// Regression tests for https://github.com/cockroachdb/cockroach/issues/93968.
		"DisallowConflicts WriteTooOldError straddling at existing range tombstone": {
			noConflict: true,
			data:       kvs{rangeKV("b", "z", 3, "")},
			sst:        kvs{pointKV("a", 3, "sst"), pointKV("c", 3, "sst")},
			expectErr:  &kvpb.WriteTooOldError{},
		},
		"DisallowConflicts WriteTooOldError straddling multiple existing range tombstones": {
			noConflict: true,
			data:       kvs{rangeKV("b", "c", 3, ""), rangeKV("d", "f", 3, "")},
			sst:        kvs{pointKV("a", 3, "sst"), pointKV("e", 3, "sst")},
			expectErr:  &kvpb.WriteTooOldError{},
		},
		"DisallowConflicts WriteTooOldError with SSTTimestampToRequestTimestamp straddling range keys below and at timestamp": {
			toReqTS:    1,
			reqTS:      3,
			noConflict: true,
			data:       kvs{rangeKV("a", "c", 2, ""), rangeKV("e", "g", 3, "")},
			sst:        kvs{pointKV("b", 1, "sst"), pointKV("d", 1, "sst"), pointKV("f", 1, "sst")},
			expectErr:  &kvpb.WriteTooOldError{},
		},
		"DisallowConflicts returns LockConflictError below intent": {
			noConflict: true,
			data:       kvs{pointKV("a", intentTS, "intent")},
			sst:        kvs{pointKV("a", 3, "sst")},
			expectErr:  &kvpb.LockConflictError{},
		},
		"DisallowConflicts returns LockConflictError below intent above range key": {
			noConflict: true,
			data:       kvs{pointKV("b", intentTS, "intent"), rangeKV("a", "d", 2, ""), pointKV("b", 1, "b1")},
			sst:        kvs{pointKV("b", 3, "sst")},
			expectErr:  &kvpb.LockConflictError{},
		},
		"DisallowConflicts returns LockConflictError in span": {
			noConflict: true,
			data:       kvs{pointKV("b", intentTS, "intent")},
			sst:        kvs{pointKV("a", 3, "sst"), pointKV("c", 3, "sst")},
			expectErr:  &kvpb.LockConflictError{},
		},
		"DisallowConflicts is not idempotent": {
			noConflict: true,
			data:       kvs{pointKV("a", 3, "a3")},
			sst:        kvs{pointKV("a", 3, "a3")},
			expectErr:  &kvpb.WriteTooOldError{},
		},
		"DisallowConflicts allows new SST tombstones": {
			noConflict: true,
			sst:        kvs{pointKV("a", 3, "")},
			expect:     kvs{pointKV("a", 3, "")},
		},
		"DisallowConflicts allows SST tombstones when shadowing": {
			noConflict: true,
			data:       kvs{pointKV("a", 2, "a2")},
			sst:        kvs{pointKV("a", 3, "")},
			expect:     kvs{pointKV("a", 3, ""), pointKV("a", 2, "a2")},
		},
		"DisallowConflicts does not error on SST range tombstones": {
			noConflict: true,
			sst:        kvs{rangeKV("a", "d", 3, "")},
			expect:     kvs{rangeKV("a", "d", 3, "")},
		},
		"DisallowConflicts allows new SST inline values": { // unfortunately, for performance
			noConflict:    true,
			sst:           kvs{pointKV("a", 0, "inline")},
			expect:        kvs{pointKV("a", 0, "inline")},
			expectErrRace: `SST contains inline value or intent for key "a"/0,0`,
		},
		"DisallowConflicts rejects SST inline values when shadowing": {
			noConflict:    true,
			data:          kvs{pointKV("a", 2, "a2")},
			sst:           kvs{pointKV("a", 0, "")},
			expectErr:     "SST keys must have timestamps",
			expectErrRace: `SST contains inline value or intent for key "a"/0,0`,
		},
		"DisallowConflicts rejects existing inline values when shadowing": {
			noConflict: true,
			data:       kvs{pointKV("a", 0, "a0")},
			sst:        kvs{pointKV("a", 3, "sst")},
			expectErr:  "inline values are unsupported",
		},
		// Regression test for https://github.com/cockroachdb/cockroach/issues/94053.
		"DisallowConflicts MVCC stats with point tombstone below range tombstone": {
			noConflict: true,
			data:       kvs{rangeKV("a", "c", 3, ""), pointKV("b", 2, ""), pointKV("b", 1, "b1")},
			sst:        kvs{pointKV("b", 5, "sst")},
			expect:     kvs{rangeKV("a", "c", 3, ""), pointKV("b", 5, "sst"), pointKV("b", 2, ""), pointKV("b", 1, "b1")},
		},

		// DisallowShadowingBelow
		"DisallowShadowingBelow errors above existing": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 3, "a3")},
			sst:           kvs{pointKV("a", 4, "sst")},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow errors below existing": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 3, "a3")},
			sst:           kvs{pointKV("a", 2, "sst")},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow errors at existing": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 3, "a3")},
			sst:           kvs{pointKV("a", 3, "sst")},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow returns WriteTooOldError at existing tombstone": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 3, "")},
			sst:           kvs{pointKV("a", 3, "sst")},
			expectErr:     &kvpb.WriteTooOldError{},
		},
		"DisallowShadowingBelow returns WriteTooOldError below existing tombstone": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 3, "")},
			sst:           kvs{pointKV("a", 2, "sst")},
			expectErr:     &kvpb.WriteTooOldError{},
		},
		"DisallowShadowingBelow allows above existing tombstone": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 3, "")},
			sst:           kvs{pointKV("a", 4, "sst")},
			expect:        kvs{pointKV("a", 4, "sst"), pointKV("a", 3, "")},
		},
		"DisallowShadowingBelow returns LockConflictError below intent": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", intentTS, "intent")},
			sst:           kvs{pointKV("a", 3, "sst")},
			expectErr:     &kvpb.LockConflictError{},
		},
		"DisallowShadowingBelow returns LockConflictError below intent above range key": {
			noShadowBelow: 5,
			data:          kvs{pointKV("b", intentTS, "intent"), rangeKV("a", "d", 2, ""), pointKV("b", 1, "b1")},
			sst:           kvs{pointKV("b", 3, "sst")},
			expectErr:     &kvpb.LockConflictError{},
		},
		"DisallowShadowingBelow returns LockConflictError in span": {
			noShadowBelow: 5,
			data:          kvs{pointKV("b", intentTS, "intent")},
			sst:           kvs{pointKV("a", 3, "sst"), pointKV("c", 3, "sst")},
			expectErr:     &kvpb.LockConflictError{},
		},
		"DisallowShadowingBelow is not generally idempotent": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 3, "a3")},
			sst:           kvs{pointKV("a", 3, "a3")},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow is not generally idempotent with tombstones": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 3, "")},
			sst:           kvs{pointKV("a", 3, "")},
			expectErr:     &kvpb.WriteTooOldError{},
		},
		"DisallowShadowingBelow allows new SST tombstones": {
			noShadowBelow: 5,
			sst:           kvs{pointKV("a", 3, "")},
			expect:        kvs{pointKV("a", 3, "")},
		},
		"DisallowShadowingBelow rejects SST tombstones when shadowing below": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 2, "a2")},
			sst:           kvs{pointKV("a", 3, "")},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow allows SST range tombstones": {
			noShadowBelow: 3,
			sst:           kvs{rangeKV("a", "d", 3, "")},
			expect:        kvs{rangeKV("a", "d", 3, "")},
		},
		"DisallowShadowingBelow allows new SST inline values": { // unfortunately, for performance
			noShadowBelow: 5,
			sst:           kvs{pointKV("a", 0, "inline")},
			expect:        kvs{pointKV("a", 0, "inline")},
			expectErrRace: `SST contains inline value or intent for key "a"/0,0`,
		},
		"DisallowShadowingBelow rejects SST inline values when shadowing": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 2, "a2")},
			sst:           kvs{pointKV("a", 0, "inline")},
			expectErr:     "SST keys must have timestamps",
			expectErrRace: `SST contains inline value or intent for key "a"/0,0`,
		},
		"DisallowShadowingBelow rejects existing inline values when shadowing": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 0, "a0")},
			sst:           kvs{pointKV("a", 3, "sst")},
			expectErr:     "inline values are unsupported",
		},
		"DisallowShadowingBelow collision SST start, existing start, above": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 2, "a2")},
			sst:           kvs{pointKV("a", 7, "sst")},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow collision SST start, existing middle, below": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 2, "a2"), pointKV("a", 1, "a1"), pointKV("b", 2, "b2"), pointKV("c", 3, "c3")},
			sst:           kvs{pointKV("b", 1, "sst")},
			expectErr:     `ingested key collides with an existing one: "b"`,
		},
		"DisallowShadowingBelow collision SST end, existing end, above": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 2, "a2"), pointKV("a", 1, "a1"), pointKV("b", 2, "b2"), pointKV("d", 3, "d3")},
			sst:           kvs{pointKV("c", 3, "sst"), pointKV("d", 4, "sst")},
			expectErr:     `ingested key collides with an existing one: "d"`,
		},
		"DisallowShadowingBelow collision after write above tombstone": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 2, ""), pointKV("a", 1, "a1"), pointKV("b", 2, "b2")},
			sst:           kvs{pointKV("a", 3, "sst"), pointKV("b", 1, "sst")},
			expectErr:     `ingested key collides with an existing one: "b"`,
		},
		"DisallowShadowingBelow tombstone above tombstone": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 2, ""), pointKV("a", 1, "a1")},
			sst:           kvs{pointKV("a", 3, "")},
			expect:        kvs{pointKV("a", 3, ""), pointKV("a", 2, ""), pointKV("a", 1, "a1")},
		},
		"DisallowShadowingBelow at limit writes": {
			noShadowBelow: 5,
			sst:           kvs{pointKV("a", 5, "sst")},
			expect:        kvs{pointKV("a", 5, "sst")},
		},
		"DisallowShadowingBelow at limit errors above existing": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 3, "a3")},
			sst:           kvs{pointKV("a", 5, "sst")},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow at limit errors above existing with same value": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 3, "a3")},
			sst:           kvs{pointKV("a", 5, "a3")},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow at limit errors on replacing": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 5, "a3")},
			sst:           kvs{pointKV("a", 5, "sst")},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow at limit is idempotent": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 5, "a3")},
			sst:           kvs{pointKV("a", 5, "a3")},
			expect:        kvs{pointKV("a", 5, "a3")},
		},
		"DisallowShadowingBelow above limit writes": {
			noShadowBelow: 5,
			sst:           kvs{pointKV("a", 7, "sst")},
			expect:        kvs{pointKV("a", 7, "sst")},
		},
		"DisallowShadowingBelow above limit errors on existing below limit": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 4, "a4")},
			sst:           kvs{pointKV("a", 7, "sst")},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow tombstone above limit errors on existing below limit": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 4, "a4")},
			sst:           kvs{pointKV("a", 7, "")},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow above limit errors on existing below limit with same value": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 4, "a4")},
			sst:           kvs{pointKV("a", 7, "a3")},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow above limit errors on existing at limit": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 5, "a5")},
			sst:           kvs{pointKV("a", 7, "sst")},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow above limit allows equal value at limit": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 5, "a5")},
			sst:           kvs{pointKV("a", 7, "a5")},
			expect:        kvs{pointKV("a", 7, "a5"), pointKV("a", 5, "a5")},
		},
		"DisallowShadowingBelow above limit errors on existing above limit": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 6, "a6")},
			sst:           kvs{pointKV("a", 7, "sst")},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow above limit allows equal value above limit": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 6, "a6")},
			sst:           kvs{pointKV("a", 7, "a6")},
			expect:        kvs{pointKV("a", 7, "a6"), pointKV("a", 6, "a6")},
		},
		"DisallowShadowingBelow above limit errors on replacing": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 7, "a7")},
			sst:           kvs{pointKV("a", 7, "sst")},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow above limit is idempotent": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 7, "a7")},
			sst:           kvs{pointKV("a", 7, "a7")},
			expect:        kvs{pointKV("a", 7, "a7")},
		},
		"DisallowShadowingBelow above limit is idempotent with tombstone": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 7, "")},
			sst:           kvs{pointKV("a", 7, "")},
			expect:        kvs{pointKV("a", 7, "")},
		},
		"DisallowShadowingBelow above limit errors below existing": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 8, "a8")},
			sst:           kvs{pointKV("a", 7, "sst")},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow above limit errors below existing with same value": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 8, "a8")},
			sst:           kvs{pointKV("a", 7, "a8")},
			expectErr:     &kvpb.WriteTooOldError{},
		},
		"DisallowShadowingBelow above limit errors below tombstone": {
			noShadowBelow: 5,
			data:          kvs{pointKV("a", 8, "")},
			sst:           kvs{pointKV("a", 7, "a8")},
			expectErr:     &kvpb.WriteTooOldError{},
		},
		// MVCC Range tombstone cases.
		"DisallowConflicts allows sst range keys": {
			noConflict: true,
			data:       kvs{pointKV("a", 6, "d")},
			sst:        kvs{rangeKV("a", "b", 8, ""), pointKV("a", 7, "a8")},
			expect:     kvs{rangeKV("a", "b", 8, ""), pointKV("a", 7, "a8"), pointKV("a", 6, "d")},
		},
		"DisallowConflicts allows sst range keys 2": {
			noConflict: true,
			reqTS:      10,
			data:       kvs{pointKV("a", 6, "d"), pointKV("bbb", 6, ""), pointKV("c", 6, "d"), pointKV("d", 6, "d"), pointKV("dd", 6, "")},
			sst:        kvs{pointKV("aa", 9, ""), pointKV("b", 9, "dee"), rangeKV("bb", "e", 9, ""), pointKV("cc", 10, "")},
			expect:     kvs{pointKV("a", 6, "d"), pointKV("aa", 9, ""), pointKV("b", 9, "dee"), rangeKV("bb", "e", 9, ""), pointKV("bbb", 6, ""), pointKV("c", 6, "d"), pointKV("cc", 10, ""), pointKV("d", 6, "d"), pointKV("dd", 6, "")},
		},
		"DisallowConflicts does not skip sst range keys ahead of first one": {
			noConflict: true,
			reqTS:      10,
			toReqTS:    8,
			data:       kvs{pointKV("a", 6, "d"), pointKV("e", 5, "d")},
			sst:        kvs{rangeKV("b", "c", 8, ""), rangeKV("cc", "f", 8, "")},
			expect:     kvs{pointKV("a", 6, "d"), rangeKV("b", "c", 10, ""), rangeKV("cc", "f", 10, ""), pointKV("e", 5, "d")},
		},
		"DisallowConflicts correctly accounts for complex fragment cases": {
			noConflict: true,
			reqTS:      10,
			data:       kvs{rangeKV("b", "c", 7, ""), rangeKV("b", "c", 6, ""), rangeKV("c", "f", 6, ""), rangeKV("f", "g", 7, ""), rangeKV("f", "g", 6, "")},
			sst:        kvs{rangeKV("a", "d", 8, ""), rangeKV("e", "g", 8, "")},
			expect:     kvs{rangeKV("a", "b", 8, ""), rangeKV("b", "c", 8, ""), rangeKV("b", "c", 7, ""), rangeKV("b", "c", 6, ""), rangeKV("c", "d", 8, ""), rangeKV("c", "d", 6, ""), rangeKV("d", "e", 6, ""), rangeKV("e", "f", 8, ""), rangeKV("e", "f", 6, ""), rangeKV("f", "g", 8, ""), rangeKV("f", "g", 7, ""), rangeKV("f", "g", 6, "")},
		},
		"DisallowConflicts correctly accounts for complex fragment cases 2": {
			noConflict: true,
			reqTS:      10,
			data:       kvs{rangeKV("a", "g", 6, ""), pointKV("bb", 5, "bar")},
			sst:        kvs{rangeKV("b", "c", 8, ""), pointKV("cc", 8, "foo"), rangeKV("e", "f", 8, "")},
			expect:     kvs{rangeKV("a", "b", 6, ""), rangeKV("b", "c", 8, ""), rangeKV("b", "c", 6, ""), pointKV("bb", 5, "bar"), rangeKV("c", "e", 6, ""), pointKV("cc", 8, "foo"), rangeKV("e", "f", 8, ""), rangeKV("e", "f", 6, ""), rangeKV("f", "g", 6, "")},
		},
		"DisallowConflicts correctly accounts for complex fragment cases 3": {
			noConflict: true,
			reqTS:      10,
			data:       kvs{rangeKV("c", "d", 6, ""), pointKV("e", 6, ""), pointKV("e", 5, "foo"), pointKV("f", 6, ""), pointKV("f", 5, ""), pointKV("g", 6, ""), rangeKV("j", "k", 5, "")},
			sst:        kvs{rangeKV("a", "l", 8, "")},
			expect:     kvs{rangeKV("a", "c", 8, ""), rangeKV("c", "d", 8, ""), rangeKV("c", "d", 6, ""), rangeKV("d", "j", 8, ""), pointKV("e", 6, ""), pointKV("e", 5, "foo"), pointKV("f", 6, ""), pointKV("f", 5, ""), pointKV("g", 6, ""), rangeKV("j", "k", 8, ""), rangeKV("j", "k", 5, ""), rangeKV("k", "l", 8, "")},
		},
		"DisallowConflicts correctly accounts for complex fragment cases 4": {
			noConflict: true,
			reqTS:      10,
			data:       kvs{rangeKV("c", "d", 6, ""), rangeKV("j", "k", 5, "")},
			sst:        kvs{rangeKV("a", "l", 8, "")},
			expect:     kvs{rangeKV("a", "c", 8, ""), rangeKV("c", "d", 8, ""), rangeKV("c", "d", 6, ""), rangeKV("d", "j", 8, ""), rangeKV("j", "k", 8, ""), rangeKV("j", "k", 5, ""), rangeKV("k", "l", 8, "")},
		},
		"DisallowConflicts correctly accounts for complex fragment cases 5": {
			noConflict: true,
			reqTS:      10,
			data:       kvs{pointKV("cc", 7, ""), pointKV("cc", 6, ""), pointKV("cc", 5, "foo"), pointKV("cc", 4, ""), pointKV("cc", 3, "bar"), pointKV("cc", 2, "barfoo"), rangeKV("ab", "g", 1, "")},
			sst:        kvs{pointKV("aa", 8, "foo"), pointKV("aaa", 8, ""), pointKV("ac", 8, "foo"), rangeKV("b", "c", 8, ""), pointKV("ca", 8, "foo"), pointKV("cb", 8, "foo"), pointKV("cc", 8, "foo"), rangeKV("d", "e", 8, ""), pointKV("e", 8, "foobar")},
			expect:     kvs{pointKV("aa", 8, "foo"), pointKV("aaa", 8, ""), rangeKV("ab", "b", 1, ""), pointKV("ac", 8, "foo"), rangeKV("b", "c", 8, ""), rangeKV("b", "c", 1, ""), rangeKV("c", "d", 1, ""), pointKV("ca", 8, "foo"), pointKV("cb", 8, "foo"), pointKV("cc", 8, "foo"), pointKV("cc", 7, ""), pointKV("cc", 6, ""), pointKV("cc", 5, "foo"), pointKV("cc", 4, ""), pointKV("cc", 3, "bar"), pointKV("cc", 2, "barfoo"), rangeKV("d", "e", 8, ""), rangeKV("d", "e", 1, ""), rangeKV("e", "g", 1, ""), pointKV("e", 8, "foobar")},
		},
		"DisallowConflicts handles existing point key above existing range tombstone": {
			noConflict: true,
			reqTS:      10,
			data:       kvs{pointKV("c", 7, ""), rangeKV("a", "g", 6, ""), pointKV("h", 7, "")},
			sst:        kvs{rangeKV("b", "d", 8, ""), rangeKV("f", "j", 8, "")},
			expect:     kvs{rangeKV("a", "b", 6, ""), rangeKV("b", "d", 8, ""), rangeKV("b", "d", 6, ""), pointKV("c", 7, ""), rangeKV("d", "f", 6, ""), rangeKV("f", "g", 8, ""), rangeKV("f", "g", 6, ""), rangeKV("g", "j", 8, ""), pointKV("h", 7, "")},
		},
		"DisallowConflicts accounts for point key already deleted in engine": {
			noConflict: true,
			reqTS:      10,
			data:       kvs{rangeKV("b", "d", 6, ""), pointKV("c", 5, "foo")},
			sst:        kvs{rangeKV("a", "d", 8, "")},
			expect:     kvs{rangeKV("a", "b", 8, ""), rangeKV("b", "d", 8, ""), rangeKV("b", "d", 6, ""), pointKV("c", 5, "foo")},
		},
		"DisallowConflicts allows fragmented sst range keys": {
			noConflict: true,
			data:       kvs{pointKV("a", 6, "d")},
			sst:        kvs{rangeKV("a", "b", 8, ""), pointKV("a", 7, "a8"), rangeKV("c", "d", 8, "")},
			expect:     kvs{rangeKV("a", "b", 8, ""), pointKV("a", 7, "a8"), pointKV("a", 6, "d"), rangeKV("c", "d", 8, "")},
		},
		"DisallowConflicts allows sst and engine range keys with no points": {
			noConflict: true,
			data:       kvs{rangeKV("a", "b", 6, ""), rangeKV("e", "f", 6, "")},
			sst:        kvs{rangeKV("a", "b", 8, ""), rangeKV("c", "d", 8, "")},
			expect:     kvs{rangeKV("a", "b", 8, ""), rangeKV("a", "b", 6, ""), rangeKV("c", "d", 8, ""), rangeKV("e", "f", 6, "")},
		},
		"DisallowConflicts returns engine intents below sst range keys as lock conflict errors": {
			noConflict: true,
			data:       kvs{pointKV("b", intentTS, "intent")},
			sst:        kvs{rangeKV("a", "c", intentTS+8, "")},
			expectErr:  &kvpb.LockConflictError{},
		},
		"DisallowConflicts disallows sst range keys below engine point key": {
			noConflict: true,
			data:       kvs{pointKV("a", 6, "d")},
			sst:        kvs{pointKV("a", 7, "a8"), rangeKV("a", "b", 5, "")},
			expectErr:  &kvpb.WriteTooOldError{},
		},
		"DisallowConflicts disallows sst point keys below engine range key": {
			noConflict: true,
			data:       kvs{rangeKV("a", "b", 8, ""), pointKV("a", 6, "b6")},
			sst:        kvs{pointKV("a", 7, "a8")},
			expectErr:  &kvpb.WriteTooOldError{},
		},
		"DisallowConflicts disallows sst range keys below engine range key": {
			noConflict: true,
			data:       kvs{rangeKV("a", "b", 8, ""), pointKV("a", 6, "d")},
			sst:        kvs{pointKV("a", 9, "a8"), rangeKV("a", "b", 7, "")},
			expectErr:  &kvpb.WriteTooOldError{},
		},
		"DisallowConflicts allows sst range keys above engine range keys": {
			noConflict: true,
			data:       kvs{pointKV("a", 6, "d"), rangeKV("a", "b", 5, "")},
			sst:        kvs{pointKV("a", 7, "a8"), rangeKV("a", "b", 8, "")},
			expect:     kvs{rangeKV("a", "b", 8, ""), rangeKV("a", "b", 5, ""), pointKV("a", 7, "a8"), pointKV("a", 6, "d")},
		},
		"DisallowConflicts allows fragmented sst range keys above engine range keys": {
			noConflict: true,
			data:       kvs{pointKV("a", 6, "d"), rangeKV("a", "b", 5, "")},
			sst:        kvs{pointKV("a", 7, "a8"), rangeKV("a", "b", 8, ""), rangeKV("c", "d", 8, "")},
			expect:     kvs{rangeKV("a", "b", 8, ""), rangeKV("a", "b", 5, ""), pointKV("a", 7, "a8"), pointKV("a", 6, "d"), rangeKV("c", "d", 8, "")},
		},
		"DisallowConflicts allows fragmented straddling sst range keys": {
			noConflict: true,
			data:       kvs{pointKV("a", 6, "d"), rangeKV("b", "d", 5, "")},
			sst:        kvs{pointKV("a", 7, "a8"), rangeKV("a", "c", 8, ""), rangeKV("c", "d", 7, "")},
			expect:     kvs{rangeKV("a", "b", 8, ""), pointKV("a", 7, "a8"), pointKV("a", 6, "d"), rangeKV("b", "c", 8, ""), rangeKV("b", "c", 5, ""), rangeKV("c", "d", 7, ""), rangeKV("c", "d", 5, "")},
		},
		"DisallowConflicts allows fragmented straddling sst range keys with no points": {
			noConflict: true,
			data:       kvs{rangeKV("b", "d", 5, "")},
			sst:        kvs{rangeKV("a", "c", 8, ""), rangeKV("c", "d", 7, "")},
			expect:     kvs{rangeKV("a", "b", 8, ""), rangeKV("b", "c", 8, ""), rangeKV("b", "c", 5, ""), rangeKV("c", "d", 7, ""), rangeKV("c", "d", 5, "")},
		},
		"DisallowConflicts allows engine range keys contained within sst range keys": {
			noConflict: true,
			data:       kvs{pointKV("a", 6, "d"), rangeKV("b", "d", 5, "")},
			sst:        kvs{pointKV("a", 7, "a8"), rangeKV("a", "e", 8, "")},
			expect:     kvs{rangeKV("a", "b", 8, ""), pointKV("a", 7, "a8"), pointKV("a", 6, "d"), rangeKV("b", "d", 8, ""), rangeKV("b", "d", 5, ""), rangeKV("d", "e", 8, "")},
		},
		"DisallowConflicts allows engine range keys contained within sst range keys 2": {
			noConflict: true,
			data:       kvs{pointKV("a", 6, "d"), rangeKV("b", "d", 5, "")},
			sst:        kvs{pointKV("a", 7, "a8"), rangeKV("a", "d", 8, "")},
			expect:     kvs{rangeKV("a", "b", 8, ""), pointKV("a", 7, "a8"), pointKV("a", 6, "d"), rangeKV("b", "d", 8, ""), rangeKV("b", "d", 5, "")},
		},
		"DisallowConflicts allows engine range keys contained within sst range keys 3": {
			noConflict: true,
			data:       kvs{pointKV("a", 6, "d"), rangeKV("a", "d", 5, "")},
			sst:        kvs{pointKV("a", 7, "a8"), rangeKV("b", "e", 8, "")},
			expect:     kvs{rangeKV("a", "b", 5, ""), pointKV("a", 7, "a8"), pointKV("a", 6, "d"), rangeKV("b", "d", 8, ""), rangeKV("b", "d", 5, ""), rangeKV("d", "e", 8, "")},
		},
		"DisallowConflicts does not skip over engine range keys covering no sst points": {
			noConflict: true,
			data:       kvs{pointKV("a", 6, "d"), rangeKV("b", "c", 6, ""), rangeKV("c", "d", 5, "")},
			sst:        kvs{pointKV("a", 7, "a8"), rangeKV("a", "e", 8, "")},
			expect:     kvs{rangeKV("a", "b", 8, ""), pointKV("a", 7, "a8"), pointKV("a", 6, "d"), rangeKV("b", "c", 8, ""), rangeKV("b", "c", 6, ""), rangeKV("c", "d", 8, ""), rangeKV("c", "d", 5, ""), rangeKV("d", "e", 8, "")},
		},
		"DisallowConflicts does not allow conflict with engine range key covering no sst points": {
			noConflict: true,
			data:       kvs{pointKV("a", 6, "d"), rangeKV("b", "c", 9, ""), rangeKV("c", "d", 5, "")},
			sst:        kvs{pointKV("a", 7, "a8"), rangeKV("a", "e", 8, "")},
			expectErr:  &kvpb.WriteTooOldError{},
		},
		"DisallowConflicts allows sst range keys contained within engine range keys": {
			noConflict: true,
			data:       kvs{pointKV("a", 6, "d"), rangeKV("a", "e", 5, "")},
			sst:        kvs{pointKV("a", 7, "a8"), rangeKV("b", "d", 8, "")},
			expect:     kvs{rangeKV("a", "b", 5, ""), pointKV("a", 7, "a8"), pointKV("a", 6, "d"), rangeKV("b", "d", 8, ""), rangeKV("b", "d", 5, ""), rangeKV("d", "e", 5, "")},
		},
		"DisallowConflicts allows sst range key fragmenting engine range keys": {
			noConflict: true,
			data:       kvs{pointKV("a", 6, "d"), rangeKV("a", "c", 5, ""), rangeKV("c", "e", 6, "")},
			sst:        kvs{pointKV("a", 7, "a8"), rangeKV("b", "d", 8, "")},
			expect:     kvs{rangeKV("a", "b", 5, ""), pointKV("a", 7, "a8"), pointKV("a", 6, "d"), rangeKV("b", "c", 8, ""), rangeKV("b", "c", 5, ""), rangeKV("c", "d", 8, ""), rangeKV("c", "d", 6, ""), rangeKV("d", "e", 6, "")},
		},
		"DisallowConflicts calculates stats correctly for merged range keys": {
			noConflict: true,
			data:       kvs{rangeKV("a", "c", 8, ""), pointKV("a", 6, "d"), rangeKV("d", "e", 8, "")},
			sst:        kvs{pointKV("a", 10, "de"), rangeKV("c", "d", 8, ""), pointKV("f", 10, "de")},
			expect:     kvs{rangeKV("a", "e", 8, ""), pointKV("a", 10, "de"), pointKV("a", 6, "d"), pointKV("f", 10, "de")},
		},
		"DisallowConflicts calculates stats correctly for merged range keys 2": {
			noConflict: true,
			data:       kvs{pointKV("a", 6, "d"), rangeKV("c", "d", 8, "")},
			sst:        kvs{rangeKV("a", "c", 8, ""), rangeKV("d", "e", 8, ""), pointKV("f", 8, "foo")},
			expect:     kvs{rangeKV("a", "e", 8, ""), pointKV("a", 6, "d"), pointKV("f", 8, "foo")},
		},
		"DisallowConflicts calculates stats correctly for merged range keys 3": {
			noConflict: true,
			data:       kvs{pointKV("a", 6, "d"), rangeKV("c", "d", 8, ""), rangeKV("e", "f", 8, "")},
			sst:        kvs{rangeKV("a", "c", 8, ""), rangeKV("d", "e", 8, ""), pointKV("g", 8, "foo")},
			expect:     kvs{rangeKV("a", "f", 8, ""), pointKV("a", 6, "d"), pointKV("g", 8, "foo")},
		},
		"DisallowShadowingBelow disallows sst range keys shadowing live keys": {
			noShadowBelow: 3,
			data:          kvs{pointKV("a", 6, "d"), rangeKV("a", "b", 5, "")},
			sst:           kvs{rangeKV("a", "b", 8, "")},
			expectErr:     "ingested range key collides with an existing one",
		},
		"DisallowShadowingBelow allows shadowing of keys deleted by engine range tombstones": {
			noShadowBelow: 3,
			data:          kvs{rangeKV("a", "b", 7, ""), pointKV("a", 6, "d")},
			sst:           kvs{pointKV("a", 8, "a8")},
			expect:        kvs{rangeKV("a", "b", 7, ""), pointKV("a", 8, "a8"), pointKV("a", 6, "d")},
		},
		"DisallowShadowingBelow allows idempotent range tombstones": {
			noShadowBelow: 3,
			data:          kvs{rangeKV("a", "b", 7, "")},
			sst:           kvs{rangeKV("a", "b", 7, "")},
			expect:        kvs{rangeKV("a", "b", 7, "")},
		},
		"DisallowConflict with allowed shadowing disallows idempotent range tombstones": {
			noConflict: true,
			data:       kvs{rangeKV("a", "b", 7, "")},
			sst:        kvs{rangeKV("a", "b", 7, "")},
			expectErr:  &kvpb.WriteTooOldError{},
		},
		"DisallowConflict allows overlapping sst range tombstones": {
			noConflict:   true,
			data:         kvs{pointKV("ib", 6, "foo"), pointKV("if", 6, "foo"), pointKV("it", 6, "foo"), rangeKV("i", "j", 5, "")},
			sst:          kvs{rangeKV("ia", "irc", 8, ""), rangeKV("ie", "iu", 9, ""), pointKV("ic", 7, "foo"), pointKV("iq", 8, "foo")},
			ignoreExpect: true,
		},
		"DisallowConflict does not miss deleted ext keys": {
			noConflict:   true,
			data:         kvs{pointKV("c", 6, "foo"), pointKV("d", 6, "foo"), pointKV("e", 6, "foo"), rangeKV("bb", "j", 5, "")},
			sst:          kvs{rangeKV("b", "k", 8, ""), pointKV("cc", 9, "foo"), pointKV("dd", 7, "foo"), pointKV("ee", 7, "foo")},
			ignoreExpect: true,
		},
		"DisallowConflict does not miss deleted ext keys 2": {
			noConflict:   true,
			data:         kvs{pointKV("kr", 7, "foo"), pointKV("krj", 7, "foo"), pointKV("ksq", 7, "foo"), pointKV("ku", 6, "foo")},
			sst:          kvs{rangeKV("ke", "l", 11, ""), pointKV("kr", 8, "bar"), pointKV("ksxk", 9, "bar")},
			ignoreExpect: true,
		},
		"DisallowConflict does not miss deleted ext keys 3": {
			noConflict:   true,
			data:         kvs{pointKV("xe", 5, "foo"), pointKV("xg", 6, "foo"), pointKV("xh", 7, "foo"), rangeKV("xf", "xk", 5, "")},
			sst:          kvs{pointKV("xeqn", 10, "foo"), pointKV("xh", 12, "foo"), rangeKV("x", "xp", 11, "")},
			ignoreExpect: true,
		},
		"DisallowConflict does not miss deleted ext keys 4": {
			noConflict:   true,
			data:         kvs{pointKV("xh", 7, "foo")},
			sst:          kvs{pointKV("xh", 12, "foo"), rangeKV("x", "xp", 11, "")},
			ignoreExpect: true,
		},
		"DisallowConflict does not repeatedly count ext value deleted by ext range": {
			noConflict:   true,
			data:         kvs{rangeKV("bf", "bjs", 7, ""), pointKV("bbeg", 6, "foo"), pointKV("bf", 6, "foo"), pointKV("bl", 6, "foo")},
			sst:          kvs{pointKV("bbtq", 11, "foo"), pointKV("bbw", 11, "foo"), pointKV("bc", 11, "foo"), pointKV("bl", 12, "foo")},
			ignoreExpect: true,
		},
		"DisallowConflict does not miss sst range keys after overlapping point": {
			noConflict:   true,
			data:         kvs{pointKV("oe", 8, "foo"), pointKV("oi", 8, "foo"), rangeKV("o", "omk", 7, ""), pointKV("od", 6, "foo")},
			sst:          kvs{pointKV("oe", 11, "foo"), pointKV("oih", 12, "foo"), rangeKV("ods", "ogvh", 10, ""), rangeKV("ogvh", "ohl", 10, ""), rangeKV("ogvh", "ohl", 9, "")},
			ignoreExpect: true,
		},
		"DisallowConflict maintains ext iter ahead of sst iter": {
			noConflict:   true,
			data:         kvs{pointKV("c", 6, "foo"), rangeKV("c", "e", 5, "")},
			sst:          kvs{rangeKV("a", "b", 10, ""), pointKV("d", 9, "foo")},
			ignoreExpect: true,
		},
		"DisallowConflict does not double count deleted ext key": {
			noConflict:   true,
			data:         kvs{pointKV("e", 6, "foo"), rangeKV("e", "g", 5, "")},
			sst:          kvs{rangeKV("a", "j", 10, ""), pointKV("b", 11, "bar"), pointKV("c", 11, "foo"), pointKV("d", 11, "foo"), pointKV("f", 11, "foo")},
			ignoreExpect: true,
		},
		"DisallowConflict does not double count deleted ext key 2": {
			noConflict:   true,
			data:         kvs{pointKV("b", 7, "foo"), pointKV("d", 6, "foo"), rangeKV("d", "e", 5, "")},
			sst:          kvs{rangeKV("a", "j", 10, ""), pointKV("c", 11, "foo"), pointKV("d", 12, "bar")},
			ignoreExpect: true,
		},
		"DisallowConflict handles complex range key cases": {
			noConflict:   true,
			data:         kvs{pointKV("cb", 6, "foo"), pointKV("cm", 6, "foo"), pointKV("cn", 6, ""), pointKV("co", 6, "foo"), rangeKV("c", "co", 5, "")},
			sst:          kvs{rangeKV("cd", "cnr", 9, ""), pointKV("cn", 8, "bar"), rangeKV("cnr", "d", 10, ""), pointKV("co", 8, "bar")},
			ignoreExpect: true,
		},
		"DisallowConflict handles complex range key cases 2": {
			noConflict:   true,
			data:         kvs{pointKV("cb", 6, "foo"), pointKV("cm", 6, "foo"), pointKV("cn", 6, ""), pointKV("cx", 6, "foo"), rangeKV("c", "co", 5, "")},
			sst:          kvs{rangeKV("cd", "cnr", 9, ""), pointKV("cn", 8, "bar"), rangeKV("cnr", "d", 10, ""), pointKV("co", 8, "bar")},
			ignoreExpect: true,
		},
	}
	testutils.RunTrueAndFalse(t, "IngestAsWrites", func(t *testing.T, ingestAsWrites bool) {
		testutils.RunValues(t, "RewriteConcurrency", []int64{0, 8}, func(t *testing.T, c int64) {
			testutils.RunValues(t, "ApproximateDiskBytes", []int{0, 1000000}, func(t *testing.T, approxBytes int) {
				approxDiskBytes := uint64(approxBytes)
				for name, tc := range testcases {
					t.Run(name, func(t *testing.T) {
						ctx := context.Background()
						st := cluster.MakeTestingClusterSettings()
						batcheval.AddSSTableRewriteConcurrency.Override(ctx, &st.SV, c)
						batcheval.AddSSTableRequireAtRequestTimestamp.Override(ctx, &st.SV, tc.requireReqTS)

						engine := storage.NewDefaultInMemForTesting()
						defer engine.Close()

						// Write initial data.
						intentTxn := roachpb.MakeTransaction("intentTxn", nil, 0, 0, hlc.Timestamp{WallTime: intentTS * 1e9}, 0, 1, 0, false /* omitInRangefeeds */)
						b := engine.NewBatch()
						defer b.Close()
						for i := len(tc.data) - 1; i >= 0; i-- { // reverse, older timestamps first
							switch kv := tc.data[i].(type) {
							case storage.MVCCKeyValue:
								var txn *roachpb.Transaction
								if kv.Key.Timestamp.WallTime == intentTS {
									txn = &intentTxn
								}
								kv.Key.Timestamp.WallTime *= 1e9
								v, err := storage.DecodeMVCCValue(kv.Value)
								require.NoError(t, err)
								_, err = storage.MVCCPut(ctx, b, kv.Key.Key, kv.Key.Timestamp, v.Value, storage.MVCCWriteOptions{Txn: txn})
								require.NoError(t, err)
							case storage.MVCCRangeKeyValue:
								v, err := storage.DecodeMVCCValue(kv.Value)
								require.NoError(t, err)
								require.True(t, v.IsTombstone(), "MVCC range keys must be tombstones")
								kv.RangeKey.Timestamp.WallTime *= 1e9
								v.MVCCValueHeader.LocalTimestamp.WallTime *= 1e9
								require.NoError(t, storage.MVCCDeleteRangeUsingTombstone(
									ctx, b, nil, kv.RangeKey.StartKey, kv.RangeKey.EndKey, kv.RangeKey.Timestamp, v.MVCCValueHeader.LocalTimestamp, nil, nil, false,
									0, 0, nil))
							default:
								t.Fatalf("unknown KV type %T", kv)
							}
						}
						require.NoError(t, b.Commit(false))
						stats := storageutils.EngineStats(t, engine, 0)
						// All timestamps are experienced in increments of 1e9 nanoseconds,
						// as 1e9 nanoseconds = 1 second. This is to accurately test for
						// GCBytesAge in stats, which is only calculated in full-second
						// increments.
						tc.toReqTS *= 1e9
						tc.reqTS *= 1e9
						tc.noShadowBelow *= 1e9

						// Build and add SST.
						if tc.toReqTS != 0 && tc.reqTS == 0 && tc.expectErr == nil {
							t.Fatal("can't set toReqTS without reqTS")
						}
						var sstKvs []interface{}
						for i := range tc.sst {
							switch kv := tc.sst[i].(type) {
							case storage.MVCCKeyValue:
								kv.Key.Timestamp.WallTime *= 1e9
								sstKvs = append(sstKvs, kv)
							case storage.MVCCRangeKeyValue:
								v, err := storage.DecodeMVCCValue(kv.Value)
								require.NoError(t, err)
								v.LocalTimestamp.WallTime *= 1e9
								kv.RangeKey.Timestamp.WallTime *= 1e9
								vBytes, err := storage.EncodeMVCCValue(v)
								require.NoError(t, err)
								sstKvs = append(sstKvs, storage.MVCCRangeKeyValue{RangeKey: kv.RangeKey, Value: vBytes})
							}
						}
						sst, start, end := storageutils.MakeSST(t, st, sstKvs)
						resp := &kvpb.AddSSTableResponse{}
						var mvccStats *enginepb.MVCCStats
						// In the no-overlap case i.e. approxDiskBytes == 0, force a regular
						// non-prefix Seek in the conflict check. Sending in nil stats
						// makes this easier as that forces the function to rely exclusively
						// on ApproxDiskBytes, otherwise EvalAddSSTable will always use
						// prefix seeks since the test cases have too few keys in the
						// sstable.
						if approxDiskBytes != 0 {
							mvccStats = storageutils.SSTStats(t, sst, 0)
						}
						result, err := batcheval.EvalAddSSTable(ctx, engine, batcheval.CommandArgs{
							EvalCtx: (&batcheval.MockEvalCtx{ClusterSettings: st, Desc: &roachpb.RangeDescriptor{}, ApproxDiskBytes: approxDiskBytes}).EvalContext(),
							Stats:   stats,
							Header: kvpb.Header{
								Timestamp: hlc.Timestamp{WallTime: tc.reqTS},
							},
							Args: &kvpb.AddSSTableRequest{
								RequestHeader:                  kvpb.RequestHeader{Key: start, EndKey: end},
								Data:                           sst,
								MVCCStats:                      mvccStats,
								DisallowConflicts:              tc.noConflict,
								DisallowShadowingBelow:         hlc.Timestamp{WallTime: tc.noShadowBelow},
								SSTTimestampToRequestTimestamp: hlc.Timestamp{WallTime: tc.toReqTS},
								IngestAsWrites:                 ingestAsWrites,
							},
						}, resp)

						expectErr := tc.expectErr
						if tc.expectErrRace != nil && util.RaceEnabled {
							expectErr = tc.expectErrRace
						}
						if expectErr != nil {
							require.Error(t, err)
							if b, ok := expectErr.(bool); ok && b {
								// any error is fine
							} else if expectMsg, ok := expectErr.(string); ok {
								require.Contains(t, err.Error(), expectMsg)
							} else if expectMsgs, ok := expectErr.([]string); ok {
								var found bool
								for _, msg := range expectMsgs {
									if strings.Contains(err.Error(), msg) {
										found = true
										break
									}
								}
								if !found {
									t.Fatalf("%q does not contain any of %q", err, expectMsgs)
								}
							} else if e, ok := expectErr.(error); ok {
								require.True(t, errors.HasType(err, e), "expected %T, got %v", e, err)
							} else {
								require.Fail(t, "invalid expectErr", "expectErr=%v", expectErr)
							}
							return
						}
						require.NoError(t, err)

						if ingestAsWrites {
							require.Nil(t, result.Replicated.AddSSTable)
						} else {
							require.NotNil(t, result.Replicated.AddSSTable)
							require.NoError(t, fs.WriteFile(engine.Env(), "sst", result.Replicated.AddSSTable.Data, fs.UnspecifiedWriteCategory))
							require.NoError(t, engine.IngestLocalFiles(ctx, []string{"sst"}))
						}

						var expect kvs
						for i := range tc.expect {
							switch kv := tc.expect[i].(type) {
							case storage.MVCCKeyValue:
								kv.Key.Timestamp.WallTime *= 1e9
								expect = append(expect, kv)
							case storage.MVCCRangeKeyValue:
								v, err := storage.DecodeMVCCValue(kv.Value)
								require.NoError(t, err)
								v.LocalTimestamp.WallTime *= 1e9
								kv.RangeKey.Timestamp.WallTime *= 1e9
								kv.RangeKey.EncodedTimestampSuffix = storage.EncodeMVCCTimestampSuffix(kv.RangeKey.Timestamp)
								vBytes, err := storage.EncodeMVCCValue(v)
								require.NoError(t, err)
								expect = append(expect, storage.MVCCRangeKeyValue{RangeKey: kv.RangeKey, Value: vBytes})
							}
						}

						// Scan resulting data from engine.
						if !tc.ignoreExpect {
							require.Equal(t, expect, storageutils.ScanEngine(t, engine))
						}

						// Check that stats were updated correctly.
						if tc.expectStatsEst {
							require.NotZero(t, stats.ContainsEstimates, "expected stats to be estimated")
						} else {
							require.Zero(t, stats.ContainsEstimates, "found estimated stats")
							expected := storageutils.EngineStats(t, engine, stats.LastUpdateNanos)
							require.Equal(t, expected, stats)
						}
					})
				}
			})
		})
	})
}

// TestEvalAddSSTableRangefeed tests EvalAddSSTable rangefeed-related
// behavior.
func TestEvalAddSSTableRangefeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	reqTS := hlc.Timestamp{WallTime: 10}

	testcases := map[string]struct {
		sst                   kvs
		toReqTS               int64 // SSTTimestampToRequestTimestamp
		asWrites              bool  // IngestAsWrites
		expectHistoryMutation bool
		expectLogicalOps      []enginepb.MVCCLogicalOp
	}{
		"Default": {
			sst:                   kvs{pointKV("a", 1, "a1"), rangeKV("d", "f", 1, "")},
			expectHistoryMutation: true,
			expectLogicalOps:      nil,
		},
		"SSTTimestampToRequestTimestamp alone": {
			sst:                   kvs{pointKV("a", 1, "a1"), rangeKV("d", "f", 1, "")},
			toReqTS:               1,
			expectHistoryMutation: false,
			expectLogicalOps:      nil,
		},
		"IngestAsWrites alone": {
			sst:                   kvs{pointKV("a", 1, "a1"), rangeKV("d", "f", 1, "")},
			asWrites:              true,
			expectHistoryMutation: true,
			expectLogicalOps:      nil,
		},
		"IngestAsWrites and SSTTimestampToRequestTimestamp": {
			sst:                   kvs{pointKV("a", 1, "a1"), pointKV("b", 1, "b1"), rangeKV("d", "f", 1, "")},
			asWrites:              true,
			toReqTS:               1,
			expectHistoryMutation: false,
			expectLogicalOps: []enginepb.MVCCLogicalOp{
				// NOTE: Value is populated by the rangefeed processor, not MVCC, so it
				// won't show up here.
				{WriteValue: &enginepb.MVCCWriteValueOp{Key: roachpb.Key("a"), Timestamp: reqTS}},
				{WriteValue: &enginepb.MVCCWriteValueOp{Key: roachpb.Key("b"), Timestamp: reqTS}},
				{DeleteRange: &enginepb.MVCCDeleteRangeOp{StartKey: roachpb.Key("d"), EndKey: roachpb.Key("f"), Timestamp: reqTS}},
			},
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			st := cluster.MakeTestingClusterSettings()
			ctx := context.Background()

			engine := storage.NewDefaultInMemForTesting()
			defer engine.Close()
			opLogger := storage.NewOpLoggerBatch(engine.NewBatch())
			defer opLogger.Close()

			// Build and add SST.
			sst, start, end := storageutils.MakeSST(t, st, tc.sst)
			result, err := batcheval.EvalAddSSTable(ctx, opLogger, batcheval.CommandArgs{
				EvalCtx: (&batcheval.MockEvalCtx{ClusterSettings: st, Desc: &roachpb.RangeDescriptor{}}).EvalContext(),
				Header: kvpb.Header{
					Timestamp: reqTS,
				},
				Stats: &enginepb.MVCCStats{},
				Args: &kvpb.AddSSTableRequest{
					RequestHeader:                  kvpb.RequestHeader{Key: start, EndKey: end},
					Data:                           sst,
					MVCCStats:                      storageutils.SSTStats(t, sst, 0),
					SSTTimestampToRequestTimestamp: hlc.Timestamp{WallTime: tc.toReqTS},
					IngestAsWrites:                 tc.asWrites,
				},
			}, &kvpb.AddSSTableResponse{})
			require.NoError(t, err)

			if tc.asWrites {
				require.Nil(t, result.Replicated.AddSSTable)
			} else {
				require.NotNil(t, result.Replicated.AddSSTable)
				require.Equal(t, roachpb.Span{Key: start, EndKey: end}, result.Replicated.AddSSTable.Span)
				require.Equal(t, tc.toReqTS != 0, result.Replicated.AddSSTable.AtWriteTimestamp)
			}
			if tc.expectHistoryMutation {
				require.Equal(t, &kvserverpb.ReplicatedEvalResult_MVCCHistoryMutation{
					Spans: []roachpb.Span{{Key: start, EndKey: end}},
				}, result.Replicated.MVCCHistoryMutation)
				require.NotNil(t, result.Replicated.MVCCHistoryMutation)
			} else {
				require.Nil(t, result.Replicated.MVCCHistoryMutation)
			}
			require.Equal(t, tc.expectLogicalOps, opLogger.LogicalOps())
		})
	}
}

// TestDBAddSSTable tests application of an SST to a database, both in-memory
// and on disk.
func TestDBAddSSTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("store=in-memory", func(t *testing.T) {
		defer log.Scope(t).Close(t)
		ctx := context.Background()
		srv, _, db := serverutils.StartServer(t, base.TestServerArgs{})
		defer srv.Stopper().Stop(ctx)
		s := srv.ApplicationLayer()
		runTestDBAddSSTable(ctx, t, db, s, nil)
	})

	t.Run("store=on-disk", func(t *testing.T) {
		defer log.Scope(t).Close(t)
		ctx := context.Background()
		storeSpec := base.DefaultTestStoreSpec
		storeSpec.InMemory = false
		storeSpec.Path = t.TempDir()
		srv, _, db := serverutils.StartServer(t, base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{storeSpec},
		})
		defer srv.Stopper().Stop(ctx)
		s := srv.ApplicationLayer()

		store, err := srv.StorageLayer().GetStores().(*kvserver.Stores).GetStore(srv.GetFirstStoreID())
		require.NoError(t, err)

		runTestDBAddSSTable(ctx, t, db, s, store)
	})
}

// if store != nil, assume it is on-disk and check ingestion semantics.
func runTestDBAddSSTable(
	ctx context.Context,
	t *testing.T,
	db *kv.DB,
	srv serverutils.ApplicationLayerInterface,
	store *kvserver.Store,
) {
	tr := srv.TracerI().(*tracing.Tracer)
	tr.TestingRecordAsyncSpans() // we assert on async span traces in this test
	const ingestAsWrites, ingestAsSST = true, false
	const allowConflicts = false
	var allowShadowingBelow hlc.Timestamp
	var nilStats *enginepb.MVCCStats
	var noTS hlc.Timestamp
	cs := cluster.MakeTestingClusterSettings()

	k := func(s string) roachpb.Key {
		k, err := keys.RewriteKeyToTenantPrefix(roachpb.Key(s), srv.Codec().TenantPrefix())
		require.NoError(t, err)
		return k
	}

	{
		sst, start, end := storageutils.MakeSSTWithPrefix(t, cs, srv.Codec().TenantPrefix(), kvs{pointKV("bb", 2, "1")})

		// Key is before the range in the request span.
		_, _, err := db.AddSSTable(
			ctx, k("d"), k("e"), sst, allowConflicts, allowShadowingBelow, nilStats, ingestAsSST, noTS)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not in request range")

		// Key is after the range in the request span.
		_, _, err = db.AddSSTable(
			ctx, k("a"), k("b"), sst, allowConflicts, allowShadowingBelow, nilStats, ingestAsSST, noTS)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not in request range")

		// Do an initial ingest.
		ingestCtx, getRecAndFinish := tracing.ContextWithRecordingSpan(ctx, tr, "test-recording")
		defer getRecAndFinish()
		_, _, err = db.AddSSTable(
			ingestCtx, start, end, sst, allowConflicts, allowShadowingBelow, nilStats, ingestAsSST, noTS)
		require.NoError(t, err)
		trace := getRecAndFinish().String()
		require.Contains(t, trace, "evaluating AddSSTable")
		require.Contains(t, trace, "sideloadable proposal detected")
		require.Contains(t, trace, "ingested SSTable at index")

		if store != nil && srv.Codec().ForSystemTenant() {
			// If this request was made by the system tenant to an on-disk store, we
			// should have the un-redacted on-disk path to the file that was ingested;
			// Look for the ingested path and verify it still exists.
			re := regexp.MustCompile(`ingested SSTable at index \d+, term \d+: (\S+)`)
			match := re.FindStringSubmatch(trace)
			require.Len(t, match, 2, "failed to extract ingested path from message %q,\n got: %v", trace, match)

			// The on-disk paths have `.ingested` appended unlike in-memory.
			_, err = os.Stat(strings.TrimSuffix(match[1], ".ingested"))
			require.NoError(t, err, "%q file missing after ingest: %+v", match[1], err)
		}
		r, err := db.Get(ctx, k("bb"))
		require.NoError(t, err)
		require.Equal(t, []byte("1"), r.ValueBytes())
	}

	// Check that ingesting a key with an earlier mvcc timestamp doesn't affect
	// the value returned by Get.
	{
		sst, start, end := storageutils.MakeSSTWithPrefix(t, cs, srv.Codec().TenantPrefix(), kvs{pointKV("bb", 1, "2")})
		_, _, err := db.AddSSTable(
			ctx, start, end, sst, allowConflicts, allowShadowingBelow, nilStats, ingestAsSST, noTS)
		require.NoError(t, err)
		r, err := db.Get(ctx, k("bb"))
		require.NoError(t, err)
		require.Equal(t, []byte("1"), r.ValueBytes())
		if store != nil {
			require.EqualValues(t, 2, store.Metrics().AddSSTableApplications.Count())
		}
	}

	// Key range in request span is not empty. First time through a different
	// key is present. Second time through checks the idempotency.
	{
		sst, start, end := storageutils.MakeSSTWithPrefix(t, cs, srv.Codec().TenantPrefix(), kvs{pointKV("bc", 1, "3")})

		var before int64
		if store != nil {
			before = store.Metrics().AddSSTableApplicationCopies.Count()
		}
		for i := 0; i < 2; i++ {
			ingestCtx, getRecAndFinish := tracing.ContextWithRecordingSpan(ctx, tr, "test-recording")
			defer getRecAndFinish()

			_, _, err := db.AddSSTable(
				ingestCtx, start, end, sst, allowConflicts, allowShadowingBelow, nilStats, ingestAsSST, noTS)
			require.NoError(t, err)
			trace := getRecAndFinish().String()
			require.Contains(t, trace, "evaluating AddSSTable")
			require.Contains(t, trace, "sideloadable proposal detected")
			require.Contains(t, trace, "ingested SSTable at index")

			r, err := db.Get(ctx, k("bb"))
			require.NoError(t, err)
			require.Equal(t, []byte("1"), r.ValueBytes())

			r, err = db.Get(ctx, k("bc"))
			require.NoError(t, err)
			require.Equal(t, []byte("3"), r.ValueBytes())
		}
		if store != nil {
			require.EqualValues(t, 4, store.Metrics().AddSSTableApplications.Count())
			// The second time though we had to make a copy of the SST since rocks saw
			// existing data (from the first time), and rejected the no-modification
			// attempt.
			require.Equal(t, before, store.Metrics().AddSSTableApplicationCopies.Count())
		}
	}

	// ... and doing the same thing but via write-batch works the same.
	{
		sst, start, end := storageutils.MakeSSTWithPrefix(t, cs, srv.Codec().TenantPrefix(), kvs{pointKV("bd", 1, "3")})

		var before int64
		if store != nil {
			before = store.Metrics().AddSSTableApplications.Count()
		}
		for i := 0; i < 2; i++ {
			ingestCtx, getRecAndFinish := tracing.ContextWithRecordingSpan(ctx, tr, "test-recording")
			defer getRecAndFinish()

			_, _, err := db.AddSSTable(
				ingestCtx, start, end, sst, allowConflicts, allowShadowingBelow, nilStats, ingestAsWrites, noTS)
			require.NoError(t, err)
			trace := getRecAndFinish().String()
			require.Contains(t, trace, "evaluating AddSSTable")
			require.Contains(t, trace, "via regular write batch")

			r, err := db.Get(ctx, k("bb"))
			require.NoError(t, err)
			require.Equal(t, []byte("1"), r.ValueBytes())

			r, err = db.Get(ctx, k("bd"))
			require.NoError(t, err)
			require.Equal(t, []byte("3"), r.ValueBytes())
		}
		if store != nil {
			require.Equal(t, before, store.Metrics().AddSSTableApplications.Count())
		}
	}

	// Invalid key/value entry checksum.
	{
		key := storage.MVCCKey{Key: k("bb"), Timestamp: hlc.Timestamp{WallTime: 1}}
		value := roachpb.MakeValueFromString("1")
		value.InitChecksum([]byte("foo"))

		var sstFile bytes.Buffer
		w := storage.MakeTransportSSTWriter(ctx, cs, &sstFile)
		defer w.Close()
		require.NoError(t, w.Put(key, value.RawBytes))
		require.NoError(t, w.Finish())

		_, _, err := db.AddSSTable(
			ctx, k("b"), k("c"), sstFile.Bytes(), allowConflicts, allowShadowingBelow, nilStats, ingestAsSST, noTS)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid checksum")
	}
}

// TestAddSSTableMVCCStats tests that statistics are computed accurately.
func TestAddSSTableMVCCStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	storage.DisableMetamorphicSimpleValueEncoding(t)

	const max = 1 << 10
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := &batcheval.MockEvalCtx{
		ClusterSettings: st,
		MaxBytes:        max,
		Desc:            &roachpb.RangeDescriptor{},
	}

	engine := storage.NewDefaultInMemForTesting()
	defer engine.Close()

	for _, kv := range []storage.MVCCKeyValue{
		pointKV("A", 1, "A"),
		pointKV("a", 1, "a"),
		pointKV("a", 6, ""),
		pointKV("b", 5, "bb"),
		pointKV("c", 6, "ccccccccccccccccccccccccccccccccccccccccccccc"), // key 4b, 50b, live 64b
		pointKV("d", 1, "d"),
		pointKV("d", 2, ""),
		pointKV("e", 1, "e"),
		pointKV("u", 3, "u"),
		pointKV("z", 2, "zzzzzz"),
	} {
		require.NoError(t, engine.PutRawMVCC(kv.Key, kv.Value))
	}

	sst, start, end := storageutils.MakeSST(t, st, kvs{
		pointKV("a", 4, "aaaaaa"), // mvcc-shadowed by existing delete.
		pointKV("a", 2, "aa"),     // mvcc-shadowed within SST.
		pointKV("c", 6, "ccc"),    // same TS as existing, LSM-shadows existing.
		pointKV("d", 4, "dddd"),   // mvcc-shadow existing deleted d.
		pointKV("e", 4, "eeee"),   // mvcc-shadow existing 1b.
		pointKV("j", 2, "jj"),     // no colission – via MVCC or LSM – with existing.
		pointKV("t", 3, ""),       // tombstone, no collission
		pointKV("u", 5, ""),       // tombstone, shadows existing
	})
	statsDelta := enginepb.MVCCStats{
		// the sst will think it added 5 keys here, but a, c, e, and t shadow or are shadowed.
		LiveCount: -4,
		LiveBytes: -129,
		// the sst will think it added 5 keys, but only j and t are new so 5 are over-counted.
		KeyCount: -5,
		KeyBytes: -22,
		// the sst will think it added 6 values, but since one was a perfect (key+ts)
		// collision, it *replaced* the existing value and is over-counted.
		ValCount: -1,
		ValBytes: -50,
	}

	// After EvalAddSSTable, cArgs.Stats contains a diff to the existing
	// stats. Make sure recomputing from scratch gets the same answer as
	// applying the diff to the stats
	statsBefore := storageutils.EngineStats(t, engine, 0)
	ts := hlc.Timestamp{WallTime: 7}
	evalCtx.Stats = *statsBefore

	cArgs := batcheval.CommandArgs{
		EvalCtx: evalCtx.EvalContext(),
		Header: kvpb.Header{
			Timestamp: ts,
		},
		Args: &kvpb.AddSSTableRequest{
			RequestHeader: kvpb.RequestHeader{Key: start, EndKey: end},
			Data:          sst,
		},
		Stats: &enginepb.MVCCStats{},
	}
	var resp kvpb.AddSSTableResponse
	_, err := batcheval.EvalAddSSTable(ctx, engine, cArgs, &resp)
	require.NoError(t, err)

	require.NoError(t, fs.WriteFile(engine.Env(), "sst", sst, fs.UnspecifiedWriteCategory))
	require.NoError(t, engine.IngestLocalFiles(ctx, []string{"sst"}))

	statsEvaled := statsBefore
	statsEvaled.Add(*cArgs.Stats)
	statsEvaled.Add(statsDelta)
	statsEvaled.ContainsEstimates = 0

	newStats := storageutils.EngineStats(t, engine, statsEvaled.LastUpdateNanos)
	require.Equal(t, newStats, statsEvaled)

	// Check that actual remaining bytes equals the returned remaining bytes once
	// the delta for stats inaccuracy is applied.
	require.Equal(t, max-newStats.Total(), resp.AvailableBytes-statsDelta.Total())

	// Check stats for a single KV.
	sst, start, end = storageutils.MakeSST(t, st, kvs{pointKV("zzzzzzz", int(ts.WallTime), "zzz")})
	cArgs = batcheval.CommandArgs{
		EvalCtx: evalCtx.EvalContext(),
		Header:  kvpb.Header{Timestamp: ts},
		Args: &kvpb.AddSSTableRequest{
			RequestHeader: kvpb.RequestHeader{Key: start, EndKey: end},
			Data:          sst,
		},
		Stats: &enginepb.MVCCStats{},
	}
	_, err = batcheval.EvalAddSSTable(ctx, engine, cArgs, &kvpb.AddSSTableResponse{})
	require.NoError(t, err)
	require.Equal(t, enginepb.MVCCStats{
		ContainsEstimates: 1,
		LastUpdateNanos:   ts.WallTime,
		LiveBytes:         28,
		LiveCount:         1,
		KeyBytes:          20,
		KeyCount:          1,
		ValBytes:          8,
		ValCount:          1,
	}, *cArgs.Stats)
}

// TestAddSSTableIntentResolution tests that AddSSTable resolves
// intents of conflicting transactions.
func TestAddSSTableIntentResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, _, db := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(109427),
	})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	// Start a transaction that writes an intent at b.
	txn := db.NewTxn(ctx, "intent")
	require.NoError(t, txn.Put(ctx, "b", "intent"))

	// Generate an SSTable that covers keys a, b, and c, and submit it with high
	// priority. This is going to abort the transaction above, encounter its
	// intent, and resolve it.
	sst, start, end := storageutils.MakeSST(t, s.ClusterSettings(), kvs{
		pointKV("a", 1, "1"),
		pointKV("b", 1, "2"),
		pointKV("c", 1, "3"),
	})
	ba := &kvpb.BatchRequest{
		Header: kvpb.Header{UserPriority: roachpb.MaxUserPriority},
	}
	ba.Add(&kvpb.AddSSTableRequest{
		RequestHeader: kvpb.RequestHeader{Key: start, EndKey: end},
		Data:          sst,
		MVCCStats:     storageutils.SSTStats(t, sst, 0),
	})
	_, pErr := db.NonTransactionalSender().Send(ctx, ba)
	require.Nil(t, pErr)

	// The transaction should now be aborted.
	err := txn.Commit(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "TransactionRetryWithProtoRefreshError: TransactionAbortedError")
}

// TestAddSSTableSSTTimestampToRequestTimestampRespectsTSCache checks that AddSSTable
// with SSTTimestampToRequestTimestamp respects the timestamp cache.
func TestAddSSTableSSTTimestampToRequestTimestampRespectsTSCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, _, db := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(109427),
		Knobs:             base.TestingKnobs{},
	})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	// Write key.
	txn := db.NewTxn(ctx, "txn")
	require.NoError(t, txn.Put(ctx, "key", "txn"))
	require.NoError(t, txn.Commit(ctx))
	txnTS, err := txn.CommitTimestamp()
	require.NoError(t, err)

	// Add an SST writing below the previous write.
	sst, start, end := storageutils.MakeSST(t, s.ClusterSettings(), kvs{pointKV("key", 1, "sst")})
	sstReq := &kvpb.AddSSTableRequest{
		RequestHeader:                  kvpb.RequestHeader{Key: start, EndKey: end},
		Data:                           sst,
		MVCCStats:                      storageutils.SSTStats(t, sst, 0),
		SSTTimestampToRequestTimestamp: hlc.Timestamp{WallTime: 1},
	}
	ba := &kvpb.BatchRequest{
		Header: kvpb.Header{Timestamp: txnTS.Prev()},
	}
	ba.Add(sstReq)
	_, pErr := db.NonTransactionalSender().Send(ctx, ba)
	require.Nil(t, pErr)

	// Reading gets the value from the txn, because the tscache allowed writing
	// below the committed value.
	kv, err := db.Get(ctx, "key")
	require.NoError(t, err)
	require.Equal(t, "txn", string(kv.ValueBytes()))

	// Adding the SST again and reading results in the new value, because the
	// tscache pushed the SST forward.
	ba = &kvpb.BatchRequest{
		Header: kvpb.Header{Timestamp: txnTS.Prev()},
	}
	ba.Add(sstReq)
	_, pErr = db.NonTransactionalSender().Send(ctx, ba)
	require.Nil(t, pErr)

	kv, err = db.Get(ctx, "key")
	require.NoError(t, err)
	require.Equal(t, "sst", string(kv.ValueBytes()))
}

// TestAddSSTableSSTTimestampToRequestTimestampRespectsClosedTS checks that AddSSTable
// with SSTTimestampToRequestTimestamp respects the closed timestamp.
func TestAddSSTableSSTTimestampToRequestTimestampRespectsClosedTS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(109427),
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableCanAckBeforeApplication: true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	// Issue a write to trigger a closed timestamp.
	require.NoError(t, db.Put(ctx, "someKey", "someValue"))

	// Get the closed timestamp for the range owning "key".
	rd, err := s.LookupRange(roachpb.Key("key"))
	require.NoError(t, err)
	r, store, err := s.GetStores().(*kvserver.Stores).GetReplicaForRangeID(ctx, rd.RangeID)
	require.NoError(t, err)
	closedTS := r.GetCurrentClosedTimestamp(ctx)
	require.NotZero(t, closedTS)

	// Add an SST writing below the closed timestamp. It should get pushed above it.
	reqTS := closedTS.Prev()
	sst, start, end := storageutils.MakeSST(t, store.ClusterSettings(), kvs{pointKV("key", 1, "sst")})
	sstReq := &kvpb.AddSSTableRequest{
		RequestHeader:                  kvpb.RequestHeader{Key: start, EndKey: end},
		Data:                           sst,
		MVCCStats:                      storageutils.SSTStats(t, sst, 0),
		SSTTimestampToRequestTimestamp: hlc.Timestamp{WallTime: 1},
	}
	ba := &kvpb.BatchRequest{
		Header: kvpb.Header{Timestamp: reqTS},
	}
	ba.Add(sstReq)
	result, pErr := db.NonTransactionalSender().Send(ctx, ba)
	require.Nil(t, pErr)
	writeTS := result.Timestamp
	require.True(t, reqTS.Less(writeTS), "timestamp did not get pushed")
	require.True(t, closedTS.LessEq(writeTS), "timestamp %s below closed timestamp %s", result.Timestamp, closedTS)

	// Check that the value was in fact written at the write timestamp.
	kvs, err := storage.Scan(context.Background(), store.TODOEngine(), roachpb.Key("key"), roachpb.Key("key").Next(), 0)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, storage.MVCCKey{Key: roachpb.Key("key"), Timestamp: writeTS}, kvs[0].Key)
	mvccVal, err := storage.DecodeMVCCValue(kvs[0].Value)
	require.NoError(t, err)
	v, err := mvccVal.Value.GetBytes()
	require.NoError(t, err)
	require.Equal(t, "sst", string(v))
}
