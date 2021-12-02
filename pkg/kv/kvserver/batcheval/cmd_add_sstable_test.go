// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval_test

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type mvccKV struct {
	key   string
	ts    int64  // 0 for inline
	value string // "" for nil (tombstone)
}

func (kv mvccKV) Key() roachpb.Key         { return roachpb.Key(kv.key) }
func (kv mvccKV) TS() hlc.Timestamp        { return hlc.Timestamp{WallTime: kv.ts} }
func (kv mvccKV) MVCCKey() storage.MVCCKey { return storage.MVCCKey{Key: kv.Key(), Timestamp: kv.TS()} }
func (kv mvccKV) ValueBytes() []byte       { return kv.Value().RawBytes }

func (kv mvccKV) Value() roachpb.Value {
	value := roachpb.MakeValueFromString(kv.value)
	if kv.value == "" {
		value = roachpb.Value{}
	}
	value.InitChecksum(kv.Key())
	return value
}

// TestEvalAddSSTable tests EvalAddSSTable directly, using only an on-disk
// Pebble engine. This allows precise manipulation of timestamps.
func TestEvalAddSSTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const defaultReqTS = 10 // request sent with this timestamp by default
	const intentTS = 100    // values with this timestamp are written as intents

	// These are run with IngestAsWrites both disabled and enabled.
	testcases := map[string]struct {
		data           []mvccKV
		sst            []mvccKV
		atReqTS        int64 // WriteAtRequestTimestamp with given timestamp
		noConflict     bool  // DisallowConflicts
		noShadow       bool  // DisallowShadowing
		noShadowBelow  int64 // DisallowShadowingBelow
		expect         []mvccKV
		expectErr      interface{} // error type, substring, or true (any error)
		expectStatsEst bool        // expect MVCCStats.ContainsEstimates, don't check stats
	}{
		// Blind writes.
		"blind writes below existing": {
			data:           []mvccKV{{"a", 5, "a5"}, {"b", 7, ""}},
			sst:            []mvccKV{{"a", 3, "sst"}, {"b", 2, "sst"}},
			expect:         []mvccKV{{"a", 5, "a5"}, {"a", 3, "sst"}, {"b", 7, ""}, {"b", 2, "sst"}},
			expectStatsEst: true,
		},
		"blind replaces existing": {
			data:           []mvccKV{{"a", 2, "a2"}},
			sst:            []mvccKV{{"a", 2, "sst"}},
			expect:         []mvccKV{{"a", 2, "sst"}},
			expectStatsEst: true,
		},
		"blind returns WriteIntentError on conflict": {
			data:      []mvccKV{{"b", intentTS, "b0"}},
			sst:       []mvccKV{{"b", 1, "sst"}},
			expectErr: &roachpb.WriteIntentError{},
		},
		"blind returns WriteIntentError in span": {
			data:      []mvccKV{{"b", intentTS, "b0"}},
			sst:       []mvccKV{{"a", 1, "sst"}, {"c", 1, "sst"}},
			expectErr: &roachpb.WriteIntentError{},
		},
		"blind writes tombstones": { // unfortunately, for performance
			sst:            []mvccKV{{"a", 1, ""}},
			expect:         []mvccKV{{"a", 1, ""}},
			expectStatsEst: true,
		},
		"blind writes SST inline values": { // unfortunately, for performance
			sst:            []mvccKV{{"a", 0, "inline"}},
			expect:         []mvccKV{{"a", 0, "inline"}},
			expectStatsEst: true,
		},
		"blind writes above existing inline values": { // unfortunately, for performance
			data:           []mvccKV{{"a", 0, "inline"}},
			sst:            []mvccKV{{"a", 2, "sst"}},
			expect:         []mvccKV{{"a", 0, "inline"}, {"a", 2, "sst"}},
			expectStatsEst: true,
		},

		// WriteAtRequestTimestamp
		"WriteAtRequestTimestamp sets timestamp": {
			atReqTS:        10,
			sst:            []mvccKV{{"a", 1, "a1"}, {"b", 3, "b3"}},
			expect:         []mvccKV{{"a", 10, "a1"}, {"b", 10, "b3"}},
			expectStatsEst: true,
		},
		"WriteAtRequestTimestamp rejects tombstones": {
			atReqTS:   10,
			sst:       []mvccKV{{"a", 1, ""}},
			expectErr: "SST values cannot be tombstones",
		},
		"WriteAtRequestTimestamp rejects inline values": {
			atReqTS:   10,
			sst:       []mvccKV{{"a", 0, "inline"}},
			expectErr: "inline values or intents are not supported",
		},
		"WriteAtRequestTimestamp writes below and replaces": {
			atReqTS:        5,
			data:           []mvccKV{{"a", 5, "a5"}, {"b", 7, "b7"}},
			sst:            []mvccKV{{"a", 1, "sst"}, {"b", 1, "sst"}},
			expect:         []mvccKV{{"a", 5, "sst"}, {"b", 7, "b7"}, {"b", 5, "sst"}},
			expectStatsEst: true,
		},
		"WriteAtRequestTimestamp returns WriteIntentError for intents": {
			atReqTS:   10,
			data:      []mvccKV{{"a", intentTS, "intent"}},
			sst:       []mvccKV{{"a", 1, "a@1"}},
			expectErr: &roachpb.WriteIntentError{},
		},
		"WriteAtRequestTimestamp errors with DisallowConflicts below existing": {
			atReqTS:    5,
			noConflict: true,
			data:       []mvccKV{{"a", 5, "a5"}, {"b", 7, "b7"}},
			sst:        []mvccKV{{"a", 10, "sst"}, {"b", 10, "sst"}},
			expectErr:  &roachpb.WriteTooOldError{},
		},
		"WriteAtRequestTimestamp succeeds with DisallowConflicts above existing": {
			atReqTS:    8,
			noConflict: true,
			data:       []mvccKV{{"a", 5, "a5"}, {"b", 7, "b7"}},
			sst:        []mvccKV{{"a", 1, "sst"}, {"b", 1, "sst"}},
			expect:     []mvccKV{{"a", 8, "sst"}, {"a", 5, "a5"}, {"b", 8, "sst"}, {"b", 7, "b7"}},
		},
		"WriteAtRequestTimestamp errors with DisallowShadowing below existing": {
			atReqTS:   5,
			noShadow:  true,
			data:      []mvccKV{{"a", 5, "a5"}, {"b", 7, "b7"}},
			sst:       []mvccKV{{"a", 10, "sst"}, {"b", 10, "sst"}},
			expectErr: `ingested key collides with an existing one: "a"`,
		},
		"WriteAtRequestTimestamp errors with DisallowShadowing above existing": {
			atReqTS:   8,
			noShadow:  true,
			data:      []mvccKV{{"a", 5, "a5"}, {"b", 7, "b7"}},
			sst:       []mvccKV{{"a", 1, "sst"}, {"b", 1, "sst"}},
			expectErr: `ingested key collides with an existing one: "a"`,
		},
		"WriteAtRequestTimestamp succeeds with DisallowShadowing above tombstones": {
			atReqTS:  8,
			noShadow: true,
			data:     []mvccKV{{"a", 5, ""}, {"b", 7, ""}},
			sst:      []mvccKV{{"a", 1, "sst"}, {"b", 1, "sst"}},
			expect:   []mvccKV{{"a", 8, "sst"}, {"a", 5, ""}, {"b", 8, "sst"}, {"b", 7, ""}},
		},
		"WriteAtRequestTimestamp succeeds with DisallowShadowing and idempotent writes": {
			atReqTS:  5,
			noShadow: true,
			data:     []mvccKV{{"a", 5, "a5"}, {"b", 5, "b5"}},
			sst:      []mvccKV{{"a", 1, "a5"}, {"b", 1, "b5"}},
			expect:   []mvccKV{{"a", 5, "a5"}, {"b", 5, "b5"}},
		},
		"WriteAtRequestTimestamp errors with DisallowShadowingBelow equal value above existing below limit": {
			atReqTS:       7,
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 3, "a3"}},
			sst:           []mvccKV{{"a", 10, "a3"}},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"WriteAtRequestTimestamp errors with DisallowShadowingBelow errors above existing above limit": {
			atReqTS:       7,
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 6, "a6"}},
			sst:           []mvccKV{{"a", 10, "sst"}},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"WriteAtRequestTimestamp allows DisallowShadowingBelow equal value above existing above limit": {
			atReqTS:       7,
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 6, "a6"}},
			sst:           []mvccKV{{"a", 10, "a6"}},
			expect:        []mvccKV{{"a", 7, "a6"}, {"a", 6, "a6"}},
		},

		// DisallowConflicts
		"DisallowConflicts allows above and beside": {
			noConflict: true,
			data:       []mvccKV{{"a", 3, "a3"}, {"b", 1, ""}},
			sst:        []mvccKV{{"a", 4, "sst"}, {"b", 3, "sst"}, {"c", 1, "sst"}},
			expect: []mvccKV{
				{"a", 4, "sst"}, {"a", 3, "a3"}, {"b", 3, "sst"}, {"b", 1, ""}, {"c", 1, "sst"},
			},
		},
		"DisallowConflicts returns WriteTooOldError below existing": {
			noConflict: true,
			data:       []mvccKV{{"a", 3, "a3"}},
			sst:        []mvccKV{{"a", 2, "sst"}},
			expectErr:  &roachpb.WriteTooOldError{},
		},
		"DisallowConflicts returns WriteTooOldError at existing": {
			noConflict: true,
			data:       []mvccKV{{"a", 3, "a3"}},
			sst:        []mvccKV{{"a", 3, "sst"}},
			expectErr:  &roachpb.WriteTooOldError{},
		},
		"DisallowConflicts returns WriteTooOldError at existing tombstone": {
			noConflict: true,
			data:       []mvccKV{{"a", 3, ""}},
			sst:        []mvccKV{{"a", 3, "sst"}},
			expectErr:  &roachpb.WriteTooOldError{},
		},
		"DisallowConflicts returns WriteIntentError below intent": {
			noConflict: true,
			data:       []mvccKV{{"a", intentTS, "intent"}},
			sst:        []mvccKV{{"a", 3, "sst"}},
			expectErr:  &roachpb.WriteIntentError{},
		},
		"DisallowConflicts ignores intents in span": { // inconsistent with blind writes
			noConflict: true,
			data:       []mvccKV{{"b", intentTS, "intent"}},
			sst:        []mvccKV{{"a", 3, "sst"}, {"c", 3, "sst"}},
			expect:     []mvccKV{{"a", 3, "sst"}, {"b", intentTS, "intent"}, {"c", 3, "sst"}},
		},
		"DisallowConflicts is not idempotent": {
			noConflict: true,
			data:       []mvccKV{{"a", 3, "a3"}},
			sst:        []mvccKV{{"a", 3, "a3"}},
			expectErr:  &roachpb.WriteTooOldError{},
		},
		"DisallowConflicts allows new SST tombstones": { // unfortunately, for performance
			noConflict: true,
			sst:        []mvccKV{{"a", 3, ""}},
			expect:     []mvccKV{{"a", 3, ""}},
		},
		"DisallowConflicts rejects SST tombstones when shadowing": {
			noConflict: true,
			data:       []mvccKV{{"a", 2, "a2"}},
			sst:        []mvccKV{{"a", 3, ""}},
			expectErr:  "SST values cannot be tombstones",
		},
		"DisallowConflicts allows new SST inline values": { // unfortunately, for performance
			noConflict: true,
			sst:        []mvccKV{{"a", 0, "inline"}},
			expect:     []mvccKV{{"a", 0, "inline"}},
		},
		"DisallowConflicts rejects SST inline values when shadowing": {
			noConflict: true,
			data:       []mvccKV{{"a", 2, "a2"}},
			sst:        []mvccKV{{"a", 0, ""}},
			expectErr:  "SST keys must have timestamps",
		},
		"DisallowConflicts rejects existing inline values when shadowing": {
			noConflict: true,
			data:       []mvccKV{{"a", 0, "a0"}},
			sst:        []mvccKV{{"a", 3, "sst"}},
			expectErr:  "inline values are unsupported",
		},

		// DisallowShadowing
		"DisallowShadowing errors above existing": {
			noShadow:  true,
			data:      []mvccKV{{"a", 3, "a3"}},
			sst:       []mvccKV{{"a", 4, "sst"}},
			expectErr: `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowing errors below existing": {
			noShadow:  true,
			data:      []mvccKV{{"a", 3, "a3"}},
			sst:       []mvccKV{{"a", 2, "sst"}},
			expectErr: `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowing errors at existing": {
			noShadow:  true,
			data:      []mvccKV{{"a", 3, "a3"}},
			sst:       []mvccKV{{"a", 3, "sst"}},
			expectErr: `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowing returns WriteTooOldError at existing tombstone": {
			noShadow:  true,
			data:      []mvccKV{{"a", 3, ""}},
			sst:       []mvccKV{{"a", 3, "sst"}},
			expectErr: &roachpb.WriteTooOldError{},
		},
		"DisallowShadowing returns WriteTooOldError below existing tombstone": {
			noShadow:  true,
			data:      []mvccKV{{"a", 3, ""}},
			sst:       []mvccKV{{"a", 2, "sst"}},
			expectErr: &roachpb.WriteTooOldError{},
		},
		"DisallowShadowing allows above existing tombstone": {
			noShadow: true,
			data:     []mvccKV{{"a", 3, ""}},
			sst:      []mvccKV{{"a", 4, "sst"}},
			expect:   []mvccKV{{"a", 4, "sst"}, {"a", 3, ""}},
		},
		"DisallowShadowing returns WriteIntentError below intent": {
			noShadow:  true,
			data:      []mvccKV{{"a", intentTS, "intent"}},
			sst:       []mvccKV{{"a", 3, "sst"}},
			expectErr: &roachpb.WriteIntentError{},
		},
		"DisallowShadowing ignores intents in span": { // inconsistent with blind writes
			noShadow: true,
			data:     []mvccKV{{"b", intentTS, "intent"}},
			sst:      []mvccKV{{"a", 3, "sst"}, {"c", 3, "sst"}},
			expect:   []mvccKV{{"a", 3, "sst"}, {"b", intentTS, "intent"}, {"c", 3, "sst"}},
		},
		"DisallowShadowing is idempotent": {
			noShadow: true,
			data:     []mvccKV{{"a", 3, "a3"}},
			sst:      []mvccKV{{"a", 3, "a3"}},
			expect:   []mvccKV{{"a", 3, "a3"}},
		},
		"DisallowShadowing allows new SST tombstones": { // unfortunately, for performance
			noShadow: true,
			sst:      []mvccKV{{"a", 3, ""}},
			expect:   []mvccKV{{"a", 3, ""}},
		},
		"DisallowShadowing rejects SST tombstones when shadowing": {
			noShadow:  true,
			data:      []mvccKV{{"a", 2, "a2"}},
			sst:       []mvccKV{{"a", 3, ""}},
			expectErr: "SST values cannot be tombstones",
		},
		"DisallowShadowing allows new SST inline values": { // unfortunately, for performance
			noShadow: true,
			sst:      []mvccKV{{"a", 0, "inline"}},
			expect:   []mvccKV{{"a", 0, "inline"}},
		},
		"DisallowShadowing rejects SST inline values when shadowing": {
			noShadow:  true,
			data:      []mvccKV{{"a", 2, "a2"}},
			sst:       []mvccKV{{"a", 0, "inline"}},
			expectErr: "SST keys must have timestamps",
		},
		"DisallowShadowing rejects existing inline values when shadowing": {
			noShadow:  true,
			data:      []mvccKV{{"a", 0, "a0"}},
			sst:       []mvccKV{{"a", 3, "sst"}},
			expectErr: "inline values are unsupported",
		},
		"DisallowShadowing collision SST start, existing start, above": {
			noShadow:  true,
			data:      []mvccKV{{"a", 2, "a2"}},
			sst:       []mvccKV{{"a", 7, "sst"}},
			expectErr: `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowing collision SST start, existing middle, below": {
			noShadow:  true,
			data:      []mvccKV{{"a", 2, "a2"}, {"a", 1, "a1"}, {"b", 2, "b2"}, {"c", 3, "c3"}},
			sst:       []mvccKV{{"b", 1, "sst"}},
			expectErr: `ingested key collides with an existing one: "b"`,
		},
		"DisallowShadowing collision SST end, existing end, above": {
			noShadow:  true,
			data:      []mvccKV{{"a", 2, "a2"}, {"a", 1, "a1"}, {"b", 2, "b2"}, {"d", 3, "d3"}},
			sst:       []mvccKV{{"c", 3, "sst"}, {"d", 4, "sst"}},
			expectErr: `ingested key collides with an existing one: "d"`,
		},
		"DisallowShadowing collision after write above tombstone": {
			noShadow:  true,
			data:      []mvccKV{{"a", 2, ""}, {"a", 1, "a1"}, {"b", 2, "b2"}},
			sst:       []mvccKV{{"a", 3, "sst"}, {"b", 1, "sst"}},
			expectErr: `ingested key collides with an existing one: "b"`,
		},

		// DisallowShadowingBelow
		"DisallowShadowingBelow cannot be used with DisallowShadowing": {
			noShadow:      true,
			noShadowBelow: 5,
			sst:           []mvccKV{{"a", 1, "sst"}},
			expectErr:     `cannot set both DisallowShadowing and DisallowShadowingBelow`,
		},
		"DisallowShadowingBelow errors above existing": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 3, "a3"}},
			sst:           []mvccKV{{"a", 4, "sst"}},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow errors below existing": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 3, "a3"}},
			sst:           []mvccKV{{"a", 2, "sst"}},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow errors at existing": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 3, "a3"}},
			sst:           []mvccKV{{"a", 3, "sst"}},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow returns WriteTooOldError at existing tombstone": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 3, ""}},
			sst:           []mvccKV{{"a", 3, "sst"}},
			expectErr:     &roachpb.WriteTooOldError{},
		},
		"DisallowShadowingBelow returns WriteTooOldError below existing tombstone": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 3, ""}},
			sst:           []mvccKV{{"a", 2, "sst"}},
			expectErr:     &roachpb.WriteTooOldError{},
		},
		"DisallowShadowingBelow allows above existing tombstone": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 3, ""}},
			sst:           []mvccKV{{"a", 4, "sst"}},
			expect:        []mvccKV{{"a", 4, "sst"}, {"a", 3, ""}},
		},
		"DisallowShadowingBelow returns WriteIntentError below intent": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", intentTS, "intent"}},
			sst:           []mvccKV{{"a", 3, "sst"}},
			expectErr:     &roachpb.WriteIntentError{},
		},
		"DisallowShadowingBelow ignores intents in span": { // inconsistent with blind writes
			noShadowBelow: 5,
			data:          []mvccKV{{"b", intentTS, "intent"}},
			sst:           []mvccKV{{"a", 3, "sst"}, {"c", 3, "sst"}},
			expect:        []mvccKV{{"a", 3, "sst"}, {"b", intentTS, "intent"}, {"c", 3, "sst"}},
		},
		"DisallowShadowingBelow is not generally idempotent": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 3, "a3"}},
			sst:           []mvccKV{{"a", 3, "a3"}},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow allows new SST tombstones": { // unfortunately, for performance
			noShadowBelow: 5,
			sst:           []mvccKV{{"a", 3, ""}},
			expect:        []mvccKV{{"a", 3, ""}},
		},
		"DisallowShadowingBelow rejects SST tombstones when shadowing": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 2, "a2"}},
			sst:           []mvccKV{{"a", 3, ""}},
			expectErr:     "SST values cannot be tombstones",
		},
		"DisallowShadowingBelow allows new SST inline values": { // unfortunately, for performance
			noShadowBelow: 5,
			sst:           []mvccKV{{"a", 0, "inline"}},
			expect:        []mvccKV{{"a", 0, "inline"}},
		},
		"DisallowShadowingBelow rejects SST inline values when shadowing": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 2, "a2"}},
			sst:           []mvccKV{{"a", 0, "inline"}},
			expectErr:     "SST keys must have timestamps",
		},
		"DisallowShadowingBelow rejects existing inline values when shadowing": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 0, "a0"}},
			sst:           []mvccKV{{"a", 3, "sst"}},
			expectErr:     "inline values are unsupported",
		},
		"DisallowShadowingBelow collision SST start, existing start, above": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 2, "a2"}},
			sst:           []mvccKV{{"a", 7, "sst"}},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow collision SST start, existing middle, below": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 2, "a2"}, {"a", 1, "a1"}, {"b", 2, "b2"}, {"c", 3, "c3"}},
			sst:           []mvccKV{{"b", 1, "sst"}},
			expectErr:     `ingested key collides with an existing one: "b"`,
		},
		"DisallowShadowingBelow collision SST end, existing end, above": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 2, "a2"}, {"a", 1, "a1"}, {"b", 2, "b2"}, {"d", 3, "d3"}},
			sst:           []mvccKV{{"c", 3, "sst"}, {"d", 4, "sst"}},
			expectErr:     `ingested key collides with an existing one: "d"`,
		},
		"DisallowShadowingBelow collision after write above tombstone": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 2, ""}, {"a", 1, "a1"}, {"b", 2, "b2"}},
			sst:           []mvccKV{{"a", 3, "sst"}, {"b", 1, "sst"}},
			expectErr:     `ingested key collides with an existing one: "b"`,
		},
		"DisallowShadowingBelow at limit writes": {
			noShadowBelow: 5,
			sst:           []mvccKV{{"a", 5, "sst"}},
			expect:        []mvccKV{{"a", 5, "sst"}},
		},
		"DisallowShadowingBelow at limit errors above existing": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 3, "a3"}},
			sst:           []mvccKV{{"a", 5, "sst"}},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow at limit errors above existing with same value": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 3, "a3"}},
			sst:           []mvccKV{{"a", 5, "a3"}},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow at limit errors on replacing": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 5, "a3"}},
			sst:           []mvccKV{{"a", 5, "sst"}},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow at limit is idempotent": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 5, "a3"}},
			sst:           []mvccKV{{"a", 5, "a3"}},
			expect:        []mvccKV{{"a", 5, "a3"}},
		},
		"DisallowShadowingBelow above limit writes": {
			noShadowBelow: 5,
			sst:           []mvccKV{{"a", 7, "sst"}},
			expect:        []mvccKV{{"a", 7, "sst"}},
		},
		"DisallowShadowingBelow above limit errors on existing below limit": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 4, "a4"}},
			sst:           []mvccKV{{"a", 7, "sst"}},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow above limit errors on existing below limit with same value": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 4, "a4"}},
			sst:           []mvccKV{{"a", 7, "a3"}},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow above limit errors on existing at limit": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 5, "a5"}},
			sst:           []mvccKV{{"a", 7, "sst"}},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow above limit allows equal value at limit": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 5, "a5"}},
			sst:           []mvccKV{{"a", 7, "a5"}},
			expect:        []mvccKV{{"a", 7, "a5"}, {"a", 5, "a5"}},
		},
		"DisallowShadowingBelow above limit errors on existing above limit": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 6, "a6"}},
			sst:           []mvccKV{{"a", 7, "sst"}},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow above limit allows equal value above limit": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 6, "a6"}},
			sst:           []mvccKV{{"a", 7, "a6"}},
			expect:        []mvccKV{{"a", 7, "a6"}, {"a", 6, "a6"}},
		},
		"DisallowShadowingBelow above limit errors on replacing": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 7, "a7"}},
			sst:           []mvccKV{{"a", 7, "sst"}},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow above limit is idempotent": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 7, "a7"}},
			sst:           []mvccKV{{"a", 7, "a7"}},
			expect:        []mvccKV{{"a", 7, "a7"}},
		},
		"DisallowShadowingBelow above limit errors below existing": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 8, "a8"}},
			sst:           []mvccKV{{"a", 7, "sst"}},
			expectErr:     `ingested key collides with an existing one: "a"`,
		},
		"DisallowShadowingBelow above limit errors below existing with same value": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 8, "a8"}},
			sst:           []mvccKV{{"a", 7, "a8"}},
			expectErr:     &roachpb.WriteTooOldError{},
		},
		"DisallowShadowingBelow above limit errors below tombstone": {
			noShadowBelow: 5,
			data:          []mvccKV{{"a", 8, ""}},
			sst:           []mvccKV{{"a", 7, "a8"}},
			expectErr:     &roachpb.WriteTooOldError{},
		},
	}
	testutils.RunTrueAndFalse(t, "IngestAsWrites", func(t *testing.T, ingestAsWrites bool) {
		for name, tc := range testcases {
			t.Run(name, func(t *testing.T) {
				st := cluster.MakeTestingClusterSettings()
				ctx := context.Background()

				dir := t.TempDir()
				engine, err := storage.Open(ctx, storage.Filesystem(filepath.Join(dir, "db")))
				require.NoError(t, err)
				defer engine.Close()

				// Write initial data.
				intentTxn := roachpb.MakeTransaction("intentTxn", nil, 0, hlc.Timestamp{WallTime: intentTS}, 0, 1)
				b := engine.NewBatch()
				for i := len(tc.data) - 1; i >= 0; i-- { // reverse, older timestamps first
					kv := tc.data[i]
					var txn *roachpb.Transaction
					if kv.ts == intentTS {
						txn = &intentTxn
					}
					require.NoError(t, storage.MVCCPut(ctx, b, nil, kv.Key(), kv.TS(), kv.Value(), txn))
				}
				require.NoError(t, b.Commit(false))
				stats := engineStats(t, engine)

				// Build and add SST.
				sst, start, end := makeSST(t, tc.sst)
				reqTS := hlc.Timestamp{WallTime: defaultReqTS}
				if tc.atReqTS != 0 {
					reqTS.WallTime = tc.atReqTS
				}
				resp := &roachpb.AddSSTableResponse{}
				result, err := batcheval.EvalAddSSTable(ctx, engine, batcheval.CommandArgs{
					EvalCtx: (&batcheval.MockEvalCtx{ClusterSettings: st}).EvalContext(),
					Stats:   stats,
					Header: roachpb.Header{
						Timestamp: reqTS,
					},
					Args: &roachpb.AddSSTableRequest{
						RequestHeader:           roachpb.RequestHeader{Key: start, EndKey: end},
						Data:                    sst,
						MVCCStats:               sstStats(t, sst),
						DisallowConflicts:       tc.noConflict,
						DisallowShadowing:       tc.noShadow,
						DisallowShadowingBelow:  hlc.Timestamp{WallTime: tc.noShadowBelow},
						WriteAtRequestTimestamp: tc.atReqTS != 0,
						IngestAsWrites:          ingestAsWrites,
					},
				}, resp)

				if tc.expectErr != nil {
					require.Error(t, err)
					if b, ok := tc.expectErr.(bool); ok && b {
						// any error is fine
					} else if expectMsg, ok := tc.expectErr.(string); ok {
						require.Contains(t, err.Error(), expectMsg)
					} else if expectErr, ok := tc.expectErr.(error); ok {
						require.True(t, errors.HasType(err, expectErr), "expected %T, got %v", expectErr, err)
					} else {
						require.Fail(t, "invalid expectErr", "expectErr=%v", tc.expectErr)
					}
					return
				}
				require.NoError(t, err)

				if ingestAsWrites {
					require.Nil(t, result.Replicated.AddSSTable)
				} else {
					require.NotNil(t, result.Replicated.AddSSTable)
					sstPath := filepath.Join(dir, "sst")
					require.NoError(t, engine.WriteFile(sstPath, result.Replicated.AddSSTable.Data))
					require.NoError(t, engine.IngestExternalFiles(ctx, []string{sstPath}))
				}

				// Scan resulting data from engine.
				iter := storage.NewMVCCIncrementalIterator(engine, storage.MVCCIncrementalIterOptions{
					EndKey:       keys.MaxKey,
					StartTime:    hlc.MinTimestamp,
					EndTime:      hlc.MaxTimestamp,
					IntentPolicy: storage.MVCCIncrementalIterIntentPolicyEmit,
					InlinePolicy: storage.MVCCIncrementalIterInlinePolicyEmit,
				})
				defer iter.Close()
				iter.SeekGE(storage.MVCCKey{Key: keys.SystemPrefix})
				scan := []mvccKV{}
				for {
					ok, err := iter.Valid()
					require.NoError(t, err)
					if !ok {
						break
					}
					key := string(iter.Key().Key)
					ts := iter.Key().Timestamp.WallTime
					var value []byte
					if iter.Key().IsValue() {
						if len(iter.Value()) > 0 {
							value, err = roachpb.Value{RawBytes: iter.Value()}.GetBytes()
							require.NoError(t, err)
						}
					} else {
						var meta enginepb.MVCCMetadata
						require.NoError(t, protoutil.Unmarshal(iter.UnsafeValue(), &meta))
						if meta.RawBytes == nil {
							// Skip intent metadata records (value emitted separately).
							iter.Next()
							continue
						}
						value, err = roachpb.Value{RawBytes: meta.RawBytes}.GetBytes()
						require.NoError(t, err)
					}
					scan = append(scan, mvccKV{key: key, ts: ts, value: string(value)})
					iter.Next()
				}
				require.Equal(t, tc.expect, scan)

				// Check that stats were updated correctly.
				if tc.expectStatsEst {
					require.True(t, stats.ContainsEstimates > 0, "expected stats to be estimated")
				} else {
					require.False(t, stats.ContainsEstimates > 0, "found estimated stats")
					stats.LastUpdateNanos = 0 // avoid spurious diffs
					require.Equal(t, stats, engineStats(t, engine))
				}
			})
		}
	})
}

// TestDBAddSSTable tests application of an SST to a database, both in-memory
// and on disk.
func TestDBAddSSTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("store=in-memory", func(t *testing.T) {
		ctx := context.Background()
		s, _, db := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
		defer s.Stopper().Stop(ctx)
		tr := s.TracerI().(*tracing.Tracer)
		runTestDBAddSSTable(ctx, t, db, tr, nil)
	})

	t.Run("store=on-disk", func(t *testing.T) {
		ctx := context.Background()
		storeSpec := base.DefaultTestStoreSpec
		storeSpec.InMemory = false
		storeSpec.Path = t.TempDir()
		s, _, db := serverutils.StartServer(t, base.TestServerArgs{
			Insecure:   true,
			StoreSpecs: []base.StoreSpec{storeSpec},
		})
		defer s.Stopper().Stop(ctx)
		tr := s.TracerI().(*tracing.Tracer)
		store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
		require.NoError(t, err)
		runTestDBAddSSTable(ctx, t, db, tr, store)
	})
}

// if store != nil, assume it is on-disk and check ingestion semantics.
func runTestDBAddSSTable(
	ctx context.Context, t *testing.T, db *kv.DB, tr *tracing.Tracer, store *kvserver.Store,
) {
	tr.TestingRecordAsyncSpans() // we assert on async span traces in this test
	const ingestAsWrites, ingestAsSST = true, false
	const writeAtSST = false
	const allowConflicts = false
	const allowShadowing = false
	var allowShadowingBelow hlc.Timestamp
	var nilStats *enginepb.MVCCStats
	var noTS hlc.Timestamp

	{
		sst, start, end := makeSST(t, []mvccKV{{"bb", 2, "1"}})

		// Key is before the range in the request span.
		err := db.AddSSTable(
			ctx, "d", "e", sst, allowConflicts, allowShadowing, allowShadowingBelow, nilStats, ingestAsSST, noTS, writeAtSST)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not in request range")

		// Key is after the range in the request span.
		err = db.AddSSTable(
			ctx, "a", "b", sst, allowConflicts, allowShadowing, allowShadowingBelow, nilStats, ingestAsSST, noTS, writeAtSST)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not in request range")

		// Do an initial ingest.
		ingestCtx, getRecAndFinish := tracing.ContextWithRecordingSpan(ctx, tr, "test-recording")
		defer getRecAndFinish()
		require.NoError(t, db.AddSSTable(
			ingestCtx, start, end, sst, allowConflicts, allowShadowing, allowShadowingBelow, nilStats, ingestAsSST, noTS, writeAtSST))
		trace := getRecAndFinish().String()
		require.Contains(t, trace, "evaluating AddSSTable")
		require.Contains(t, trace, "sideloadable proposal detected")
		require.Contains(t, trace, "ingested SSTable at index")

		if store != nil {
			// Look for the ingested path and verify it still exists.
			re := regexp.MustCompile(`ingested SSTable at index \d+, term \d+: (\S+)`)
			match := re.FindStringSubmatch(trace)
			require.Len(t, match, 2, "failed to extract ingested path from message %q,\n got: %v", trace, match)

			// The on-disk paths have `.ingested` appended unlike in-memory.
			_, err = os.Stat(strings.TrimSuffix(match[1], ".ingested"))
			require.NoError(t, err, "%q file missing after ingest: %+v", match[1], err)
		}
		r, err := db.Get(ctx, "bb")
		require.NoError(t, err)
		require.Equal(t, []byte("1"), r.ValueBytes())
	}

	// Check that ingesting a key with an earlier mvcc timestamp doesn't affect
	// the value returned by Get.
	{
		sst, start, end := makeSST(t, []mvccKV{{"bb", 1, "2"}})
		require.NoError(t, db.AddSSTable(
			ctx, start, end, sst, allowConflicts, allowShadowing, allowShadowingBelow, nilStats, ingestAsSST, noTS, writeAtSST))
		r, err := db.Get(ctx, "bb")
		require.NoError(t, err)
		require.Equal(t, []byte("1"), r.ValueBytes())
		if store != nil {
			require.EqualValues(t, 2, store.Metrics().AddSSTableApplications.Count())
		}
	}

	// Key range in request span is not empty. First time through a different
	// key is present. Second time through checks the idempotency.
	{
		sst, start, end := makeSST(t, []mvccKV{{"bc", 1, "3"}})

		var before int64
		if store != nil {
			before = store.Metrics().AddSSTableApplicationCopies.Count()
		}
		for i := 0; i < 2; i++ {
			ingestCtx, getRecAndFinish := tracing.ContextWithRecordingSpan(ctx, tr, "test-recording")
			defer getRecAndFinish()

			require.NoError(t, db.AddSSTable(
				ingestCtx, start, end, sst, allowConflicts, allowShadowing, allowShadowingBelow, nilStats, ingestAsSST, noTS, writeAtSST))
			trace := getRecAndFinish().String()
			require.Contains(t, trace, "evaluating AddSSTable")
			require.Contains(t, trace, "sideloadable proposal detected")
			require.Contains(t, trace, "ingested SSTable at index")

			r, err := db.Get(ctx, "bb")
			require.NoError(t, err)
			require.Equal(t, []byte("1"), r.ValueBytes())

			r, err = db.Get(ctx, "bc")
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
		sst, start, end := makeSST(t, []mvccKV{{"bd", 1, "3"}})

		var before int64
		if store != nil {
			before = store.Metrics().AddSSTableApplications.Count()
		}
		for i := 0; i < 2; i++ {
			ingestCtx, getRecAndFinish := tracing.ContextWithRecordingSpan(ctx, tr, "test-recording")
			defer getRecAndFinish()

			require.NoError(t, db.AddSSTable(
				ingestCtx, start, end, sst, allowConflicts, allowShadowing, allowShadowingBelow, nilStats, ingestAsWrites, noTS, writeAtSST))
			trace := getRecAndFinish().String()
			require.Contains(t, trace, "evaluating AddSSTable")
			require.Contains(t, trace, "via regular write batch")

			r, err := db.Get(ctx, "bb")
			require.NoError(t, err)
			require.Equal(t, []byte("1"), r.ValueBytes())

			r, err = db.Get(ctx, "bd")
			require.NoError(t, err)
			require.Equal(t, []byte("3"), r.ValueBytes())
		}
		if store != nil {
			require.Equal(t, before, store.Metrics().AddSSTableApplications.Count())
		}
	}

	// Invalid key/value entry checksum.
	{
		key := storage.MVCCKey{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 1}}
		value := roachpb.MakeValueFromString("1")
		value.InitChecksum([]byte("foo"))

		sstFile := &storage.MemFile{}
		w := storage.MakeBackupSSTWriter(sstFile)
		defer w.Close()
		require.NoError(t, w.Put(key, value.RawBytes))
		require.NoError(t, w.Finish())

		err := db.AddSSTable(
			ctx, "b", "c", sstFile.Data(), allowConflicts, allowShadowing, allowShadowingBelow, nilStats, ingestAsSST, noTS, writeAtSST)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid checksum")
	}
}

// TestAddSSTableMVCCStats tests that statistics are computed accurately.
func TestAddSSTableMVCCStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := (&batcheval.MockEvalCtx{ClusterSettings: st}).EvalContext()

	dir := t.TempDir()
	engine, err := storage.Open(ctx, storage.Filesystem(filepath.Join(dir, "db")))
	require.NoError(t, err)
	defer engine.Close()

	for _, kv := range []mvccKV{
		{"A", 1, "A"},
		{"a", 1, "a"},
		{"a", 6, ""},
		{"b", 5, "bb"},
		{"c", 6, "ccccccccccccccccccccccccccccccccccccccccccccc"}, // key 4b, 50b, live 64b
		{"d", 1, "d"},
		{"d", 2, ""},
		{"e", 1, "e"},
		{"z", 2, "zzzzzz"},
	} {
		require.NoError(t, engine.PutMVCC(kv.MVCCKey(), kv.ValueBytes()))
	}

	sst, start, end := makeSST(t, []mvccKV{
		{"a", 4, "aaaaaa"}, // mvcc-shadowed by existing delete.
		{"a", 2, "aa"},     // mvcc-shadowed within SST.
		{"c", 6, "ccc"},    // same TS as existing, LSM-shadows existing.
		{"d", 4, "dddd"},   // mvcc-shadow existing deleted d.
		{"e", 4, "eeee"},   // mvcc-shadow existing 1b.
		{"j", 2, "jj"},     // no colission – via MVCC or LSM – with existing.
	})
	statsDelta := enginepb.MVCCStats{
		// the sst will think it added 4 keys here, but a, c, and e shadow or are shadowed.
		LiveCount: -3,
		LiveBytes: -109,
		// the sst will think it added 5 keys, but only j is new so 4 are over-counted.
		KeyCount: -4,
		KeyBytes: -20,
		// the sst will think it added 6 values, but since one was a perfect (key+ts)
		// collision, it *replaced* the existing value and is over-counted.
		ValCount: -1,
		ValBytes: -50,
	}

	// After EvalAddSSTable, cArgs.Stats contains a diff to the existing
	// stats. Make sure recomputing from scratch gets the same answer as
	// applying the diff to the stats
	statsBefore := engineStats(t, engine)
	ts := hlc.Timestamp{WallTime: 7}
	cArgs := batcheval.CommandArgs{
		EvalCtx: evalCtx,
		Header: roachpb.Header{
			Timestamp: ts,
		},
		Args: &roachpb.AddSSTableRequest{
			RequestHeader: roachpb.RequestHeader{Key: start, EndKey: end},
			Data:          sst,
		},
		Stats: &enginepb.MVCCStats{},
	}
	_, err = batcheval.EvalAddSSTable(ctx, engine, cArgs, nil)
	require.NoError(t, err)

	sstPath := filepath.Join(dir, "sst")
	require.NoError(t, engine.WriteFile(sstPath, sst))
	require.NoError(t, engine.IngestExternalFiles(ctx, []string{sstPath}))

	statsEvaled := statsBefore
	statsEvaled.Add(*cArgs.Stats)
	statsEvaled.Add(statsDelta)
	statsEvaled.ContainsEstimates = 0
	statsEvaled.LastUpdateNanos = 0
	require.Equal(t, engineStats(t, engine), statsEvaled)

	// Check stats for a single KV.
	sst, start, end = makeSST(t, []mvccKV{{"zzzzzzz", ts.WallTime, "zzz"}})
	cArgsWithStats := batcheval.CommandArgs{
		EvalCtx: evalCtx,
		Header:  roachpb.Header{Timestamp: ts},
		Args: &roachpb.AddSSTableRequest{
			RequestHeader: roachpb.RequestHeader{Key: start, EndKey: end},
			Data:          sst,
			MVCCStats:     &enginepb.MVCCStats{KeyCount: 10},
		},
		Stats: &enginepb.MVCCStats{},
	}
	_, err = batcheval.EvalAddSSTable(ctx, engine, cArgsWithStats, nil)
	require.NoError(t, err)
	require.Equal(t, enginepb.MVCCStats{ContainsEstimates: 1, KeyCount: 10}, *cArgsWithStats.Stats)
}

// TestAddSSTableMVCCStatsDisallowShadowing tests that stats are computed
// accurately when DisallowShadowing is set.
func TestAddSSTableMVCCStatsDisallowShadowing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := (&batcheval.MockEvalCtx{ClusterSettings: st}).EvalContext()

	engine := storage.NewDefaultInMemForTesting()
	defer engine.Close()

	for _, kv := range []mvccKV{
		{"a", 2, "aa"},
		{"b", 1, "bb"},
		{"b", 6, ""},
		{"g", 5, "gg"},
		{"r", 1, "rr"},
		{"y", 1, "yy"},
		{"y", 2, ""},
		{"y", 5, "yyy"},
		{"z", 2, "zz"},
	} {
		require.NoError(t, engine.PutMVCC(kv.MVCCKey(), kv.ValueBytes()))
	}

	// This test ensures accuracy of MVCCStats in the situation that successive
	// SSTs being ingested via AddSSTable have "perfectly shadowing" keys (same ts
	// and value). Such KVs are not considered as collisions and so while they are
	// skipped during ingestion, their stats would previously be double counted.
	// To mitigate this problem we now return the stats of such skipped KVs while
	// evaluating the AddSSTable command, and accumulate accurate stats in the
	// CommandArgs Stats field by using:
	// cArgs.Stats + ingested_stats - skipped_stats.
	// Successfully evaluate the first SST as there are no key collisions.
	kvs := []mvccKV{
		{"c", 2, "bb"},
		{"h", 6, "hh"},
	}
	sst, start, end := makeSST(t, kvs)

	// Accumulate stats across SST ingestion.
	commandStats := enginepb.MVCCStats{}

	cArgs := batcheval.CommandArgs{
		EvalCtx: evalCtx,
		Header: roachpb.Header{
			Timestamp: hlc.Timestamp{WallTime: 7},
		},
		Args: &roachpb.AddSSTableRequest{
			RequestHeader:     roachpb.RequestHeader{Key: start, EndKey: end},
			Data:              sst,
			DisallowShadowing: true,
			MVCCStats:         sstStats(t, sst),
		},
		Stats: &commandStats,
	}
	_, err := batcheval.EvalAddSSTable(ctx, engine, cArgs, nil)
	require.NoError(t, err)
	firstSSTStats := commandStats

	// Insert KV entries so that we can correctly identify keys to skip when
	// ingesting the perfectly shadowing KVs (same ts and same value) in the
	// second SST.
	for _, kv := range kvs {
		require.NoError(t, engine.PutMVCC(kv.MVCCKey(), kv.ValueBytes()))
	}

	// Evaluate the second SST. Both the KVs are perfectly shadowing and should
	// not contribute to the stats.
	sst, start, end = makeSST(t, []mvccKV{
		{"c", 2, "bb"}, // key has the same timestamp and value as the one present in the existing data.
		{"h", 6, "hh"}, // key has the same timestamp and value as the one present in the existing data.
	})

	cArgs.Args = &roachpb.AddSSTableRequest{
		RequestHeader:     roachpb.RequestHeader{Key: start, EndKey: end},
		Data:              sst,
		DisallowShadowing: true,
		MVCCStats:         sstStats(t, sst),
	}
	_, err = batcheval.EvalAddSSTable(ctx, engine, cArgs, nil)
	require.NoError(t, err)

	// Check that there has been no double counting of stats. All keys in second SST are shadowing.
	require.Equal(t, firstSSTStats, *cArgs.Stats)

	// Evaluate the third SST. Two of the three KVs are perfectly shadowing, but
	// there is one valid KV which should contribute to the stats.
	sst, start, end = makeSST(t, []mvccKV{
		{"c", 2, "bb"}, // key has the same timestamp and value as the one present in the existing data.
		{"e", 2, "ee"},
		{"h", 6, "hh"}, // key has the same timestamp and value as the one present in the existing data.
	})

	cArgs.Args = &roachpb.AddSSTableRequest{
		RequestHeader:     roachpb.RequestHeader{Key: start, EndKey: end},
		Data:              sst,
		DisallowShadowing: true,
		MVCCStats:         sstStats(t, sst),
	}
	_, err = batcheval.EvalAddSSTable(ctx, engine, cArgs, nil)
	require.NoError(t, err)

	// This is the stats contribution of the KV {"e", 2, "ee"}. This should be
	// the only addition to the cumulative stats, as the other two KVs are
	// perfect shadows of existing data.
	delta := enginepb.MVCCStats{
		LiveCount: 1,
		LiveBytes: 21,
		KeyCount:  1,
		KeyBytes:  14,
		ValCount:  1,
		ValBytes:  7,
	}

	// Check that there has been no double counting of stats.
	firstSSTStats.Add(delta)
	require.Equal(t, firstSSTStats, *cArgs.Stats)
}

// TestAddSSTableIntentResolution tests that AddSSTable resolves
// intents of conflicting transactions.
func TestAddSSTableIntentResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Start a transaction that writes an intent at b.
	txn := db.NewTxn(ctx, "intent")
	require.NoError(t, txn.Put(ctx, "b", "intent"))

	// Generate an SSTable that covers keys a, b, and c, and submit it with high
	// priority. This is going to abort the transaction above, encounter its
	// intent, and resolve it.
	sst, start, end := makeSST(t, []mvccKV{
		{"a", 1, "1"},
		{"b", 1, "2"},
		{"c", 1, "3"},
	})
	ba := roachpb.BatchRequest{
		Header: roachpb.Header{UserPriority: roachpb.MaxUserPriority},
	}
	ba.Add(&roachpb.AddSSTableRequest{
		RequestHeader:     roachpb.RequestHeader{Key: start, EndKey: end},
		Data:              sst,
		MVCCStats:         sstStats(t, sst),
		DisallowShadowing: true,
	})
	_, pErr := db.NonTransactionalSender().Send(ctx, ba)
	require.Nil(t, pErr)

	// The transaction should now be aborted.
	err := txn.Commit(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "TransactionRetryWithProtoRefreshError: TransactionAbortedError")
}

// TestAddSSTableWriteAtRequestTimestampRespectsTSCache checks that AddSSTable
// with WriteAtRequestTimestamp respects the timestamp cache.
func TestAddSSTableWriteAtRequestTimestampRespectsTSCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{},
	})
	defer s.Stopper().Stop(ctx)

	// Write key.
	txn := db.NewTxn(ctx, "txn")
	require.NoError(t, txn.Put(ctx, "key", "txn"))
	require.NoError(t, txn.Commit(ctx))
	txnTS := txn.CommitTimestamp()

	// Add an SST writing below the previous write.
	sst, start, end := makeSST(t, []mvccKV{{"key", 1, "sst"}})
	sstReq := &roachpb.AddSSTableRequest{
		RequestHeader:           roachpb.RequestHeader{Key: start, EndKey: end},
		Data:                    sst,
		MVCCStats:               sstStats(t, sst),
		WriteAtRequestTimestamp: true,
	}
	ba := roachpb.BatchRequest{
		Header: roachpb.Header{Timestamp: txnTS.Prev()},
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
	ba = roachpb.BatchRequest{
		Header: roachpb.Header{Timestamp: txnTS.Prev()},
	}
	ba.Add(sstReq)
	_, pErr = db.NonTransactionalSender().Send(ctx, ba)
	require.Nil(t, pErr)

	kv, err = db.Get(ctx, "key")
	require.NoError(t, err)
	require.Equal(t, "sst", string(kv.ValueBytes()))
}

// TestAddSSTableWriteAtRequestTimestampRespectsClosedTS checks that AddSSTable
// with WriteAtRequestTimestamp respects the closed timestamp.
func TestAddSSTableWriteAtRequestTimestampRespectsClosedTS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	si, _, db := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{},
	})
	defer si.Stopper().Stop(ctx)
	s := si.(*server.TestServer)

	// Issue a write to trigger a closed timestamp.
	require.NoError(t, db.Put(ctx, "someKey", "someValue"))

	// Get the closed timestamp for the range owning "key".
	rd, err := s.LookupRange(roachpb.Key("key"))
	require.NoError(t, err)
	r, store, err := s.GetStores().(*kvserver.Stores).GetReplicaForRangeID(ctx, rd.RangeID)
	require.NoError(t, err)
	closedTS := r.GetClosedTimestamp(ctx)
	require.NotZero(t, closedTS)

	// Add an SST writing below the closed timestamp. It should get pushed above it.
	reqTS := closedTS.Prev()
	sst, start, end := makeSST(t, []mvccKV{{"key", 1, "sst"}})
	sstReq := &roachpb.AddSSTableRequest{
		RequestHeader:           roachpb.RequestHeader{Key: start, EndKey: end},
		Data:                    sst,
		MVCCStats:               sstStats(t, sst),
		WriteAtRequestTimestamp: true,
	}
	ba := roachpb.BatchRequest{
		Header: roachpb.Header{Timestamp: reqTS},
	}
	ba.Add(sstReq)
	result, pErr := db.NonTransactionalSender().Send(ctx, ba)
	require.Nil(t, pErr)
	writeTS := result.Timestamp
	require.True(t, reqTS.Less(writeTS), "timestamp did not get pushed")
	require.True(t, closedTS.LessEq(writeTS), "timestamp %s below closed timestamp %s", result.Timestamp, closedTS)

	// Check that the value was in fact written at the write timestamp.
	kvs, err := storage.Scan(store.Engine(), roachpb.Key("key"), roachpb.Key("key").Next(), 0)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, storage.MVCCKey{Key: roachpb.Key("key"), Timestamp: writeTS}, kvs[0].Key)
	v, err := roachpb.Value{RawBytes: kvs[0].Value}.GetBytes()
	require.NoError(t, err)
	require.Equal(t, "sst", string(v))
}

// makeSST builds a binary in-memory SST from the given data.
func makeSST(t *testing.T, kvs []mvccKV) ([]byte, roachpb.Key, roachpb.Key) {
	t.Helper()

	sstFile := &storage.MemFile{}
	writer := storage.MakeBackupSSTWriter(sstFile)
	defer writer.Close()

	start, end := keys.MaxKey, keys.MinKey
	for _, kv := range kvs {
		if kv.key < string(start) {
			start = roachpb.Key(kv.key)
		}
		if kv.key > string(end) {
			end = roachpb.Key(kv.key)
		}
		if kv.ts == 0 {
			meta := &enginepb.MVCCMetadata{RawBytes: kv.ValueBytes()}
			metaBytes, err := protoutil.Marshal(meta)
			require.NoError(t, err)
			require.NoError(t, writer.PutUnversioned(kv.Key(), metaBytes))
		} else {
			require.NoError(t, writer.PutMVCC(kv.MVCCKey(), kv.ValueBytes()))
		}
	}
	require.NoError(t, writer.Finish())
	writer.Close()

	return sstFile.Data(), start, end.Next()
}

// sstStats computes the MVCC stats for the given binary SST.
func sstStats(t *testing.T, sst []byte) *enginepb.MVCCStats {
	t.Helper()

	iter, err := storage.NewMemSSTIterator(sst, true)
	require.NoError(t, err)
	defer iter.Close()

	stats, err := storage.ComputeStatsForRange(iter, keys.MinKey, keys.MaxKey, 0)
	require.NoError(t, err)
	return &stats
}

// engineStats computes the MVCC stats for the given engine.
func engineStats(t *testing.T, engine storage.Engine) *enginepb.MVCCStats {
	t.Helper()

	iter := engine.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
		LowerBound: keys.LocalMax,
		UpperBound: keys.MaxKey,
	})
	defer iter.Close()
	// We don't care about nowNanos, because the SST can't contain intents or
	// tombstones and all existing intents will be resolved.
	stats, err := storage.ComputeStatsForRange(iter, keys.LocalMax, keys.MaxKey, 0)
	require.NoError(t, err)
	return &stats
}
