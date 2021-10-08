// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// streamClientValidatorWrapper wraps a Validator and exposes additional methods
// used by stream ingestion to check for correctness.
type streamClientValidator struct {
	cdctest.StreamValidator

	mu syncutil.Mutex
}

// newStreamClientValidator returns a wrapped Validator, that can be used
// to validate the events emitted by the cluster to cluster streaming client.
// The wrapper currently only "wraps" an orderValidator, but can be built out
// to utilize other Validator's.
// The wrapper also allows querying the orderValidator to retrieve streamed
// events from an in-memory store.
func newStreamClientValidator() *streamClientValidator {
	ov := cdctest.NewStreamOrderValidator()
	return &streamClientValidator{
		StreamValidator: ov,
	}
}

func (sv *streamClientValidator) noteRow(
	partition string, key, value string, updated hlc.Timestamp,
) error {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	return sv.NoteRow(partition, key, value, updated)
}

func (sv *streamClientValidator) noteResolved(partition string, resolved hlc.Timestamp) error {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	return sv.NoteResolved(partition, resolved)
}

func (sv *streamClientValidator) failures() []string {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	return sv.Failures()
}

func (sv *streamClientValidator) getValuesForKeyBelowTimestamp(
	key string, timestamp hlc.Timestamp,
) ([]roachpb.KeyValue, error) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	return sv.GetValuesForKeyBelowTimestamp(key, timestamp)
}
