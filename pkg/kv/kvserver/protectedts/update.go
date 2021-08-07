// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package protectedts

import (
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// UpdateFn is a callback that updates a protected timestamp record.
type UpdateFn func(txn *kv.Txn, r RecordMetadata, ru *RecordUpdater) error

// RecordMetadata groups the record metadata values passed to UpdateFn.
type RecordMetadata struct {
	Timestamp hlc.Timestamp
}

// RecordUpdater accumulates changes to record metadata that are to be
// persisted.
type RecordUpdater struct {
	r RecordMetadata
}

// GetTimestamp gets the new timestamp.
func (ru *RecordUpdater) GetTimestamp() hlc.Timestamp {
	return ru.r.Timestamp
}

// UpdateTimestamp sets a new timestamp (to be persisted).
func (ru *RecordUpdater) UpdateTimestamp(newProtectTimestamp hlc.Timestamp) {
	ru.r.Timestamp = newProtectTimestamp
}

// HasUpdates returns true if any changes to record metadata are to be
// persisted.
func (ru *RecordUpdater) HasUpdates() bool {
	return ru.r != RecordMetadata{}
}
