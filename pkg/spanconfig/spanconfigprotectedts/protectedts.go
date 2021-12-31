// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigprotectedts

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

type ProtectedTimestampStore interface {
	ProtectedTimestampStoreWriter
	ProtectedTimestampStoreReader
}

type ProtectedTimestampStoreWriter interface {
	Apply(hlc.Timestamp, ...ProtectedTimestampUpdate)
}

type ProtectedTimestampStoreReader interface {
	GetPTSRecordsForTableAsOf(descpb.ID, descpb.ID, hlc.Timestamp) []roachpb.ProtectedTimestampRecord
}

type ProtectedTimestampSubscriber interface {
	ProtectedTimestampStoreReader
	Subscribe(func(updated ProtectedTimestampUpdate))
}

type ProtectedTimestampUpdate struct {
	Target    ptpb.Target
	RecordID  uuid.UUID
	Timestamp hlc.Timestamp
}

func ProtectedTimestampAddition(
	recordID uuid.UUID, target ptpb.Target, ts hlc.Timestamp,
) ProtectedTimestampUpdate {
	return ProtectedTimestampUpdate{
		Target:    target,
		RecordID:  recordID,
		Timestamp: ts,
	}
}

func ProtectedTimestampDeletion(recordID uuid.UUID, target ptpb.Target) ProtectedTimestampUpdate {
	return ProtectedTimestampUpdate{
		Target:    target,
		RecordID:  recordID,
		Timestamp: hlc.Timestamp{}, // delete
	}
}
