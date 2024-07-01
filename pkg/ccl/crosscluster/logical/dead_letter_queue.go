// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package logical

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// TODO(azhu): Use WriteType as built-in database enum.
type WriteType int

const (
	Insert WriteType = iota
	Delete
)

func (t WriteType) String() string {
	switch t {
	case Insert:
		return "Insert"
	case Delete:
		return "Delete"
	default:
		return fmt.Sprintf("Unrecognized WriteType(%d)", int(t))
	}
}

type ConflictResolutionType int

const (
	Unresolved ConflictResolutionType = iota
	ManuallyResolved
	Applied
	Ignored
)

type DeadLetterQueueClient interface {
	Create() error

	Log(
		ctx context.Context,
		ingestionJobID int64,
		kv roachpb.KeyValue,
		cdcEventRow cdcevent.Row,
		writeTimestamp hlc.Timestamp,
		conflictReason string,
	) error
}

type loggingDeadLetterQueueClient struct {
}

// TODO(azhu): Implement after V0.
// In future iterations Create() should create a DLQ table in the database.
func (dlq *loggingDeadLetterQueueClient) Create() error {
	return nil
}

// TODO(azhu): Append complete log as rows to DLQ table.
func (dlq *loggingDeadLetterQueueClient) Log(
	ctx context.Context,
	ingestionJobID int64,
	kv roachpb.KeyValue,
	cdcEventRow cdcevent.Row,
	writeTimestamp hlc.Timestamp,
	conflictReason string,
) error {
	if err := dlq.exists(); err != nil {
		return errors.New("dead letter queue table needs to be created in order to append logs")
	}

	if !cdcEventRow.IsInitialized() {
		return errors.New("cdc event row not initialized")
	}

	tableID := cdcEventRow.TableID
	var writeType WriteType

	if cdcEventRow.IsDeleted() {
		writeType = Delete
	} else {
		writeType = Insert
	}

	// TODO(azhu): once we have the actual conflictReason passed in, replace DebugString() with conflictReason
	log.Infof(ctx,
		`ingestion_job_id: %d, \ntable_id: %d, \nwrite_timestamp: %d, \nwrite_type: %s,\nconflict_reason: %s`,
		ingestionJobID, tableID, writeTimestamp, writeType, cdcEventRow.DebugString())
	return nil
}

// TODO(azhu): verify that DLQ table exists.
func (dlq *loggingDeadLetterQueueClient) exists() error {
	return nil
}

func InitDeadLetterQueueClient() DeadLetterQueueClient {
	return &loggingDeadLetterQueueClient{}
}
