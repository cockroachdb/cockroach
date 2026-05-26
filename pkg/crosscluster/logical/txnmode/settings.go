// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
)

// ErrEndTimeReached signals that the coordinator has processed all transactions
// with timestamp strictly less than endTime.
var ErrEndTimeReached = errors.New("LDR endTime reached")

var txnNumWriters = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.txn_num_writers",
	"the number of parallel writers for transactional logical replication",
	128,
	settings.PositiveInt,
)

var txnSchedulerLockCount = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.txn_scheduler_lock_count",
	"the maximum number of locks tracked by the transaction scheduler "+
		"before it starts evicting old transactions",
	1024*1024,
	settings.PositiveInt,
)

var txnBatchSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.txn_batch_size",
	"target number of rows or KVs per batch in the replication pipeline",
	1024,
	settings.PositiveInt,
)

var txnBatchFlushInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.txn_batch_flush_interval",
	"the maximum time to wait before flushing a partial batch of "+
		"decoded transactions",
	50*time.Millisecond,
)
