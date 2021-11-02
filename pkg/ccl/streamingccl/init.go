// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingccl

import (
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type replicationStreamManagerImpl struct{}

// CompleteStreamIngestionHook hooks a CompleteStreamIngestion implementation
// inside streamingccl package.
var CompleteStreamIngestionHook func(evalCtx *tree.EvalContext, txn *kv.Txn, jobID int, cutoverTimestamp hlc.Timestamp) error

func (r replicationStreamManagerImpl) CompleteStreamIngestion(
	evalCtx *tree.EvalContext, txn *kv.Txn, jobID int, cutoverTimestamp hlc.Timestamp,
) error {
	if CompleteStreamIngestionHook == nil {
		return errors.New("CompleteStreamIngestionHook is not registered")
	}
	return CompleteStreamIngestionHook(evalCtx, txn, jobID, cutoverTimestamp)
}

// InitStreamHook hooks an InitStream implementation inside streamingccl package.
var InitStreamHook func(evalCtx *tree.EvalContext, txn *kv.Txn, tenantID uint64) (jobspb.JobID, error)

func (r replicationStreamManagerImpl) InitStream(
	evalCtx *tree.EvalContext, txn *kv.Txn, tenantID uint64,
) (jobspb.JobID, error) {
	if InitStreamHook == nil {
		return jobspb.InvalidJobID, errors.New("InitStreamHook is not registered")
	}
	return InitStreamHook(evalCtx, txn, tenantID)
}

func init() {
	streaming.GetReplicationStreamManagerHook = func() (streaming.ReplicationStreamManager, error) {
		return replicationStreamManagerImpl{}, nil
	}
}
