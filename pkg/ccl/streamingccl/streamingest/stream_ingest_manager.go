// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type streamIngestManagerImpl struct{}

// CompleteStreamIngestion implements streaming.StreamIngestManager interface.
func (r *streamIngestManagerImpl) CompleteStreamIngestion(
	evalCtx *eval.Context, txn *kv.Txn, streamID streaming.StreamID, cutoverTimestamp hlc.Timestamp,
) error {
	return completeStreamIngestion(evalCtx, txn, streamID, cutoverTimestamp)
}

func newStreamIngestManagerWithPrivilegesCheck(
	evalCtx *eval.Context,
) (streaming.StreamIngestManager, error) {
	isAdmin, err := evalCtx.SessionAccessor.HasAdminRole(evalCtx.Context)
	if err != nil {
		return nil, err
	}

	if !isAdmin {
		return nil,
			pgerror.New(pgcode.InsufficientPrivilege, "replication restricted to ADMIN role")
	}

	execCfg := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)
	enterpriseCheckErr := utilccl.CheckEnterpriseEnabled(
		execCfg.Settings, execCfg.LogicalClusterID(), execCfg.Organization(), "REPLICATION")
	if enterpriseCheckErr != nil {
		return nil, pgerror.Wrap(enterpriseCheckErr,
			pgcode.InsufficientPrivilege, "replication requires enterprise license")
	}

	return &streamIngestManagerImpl{}, nil
}

func init() {
	streaming.GetStreamIngestManagerHook = newStreamIngestManagerWithPrivilegesCheck
}
