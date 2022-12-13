// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type jobsGenerator struct {
	ctx                         context.Context
	txn                         *kv.Txn
	executor                    sqlutil.InternalExecutor
	user                        username.SQLUsername
	userIsAdmin                 bool
	userHasControlJobRoleOption bool

	planner eval.Planner

	iter sqlutil.InternalRows

	buf tree.Datums
}

// NewJobsGenerator creates a new jobs table iterator.
// This iterator will skip jobs which the user does not
// have access to.
func NewJobsGenerator(
	ctx context.Context,
	txn *kv.Txn,
	executor sqlutil.InternalExecutor,
	planner eval.Planner,
	user username.SQLUsername,
) (eval.ValueGenerator, error) {
	userIsAdmin, err := planner.UserHasAdminRole(ctx, user)
	if err != nil {
		return &jobsGenerator{}, err
	}
	userHasControlJobRoleOption, err := planner.HasRoleOption(ctx, roleoption.CONTROLJOB)
	if err != nil {
		return &jobsGenerator{}, err
	}
	return &jobsGenerator{
		ctx:                         ctx,
		txn:                         txn,
		executor:                    executor,
		user:                        user,
		userIsAdmin:                 userIsAdmin,
		userHasControlJobRoleOption: userHasControlJobRoleOption,
		planner:                     planner,
	}, nil
}

var jobsIterType = types.MakeLabeledTuple(
	[]*types.T{types.Int, types.String, types.Timestamp, types.Bytes, types.Bytes, types.String,
		types.Int, types.Bytes, types.Int, types.Int, types.Timestamp},
	[]string{"id", "status", "created", "payload", "progress", "created_by_type",
		"created_by_id", "claim_session_id", "claim_instance_id", "num_runs", "last_run"},
)

// ResolvedType implements the eval.ValueGenerator interface.
func (*jobsGenerator) ResolvedType() *types.T {
	return jobsIterType
}

const jobsTableWithUsernameQuery = `
SELECT 
    *,
    crdb_internal.pb_to_json('payload', payload) ->> 'usernameProto'
FROM system.jobs;
`

// Start implements the eval.ValueGenerator interface.
func (gen *jobsGenerator) Start(ctx context.Context, txn *kv.Txn) error {
	iter, err := gen.executor.QueryIteratorEx(ctx,
		"system-jobs-scan",
		txn,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		jobsTableWithUsernameQuery,
	)
	if err != nil {
		return err
	}

	gen.iter = iter
	return nil
}

// Next implements the eval.ValueGenerator interface.
//
// This method is not idempotent.
func (gen *jobsGenerator) Next(ctx context.Context) (bool, error) {
	for {
		hasNext, err := gen.iter.Next(ctx)
		if !hasNext || err != nil {
			return hasNext, err
		}

		currentRow := gen.iter.Cur()
		jobOwnerDatum := currentRow[gen.iter.Cur().Len()-1]
		jobOwnerUser := username.MakeSQLUsernameFromPreNormalizedString(strings.Trim(jobOwnerDatum.String(), "'"))
		jobOwnerIsAdmin, err := gen.planner.UserHasAdminRole(ctx, jobOwnerUser)
		if err != nil {
			return false, err
		}

		if !userHasJobAccess(gen.user, gen.userIsAdmin, gen.userHasControlJobRoleOption, jobOwnerUser, jobOwnerIsAdmin) {
			continue
		}

		gen.buf = gen.iter.Cur()[:gen.iter.Cur().Len()-1]
		return hasNext, err
	}
}

// userHasJobAccess determines if a user can access a job based on the
// job owner and role options.
//
// The user can access the row if the meet one of the conditions:
//  1. The user is an admin.
//  2. The job is owned by the user.
//  3. The user has CONTROLJOB privilege and the job is not owned by
//     an admin.
func userHasJobAccess(
	currentUser username.SQLUsername,
	currentUserIsAdmin bool,
	currentUserHasControlJob bool,
	jobOwner username.SQLUsername,
	jobOwnerIsAdmin bool,
) bool {
	if currentUserIsAdmin {
		return true
	}
	if !jobOwnerIsAdmin && currentUserHasControlJob {
		return true
	}
	if currentUser == jobOwner {
		return true
	}
	return false
}

// Values implements the eval.ValueGenerator interface.
func (gen *jobsGenerator) Values() (tree.Datums, error) {
	return gen.buf, nil
}

// Close implements the eval.ValueGenerator interface.
func (gen *jobsGenerator) Close(ctx context.Context) {
	err := gen.iter.Close()
	if err != nil {
		log.Error(ctx, "could not close jobs generator iterator")
	}
}
