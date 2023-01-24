// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scrun

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
)

// JobTxnFunc is used to run a transactional stage of a schema change on
// behalf of a job. See JobRunDependencies.WithTxnInJob().
type JobTxnFunc = func(ctx context.Context, txnDeps scexec.Dependencies, eventLogger EventLogger) error

// JobRunDependencies contains the dependencies required for
// executing the schema change job, i.e. for the logic in its Resume() method.
type JobRunDependencies interface {
	// WithTxnInJob is a wrapper for opening and committing a transaction around
	// the execution of the callback. After committing the transaction, the job
	// registry should be notified to adopt jobs.
	WithTxnInJob(ctx context.Context, fn JobTxnFunc) error

	// ClusterSettings returns the cluster settings.
	ClusterSettings() *cluster.Settings
}

// EventLogger contains the dependencies required for logging schema change
// events.
type EventLogger interface {

	// LogEventForSchemaChange writes a schema change job event into the event
	// log: reverse_schema_change, finish_schema_change or
	// finish_schema_change_rollback.
	LogEventForSchemaChange(
		ctx context.Context, event logpb.EventPayload,
	) error
}
