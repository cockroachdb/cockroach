// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	gojson "encoding/json"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
)

// tableSink will report any inspect errors directly to system.inspect_errors.
type tableSink struct {
	foundIssue atomic.Bool
	db         descs.DB
	jobID      jobspb.JobID
}

var _ inspectLogger = &tableSink{}

// logIssue implements the inspectLogger interface.
func (c *tableSink) logIssue(ctx context.Context, issue *inspectIssue) error {
	c.foundIssue.Store(true)

	detailsBytes, err := gojson.Marshal(issue.Details)
	if err != nil {
		return err
	}

	executor := c.db.Executor()

	if _, err = executor.ExecEx(
		ctx,
		"insert-inspect-error",
		nil, /* txn */
		sessiondata.InternalExecutorOverride{
			User:             username.NodeUserName(),
			QualityOfService: &sessiondatapb.BulkLowQoS,
		},
		`INSERT INTO system.inspect_errors
			(job_id, error_type, aost, database_id, schema_id, id, primary_key, details)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		c.jobID,
		string(issue.ErrorType),
		issue.AOST,
		issue.DatabaseID,
		issue.SchemaID,
		issue.ObjectID,
		issue.PrimaryKey,
		string(detailsBytes),
	); err != nil {
		return err
	}

	return nil
}

// hasIssues implements the inspectLogger interface.
func (c *tableSink) hasIssues() bool {
	return c.foundIssue.Load()
}
