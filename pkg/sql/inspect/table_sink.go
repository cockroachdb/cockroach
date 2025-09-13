// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// tableSink will report any inspect errors directly to system.inspect_errors.
type tableSink struct {
	foundIssue bool
	db         descs.DB
	jobID      jobspb.JobID
}

var _ inspectLogger = &tableSink{}

// logIssue implements the inspectLogger interface.
func (c *tableSink) logIssue(ctx context.Context, issue *inspectIssue) error {
	c.foundIssue = true
	log.Errorf(ctx, "inspect issue: %+v", issue)

	// Serialize the details to JSON
	detailsJSON, err := json.Marshal(issue.Details)
	if err != nil {
		return err
	}

	// Insert the issue into system.inspect_errors
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
			(job_id, error_type, database_id, schema_id, id, primary_key, details)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		c.jobID,
		string(issue.ErrorType),
		issue.DatabaseID,
		issue.SchemaID,
		issue.ObjectID,
		issue.PrimaryKey,
		string(detailsJSON),
	); err != nil {
		return err
	}

	return nil
}

// HasIssues implements the inspectLogger interface.
func (c *tableSink) hasIssues() bool {
	return c.foundIssue
}
