// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type showInspectErrorsNode struct {
	zeroInputPlanNode

	n *tree.ShowInspectErrors
}

// ShowInspectErrors shows records from INSPECT consistency validation.
// Privileges: INSPECT.
func (p *planner) ShowInspectErrors(
	ctx context.Context, n *tree.ShowInspectErrors,
) (planNode, error) {
	if n.TableName != nil {
		_, tableDesc, err := p.ResolveMutableTableDescriptor(ctx, n.TableName, true /* required */, tree.ResolveRequireTableDesc)
		if err != nil {
			return nil, err
		}

		if err := checkInspectPrivilege(ctx, p, tableDesc.GetID()); err != nil {
			return nil, err
		}
	}

	if n.JobID != nil {
		if err := checkInspectPrivilegeForJob(ctx, p, jobspb.JobID(*n.JobID)); err != nil {
			return nil, err
		}
	}

	return &showInspectErrorsNode{n: n}, nil
}

func (n *showInspectErrorsNode) startExec(params runParams) error {
	return nil
}

func (n *showInspectErrorsNode) Next(params runParams) (bool, error) {
	return false, nil
}

func (n *showInspectErrorsNode) Values() tree.Datums {
	return nil
}

func (n *showInspectErrorsNode) Close(ctx context.Context) {
}

// checkInspectPrivilege checks that the user has the INSPECT privilege on the
// given table or its parent database.
func checkInspectPrivilege(ctx context.Context, p *planner, tableID descpb.ID) error {
	if err := p.CheckPrivilegeForTableID(ctx, tableID, privilege.INSPECT); err == nil {
		return nil
	}

	desc, err := p.LookupTableByID(ctx, tableID)
	if err != nil {
		return err
	}

	if err := p.CheckPrivilegeForDatabaseID(ctx, desc.GetParentID(), privilege.INSPECT); err == nil {
		return nil
	}

	return errors.Newf("user does not have INSPECT privilege on table %s or its parent database", desc.GetName())
}

// checkInspectPrivilegeForJob checks that the user has privileges for each
// table referenced by the specified inspect job.
func checkInspectPrivilegeForJob(ctx context.Context, p *planner, jobID jobspb.JobID) error {
	row, err := p.InternalSQLTxn().QueryRow(ctx, "check-inspect-job-privilege", p.txn,
		`SELECT ji.value as payload, j.status as status FROM system.jobs j JOIN system.job_info ji ON j.id = ji.job_id WHERE j.id = $1 AND j.job_type = 'INSPECT' AND ji.info_key = 'legacy_payload'`, jobID)
	if err != nil {
		return err
	}
	if row == nil {
		return errors.Newf("job %d not found or is not an INSPECT job", jobID)
	}

	statusDatum, ok := row[1].(*tree.DString)
	if !ok {
		return errors.New("failed to read job status")
	}
	if status := string(*statusDatum); status != string(jobs.StateSucceeded) {
		return errors.Newf("job %d is not yet completed (status: %s)", jobID, status)
	}
	payloadBytes, ok := row[0].(*tree.DBytes)
	if !ok {
		return errors.New("failed to read job payload")
	}

	var payload jobspb.Payload
	if err := payload.Unmarshal([]byte(*payloadBytes)); err != nil {
		return errors.Wrap(err, "failed to unmarshal job payload")
	}

	inspectPayload := payload.Details.(*jobspb.Payload_InspectDetails)
	if inspectPayload == nil {
		return errors.New("job payload does not contain inspect details")
	}

	for _, check := range inspectPayload.InspectDetails.Checks {
		if err := checkInspectPrivilege(ctx, p, check.TableID); err != nil {
			return err
		}
	}

	return nil
}
