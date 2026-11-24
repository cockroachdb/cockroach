// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// LogicalReplicationPlanner generates a physical plan for logical replication.
// An initial plan is generated during job startup and the replanner will
// periodically call GeneratePlan to recalculate the best plan. If the newly
// generated plan differs significantly from the initial plan, the entire
// distSQL flow is shut down and a new initial plan will be created.
type LogicalReplicationPlanner struct {
	Job        *jobs.Job
	JobExecCtx sql.JobExecContext
	Client     streamclient.Client
}

// LogicalReplicationPlanInfo contains metadata about a logical replication
// physical plan.
type LogicalReplicationPlanInfo struct {
	SourceSpans      []roachpb.Span
	PartitionPgUrls  []string
	DestTableBySrcID map[descpb.ID]dstTableMetadata
	// WriteProcessorCount is the number of processors writing data on the
	// destination cluster (offline or otherwise).
	WriteProcessorCount int
}

// MakeLogicalReplicationPlanner creates a new LogicalReplicationPlanner.
func MakeLogicalReplicationPlanner(
	jobExecCtx sql.JobExecContext, job *jobs.Job, client streamclient.Client,
) LogicalReplicationPlanner {
	return LogicalReplicationPlanner{
		Job:        job,
		JobExecCtx: jobExecCtx,
		Client:     client,
	}
}

// GetSourcePlan fetches the logical replication plan from the source cluster.
// It is the shared setup used by planRowReplication, planOfflineInitialScan,
// and transaction mode replication planning.
func (p *LogicalReplicationPlanner) GetSourcePlan(
	ctx context.Context,
) (streamclient.LogicalReplicationPlan, error) {
	progress := p.Job.Progress().Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
	payload := p.Job.Payload().Details.(*jobspb.Payload_LogicalReplicationDetails).LogicalReplicationDetails

	asOf := progress.ReplicatedTime
	if asOf.IsEmpty() {
		asOf = payload.ReplicationStartTime
	}

	req := streampb.LogicalReplicationPlanRequest{
		PlanAsOf: asOf,
		// During an offline initial scan, we need to replicate the whole table, not
		// just the primary keys.
		UseTableSpan: payload.CreateTable && progress.ReplicatedTime.IsEmpty(),
		StreamID:     streampb.StreamID(payload.StreamID),
	}
	for _, pair := range payload.ReplicationPairs {
		req.TableIDs = append(req.TableIDs, pair.SrcDescriptorID)
	}

	return p.Client.PlanLogicalReplication(ctx, req)
}

// GeneratePlan creates a new physical plan for row replication. It is called
// by the replanner to detect topology changes and trigger re-planning.
func (p *LogicalReplicationPlanner) GeneratePlan(
	ctx context.Context, dsp *sql.DistSQLPlanner,
) (*sql.PhysicalPlan, *sql.PlanningCtx, error) {
	sourcePlan, err := p.GetSourcePlan(ctx)
	if err != nil {
		return nil, nil, err
	}
	plan, planCtx, _, err := p.planRowReplication(ctx, dsp, sourcePlan)
	return plan, planCtx, err
}
