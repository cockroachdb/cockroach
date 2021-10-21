package streamproducer

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"time"
)


// Create a job record
func doInitStream(evalCtx *tree.EvalContext, txn *kv.Txn, tenantID uint64, timeout duration.Duration) (jobspb.JobID, error) {
	prefix := keys.MakeTenantPrefix(roachpb.MakeTenantID(tenantID))
	span := roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}

	jr := jobs.Record{
		Description: "", // TODO: how does changefeed do descriptrion?
		Username:    evalCtx.Username,
		Details:  jobspb.StreamReplicationDetails{
			TenantID:      tenantID,
			Span:          &span,
			StreamTimeout: time.Duration(timeout.Nanos()),
		},
		Progress: jobspb.StreamReplicationProgress{LastActive: timeutil.ToUnixMicros(timeutil.Now())},
	}

	ctx := evalCtx.Ctx()
	jobRegistry := evalCtx.ExecConfigAccessor.JobRegistry().(*jobs.Registry)
	jobID := jobRegistry.MakeJobID()

	if _, err := jobRegistry.CreateAdoptableJobWithTxn(ctx, jr, jobID, txn); err != nil {
		return 0, err
	}
	return jobID, nil
}

type producerJobResumer struct {
	job *jobs.Job
}

// Resume is part of the jobs.Resumer interface.
func (p *producerJobResumer) Resume(ctx context.Context, execCtx interface{}) error {
	fmt.Println("Resume:",time.Now())
	jobExec := execCtx.(sql.JobExecContext)
	execCfg := jobExec.ExecCfg()
	isTimedOut := func(job *jobs.Job) bool {
		streamTimeout := p.job.Details().(jobspb.StreamReplicationDetails).StreamTimeout
		progress := p.job.Progress()
		lastActive := timeutil.FromUnixMicros(progress.GetStreamReplicationProgress().LastActive)
		timeoutFuture := lastActive.Add(streamTimeout)
		now := timeutil.Now()
		return timeoutFuture.Before(now)
	}
	if isTimedOut(p.job) {
		return errors.Errorf("stream %d timed out", p.job.ID())
	}

	t := timeutil.NewTimer()
	t.Reset(20 * time.Millisecond) // change it a cluster settings or a knob
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			t.Read = true
			t.Reset(1 * time.Millisecond)
			// read the last active
			if j, err := execCfg.JobRegistry.LoadJob(ctx, p.job.ID()); err != nil {
				return err
			} else if isTimedOut(j) {
				return errors.Errorf("stream %d timed out", p.job.ID())
			}
		}
	}
}

func (p *producerJobResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	return nil
}

func init() {
	streamingccl.RegisterStreamAPI("init_stream",
		func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			tenantID := uint64(*args[0].(*tree.DInt))
			timeout := args[1].(*tree.DInterval).Duration
			jobID, err := doInitStream(evalCtx, evalCtx.Txn, tenantID, timeout)
			return tree.NewDInt(tree.DInt(jobID)), err
		})
	jobs.RegisterConstructor(
		jobspb.TypeStreamReplication,
		func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return &producerJobResumer{job: job}
		},
	)
}


