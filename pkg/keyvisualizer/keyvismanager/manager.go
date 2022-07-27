package keyvismanager

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvisjob"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvissettings"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)



// Manager is the coordinator of the key visualizer subsystem.
// It ensures that there's only one key visualizer job for a given tenant.
type Manager struct {
	db       *kv.DB
	jr       *jobs.Registry
	ie       sqlutil.InternalExecutor
	stopper  *stop.Stopper
	settings *cluster.Settings

	keyvisualizer.SpanStatsConsumer
}

func New(
	db *kv.DB,
	jr *jobs.Registry,
	ie sqlutil.InternalExecutor,
	stopper *stop.Stopper,
	settings *cluster.Settings,
	consumer keyvisualizer.SpanStatsConsumer,
) *Manager {

	keyvisjob.Initialize()

	return &Manager{
		db:                db,
		jr:                jr,
		ie:                ie,
		stopper:           stopper,
		settings:          settings,
		SpanStatsConsumer: consumer,
	}
}

func (m *Manager) Start(ctx context.Context) error {
	return m.stopper.RunAsyncTask(ctx, "key-visualizer-manager",
		func(ctx context.Context) {
			m.run(ctx)
		})
}

func (m *Manager) run(ctx context.Context) {
	jobCheckCh := make(chan struct{}, 1)
	triggerJobCheck := func() {
		select {
		case jobCheckCh <- struct{}{}:
		default:
		}
	}

	// TODO(zachlite): trigger a job check when the cluster version changes
	keyvissettings.Enabled.SetOnChange(&m.settings.SV, func(ctx context.Context) {
		triggerJobCheck()
	})

	keyvissettings.SampleInterval.SetOnChange(&m.settings.SV,
		func(ctx context.Context) {
		triggerJobCheck()
	})

	checkJob := func() {
		if !keyvissettings.Enabled.Get(&m.settings.SV) {
			return
		}
		started, err := m.createAndStartJobIfNoneExists(ctx)
		if err != nil {
			log.Errorf(ctx, "error starting key visualizer job: %v", err)
		}
		if started {
			log.Infof(ctx, "started key visualizer job")
		}
	}

	// Periodically check if the job exists and start
	// it if it doesn't.
	timer := timeutil.NewTimer()
	defer timer.Stop()

	triggerJobCheck()
	for {
		timer.Reset(keyvissettings.SampleInterval.Get(&m.settings.SV))
		select {
		case <-jobCheckCh:
			checkJob()
		case <-timer.C:
			timer.Read = true
			checkJob()
		case <-m.stopper.ShouldQuiesce():
			return
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) createAndStartJobIfNoneExists(ctx context.Context) (bool, error) {
	record := jobs.Record{
		JobID:         m.jr.MakeJobID(),
		Description:   "key visualizer job",
		Username:      username.NodeUserName(),
		Details:       jobspb.KeyVisualizerDetails{},
		Progress:      jobspb.KeyVisualizerProgress{},
		NonCancelable: true,
	}

	var job *jobs.Job
	if err := m.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		exists, err := jobs.RunningJobExists(ctx, jobspb.InvalidJobID, m.ie, txn,
			func(payload *jobspb.Payload) bool {
				return payload.Type() == jobspb.TypeKeyVisualizer
			},
		)
		if err != nil {
			return err
		}

		if exists {
			// Nothing to do here.
			job = nil
			return nil
		}
		job, err = m.jr.CreateJobWithTxn(ctx, record, record.JobID, txn)
		return err
	}); err != nil {
		return false, err
	}

	if job == nil {
		return false, nil
	}

	m.jr.NotifyToResume(ctx, job.ID())
	return true, nil
}
