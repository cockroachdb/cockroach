// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keyvismanager

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	_ "github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvisjob"
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
	db           *kv.DB
	jr           *jobs.Registry
	ie           sqlutil.InternalExecutor
	stopper      *stop.Stopper
	settings     *cluster.Settings
	runningJobID *jobspb.JobID
}

// New constructs a new keyvismanager.Manager.
func New(
	db *kv.DB,
	jr *jobs.Registry,
	ie sqlutil.InternalExecutor,
	stopper *stop.Stopper,
	settings *cluster.Settings,
) *Manager {
	return &Manager{
		db:       db,
		jr:       jr,
		ie:       ie,
		stopper:  stopper,
		settings: settings,
	}
}

// Start creates a background task that starts the key visualizer job.
func (m *Manager) Start(ctx context.Context) error {
	return m.stopper.RunAsyncTask(ctx, "key-visualizer-manager", m.run)
}

func (m *Manager) run(ctx context.Context) {
	jobCheckCh := make(chan struct{}, 1)
	triggerJobCheck := func() {
		select {
		case jobCheckCh <- struct{}{}:
		default:
		}
	}

	keyvissettings.Enabled.SetOnChange(&m.settings.SV, func(ctx context.Context) {
		triggerJobCheck()
	})

	keyvissettings.CheckInterval.SetOnChange(&m.settings.SV,
		func(ctx context.Context) {
			triggerJobCheck()
		})

	runningJobID := jobspb.InvalidJobID

	checkJob := func() {
		if !keyvissettings.Enabled.Get(&m.settings.SV) {
			if runningJobID != jobspb.InvalidJobID {
				if err := m.jr.CancelRequested(ctx, nil, runningJobID); err != nil {
					log.Errorf(ctx, "error requesting cancel for key visualizer job: %v", err)
				}
				runningJobID = jobspb.InvalidJobID
			}
			return
		}
		started, jobID, err := m.createAndStartJobIfNoneExists(ctx)
		if err != nil {
			log.Errorf(ctx, "error starting key visualizer job: %v", err)
		}
		if started {
			runningJobID = jobID
			log.Infof(ctx, "started key visualizer job")
		}
	}

	// Periodically check if the job exists and start
	// it if it doesn't.
	timer := timeutil.NewTimer()
	defer timer.Stop()

	triggerJobCheck()
	for {
		timer.Reset(keyvissettings.CheckInterval.Get(&m.settings.SV))
		select {
		case <-timer.C:
			timer.Read = true
			checkJob()
		case <-jobCheckCh:
			checkJob()
		case <-m.stopper.ShouldQuiesce():
			return
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) createAndStartJobIfNoneExists(
	ctx context.Context,
) (started bool, jobID jobspb.JobID, err error) {
	record := jobs.Record{
		JobID:         m.jr.MakeJobID(),
		Description:   "key visualizer job",
		Username:      username.NodeUserName(),
		Details:       jobspb.KeyVisualizerDetails{},
		Progress:      jobspb.KeyVisualizerProgress{},
		NonCancelable: false,
	}

	var createdJob *jobs.Job
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
			return nil
		}

		createdJob, err = m.jr.CreateJobWithTxn(ctx, record, record.JobID, txn)
		m.jr.NotifyToResume(ctx, createdJob.ID())
		return err
	}); err != nil {
		return false, jobspb.InvalidJobID, err
	}

	if createdJob == nil {
		// A running job already exists.
		return false, jobspb.InvalidJobID, nil
	}

	// A job was created and notified to resume.
	return true, createdJob.ID(), nil
}
