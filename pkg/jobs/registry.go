// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/optionalnodeliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/logtags"
)

const defaultLeniencySetting = 60 * time.Second

var (
	gcSetting = settings.RegisterDurationSetting(
		"jobs.retention_time",
		"the amount of time to retain records for completed jobs before",
		time.Hour*24*14,
	).WithPublic()
)

// adoptedJobs represents a the epoch and cancelation of a job id being run
// by the registry.
type adoptedJob struct {
	sid sqlliveness.SessionID
	// Calling the func will cancel the context the job was resumed with.
	cancel context.CancelFunc
}

// adoptionNotice is used by Run to notify the registry to resumeClaimedJobs
// and by TestingNudgeAdoptionQueue to claimAndResumeClaimedJobs.
type adoptionNotice bool

const (
	resumeClaimedJobs         adoptionNotice = false
	claimAndResumeClaimedJobs adoptionNotice = true
)

// Registry creates Jobs and manages their leases and cancelation.
//
// Job information is stored in the `system.jobs` table.  Each node will
// poll this table and establish a lease on any claimed job. Registry
// calculates its own liveness for a node based on the expiration time
// of the underlying node-liveness lease.  This is because we want to
// allow jobs assigned to temporarily non-live (i.e. saturated) nodes to
// continue without being canceled.
//
// When a lease has been determined to be stale, a node may attempt to
// claim the relevant job. Thus, a Registry must occasionally
// re-validate its own leases to ensure that another node has not stolen
// the work and cancel the local job if so.
//
// Prior versions of Registry used the node's epoch value to determine
// whether or not a job should be stolen.  The current implementation
// uses a time-based approach, where a node's last reported expiration
// timestamp is used to calculate a liveness value for the purpose
// of job scheduling.
//
// Mixed-version operation between epoch- and time-based nodes works
// since we still publish epoch information in the leases for time-based
// nodes.  From the perspective of a time-based node, an epoch-based
// node simply behaves as though its leniency period is 0. Epoch-based
// nodes will see time-based nodes delay the act of stealing a job.
type Registry struct {
	ac       log.AmbientContext
	stopper  *stop.Stopper
	nl       optionalnodeliveness.Container
	db       *kv.DB
	ex       sqlutil.InternalExecutor
	clock    *hlc.Clock
	nodeID   *base.SQLIDContainer
	settings *cluster.Settings
	execCtx  jobExecCtxMaker
	metrics  Metrics
	knobs    TestingKnobs

	// adoptionChan is used to nudge the registry to resume claimed jobs and
	// potentially attempt to claim jobs.
	adoptionCh  chan adoptionNotice
	sqlInstance sqlliveness.Instance

	// sessionBoundInternalExecutorFactory provides a way for jobs to create
	// internal executors. This is rarely needed, and usually job resumers should
	// use the internal executor from the JobExecCtx. The intended user of this
	// interface is the schema change job resumer, which needs to set the
	// tableCollectionModifier on the internal executor to different values in
	// multiple concurrent queries. This situation is an exception to the internal
	// executor generally being a stateless wrapper, and makes it impossible to
	// reuse the same internal executor across all the queries (without
	// refactoring to get rid of the tableCollectionModifier field, which we
	// should do eventually).
	//
	// Note that, while this API is not ideal, internal executors are basically
	// lightweight wrappers requiring no additional teardown. There's not much
	// cost incurred in creating these.
	//
	// TODO (lucy): We should refactor and get rid of the tableCollectionModifier
	// field. Modifying the TableCollection is basically a per-query operation
	// and should be a per-query setting. #34304 is the issue for creating/
	// improving this API.
	sessionBoundInternalExecutorFactory sqlutil.SessionBoundInternalExecutorFactory

	// if non-empty, indicates path to file that prevents any job adoptions.
	preventAdoptionFile string

	mu struct {
		syncutil.Mutex
		// epoch is present to support older nodes that are not using
		// the timestamp-based approach to determine when to steal jobs.
		// TODO: Remove this and deprecate Lease.Epoch proto field
		deprecatedEpoch int64
		// jobs holds a map from job id to its context cancel func. This should
		// be populated with jobs that are currently being run (and owned) by
		// this registry. Calling the func will cancel the context the job was
		// started/resumed with. This should only be called by the registry when
		// it is attempting to halt its own jobs due to liveness problems. Jobs
		// are normally canceled on any node by the CANCEL JOB statement, which is
		// propagated to jobs via the .Progressed call. This function should not be
		// used to cancel a job in that way.
		// TODO(spaskob): add deprecated notice.
		deprecatedJobs map[jobspb.JobID]context.CancelFunc

		// adoptedJobs holds a map from job id to its context cancel func and epoch.
		// It contains the that are adopted and rpobably being run. One exception is
		// jobs scheduled inside a transaction, they will show in this map but will
		// only be run when the transaction commits.
		adoptedJobs map[jobspb.JobID]*adoptedJob
	}

	TestingResumerCreationKnobs map[jobspb.Type]func(Resumer) Resumer
}

// jobExecCtxMaker is a wrapper around sql.NewInternalPlanner. It returns an
// *sql.planner as an interface{} due to package dependency cycles. It should
// be cast to that type in the sql package when it is used. Returns a cleanup
// function that must be called once the caller is done with the planner.
//
// TODO(mjibson): Can we do something to avoid passing an interface{} here
// that must be type casted in a Resumer? It cannot be done here because
// JobExecContext lives in the sql package, which would create a dependency
// cycle if listed here. Furthermore, moving JobExecContext into a common
// subpackage like sqlbase is difficult because of the amount of sql-only
// stuff that JobExecContext exports. One other choice is to merge this package
// back into the sql package. There's maybe a better way that I'm unaware of.
type jobExecCtxMaker func(opName string, user security.SQLUsername) (interface{}, func())

// PreventAdoptionFile is the name of the file which, if present in the first
// on-disk store, will prevent the adoption of background jobs by that node.
const PreventAdoptionFile = "DISABLE_STARTING_BACKGROUND_JOBS"

// MakeRegistry creates a new Registry. planFn is a wrapper around
// sql.newInternalPlanner. It returns a sql.JobExecCtx, but must be
// coerced into that in the Resumer functions.
func MakeRegistry(
	ac log.AmbientContext,
	stopper *stop.Stopper,
	clock *hlc.Clock,
	nl optionalnodeliveness.Container,
	db *kv.DB,
	ex sqlutil.InternalExecutor,
	nodeID *base.SQLIDContainer,
	sqlInstance sqlliveness.Instance,
	settings *cluster.Settings,
	histogramWindowInterval time.Duration,
	execCtxFn jobExecCtxMaker,
	preventAdoptionFile string,
	knobs *TestingKnobs,
) *Registry {
	r := &Registry{
		ac:                  ac,
		stopper:             stopper,
		clock:               clock,
		nl:                  nl,
		db:                  db,
		ex:                  ex,
		nodeID:              nodeID,
		sqlInstance:         sqlInstance,
		settings:            settings,
		execCtx:             execCtxFn,
		preventAdoptionFile: preventAdoptionFile,
		adoptionCh:          make(chan adoptionNotice),
	}
	if knobs != nil {
		r.knobs = *knobs
	}
	r.mu.deprecatedEpoch = 1
	r.mu.deprecatedJobs = make(map[jobspb.JobID]context.CancelFunc)
	r.mu.adoptedJobs = make(map[jobspb.JobID]*adoptedJob)
	r.metrics.init(histogramWindowInterval)
	return r
}

func (r *Registry) startUsingSQLLivenessAdoption(ctx context.Context) bool {
	return sqlliveness.IsActive(ctx, r.settings)
}

// SetSessionBoundInternalExecutorFactory sets the
// SessionBoundInternalExecutorFactory that will be used by the job registry
// executor. We expose this separately from the constructor to avoid a circular
// dependency.
func (r *Registry) SetSessionBoundInternalExecutorFactory(
	factory sqlutil.SessionBoundInternalExecutorFactory,
) {
	r.sessionBoundInternalExecutorFactory = factory
}

// MetricsStruct returns the metrics for production monitoring of each job type.
// They're all stored as the `metric.Struct` interface because of dependency
// cycles.
func (r *Registry) MetricsStruct() *Metrics {
	return &r.metrics
}

// CurrentlyRunningJobs returns a slice of the ids of all jobs running on this node.
func (r *Registry) CurrentlyRunningJobs() []jobspb.JobID {
	r.mu.Lock()
	defer r.mu.Unlock()
	jobs := make([]jobspb.JobID, len(r.mu.deprecatedJobs)+len(r.mu.adoptedJobs))
	i := 0
	// The following are maps keyed by job id.
	for jID := range r.mu.deprecatedJobs {
		jobs[i] = jID
		i++
	}
	for jID := range r.mu.adoptedJobs {
		jobs[i] = jID
		i++
	}
	return jobs
}

// ID returns a unique during the lifetume of the registry id that is
// used for keying sqlliveness claims held by the registry.
func (r *Registry) ID() base.SQLInstanceID {
	return r.nodeID.SQLInstanceID()
}

// makeCtx returns a new context from r's ambient context and an associated
// cancel func.
func (r *Registry) makeCtx() (context.Context, func()) {
	return context.WithCancel(r.ac.AnnotateCtx(context.Background()))
}

// MakeJobID generates a new job ID.
func (r *Registry) MakeJobID() jobspb.JobID {
	return jobspb.JobID(builtins.GenerateUniqueInt(r.nodeID.SQLInstanceID()))
}

// NotifyToAdoptJobs notifies the job adoption loop to start claimed jobs.
func (r *Registry) NotifyToAdoptJobs(ctx context.Context) error {
	select {
	case r.adoptionCh <- resumeClaimedJobs:
	case <-r.stopper.ShouldQuiesce():
		return stop.ErrUnavailable
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// WaitForJobs waits for a given list of jobs to reach some sort
// of terminal state.
func (r *Registry) WaitForJobs(
	ctx context.Context, ex sqlutil.InternalExecutor, jobs []jobspb.JobID,
) error {
	if len(jobs) == 0 {
		return nil
	}
	buf := bytes.Buffer{}
	for i, id := range jobs {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf(" %d", id))
	}
	// Manually retry instead of using SHOW JOBS WHEN COMPLETE so we have greater
	// control over retries. Also, avoiding SHOW JOBS prevents us from having to
	// populate the crdb_internal.jobs vtable.
	query := fmt.Sprintf(
		`SELECT count(*) FROM system.jobs WHERE id IN (%s)
       AND (status != 'succeeded' AND status != 'failed' AND status != 'canceled')`,
		buf.String())
	for r := retry.StartWithCtx(ctx, retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		Multiplier:     1.5,
	}); r.Next(); {
		// We poll the number of queued jobs that aren't finished. As with SHOW JOBS
		// WHEN COMPLETE, if one of the jobs is missing from the jobs table for
		// whatever reason, we'll fail later when we try to load the job.
		row, err := ex.QueryRowEx(
			ctx,
			"poll-show-jobs",
			nil, /* txn */
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			query,
		)
		if err != nil {
			return errors.Wrap(err, "polling for queued jobs to complete")
		}
		if row == nil {
			return errors.New("polling for queued jobs failed")
		}
		count := int64(tree.MustBeDInt(row[0]))
		if log.V(3) {
			log.Infof(ctx, "waiting for %d queued jobs to complete", count)
		}
		if count == 0 {
			break
		}
	}
	for i, id := range jobs {
		j, err := r.LoadJob(ctx, id)
		if err != nil {
			return errors.WithHint(
				errors.Wrapf(err, "job %d could not be loaded", jobs[i]),
				"The job may not have succeeded.")
		}
		if j.Payload().FinalResumeError != nil {
			decodedErr := errors.DecodeError(ctx, *j.Payload().FinalResumeError)
			return decodedErr
		}
		if j.Payload().Error != "" {
			return errors.Newf("job %d failed with error: %s", jobs[i], j.Payload().Error)
		}
	}
	return nil
}

// Run starts previously unstarted jobs from a list of scheduled
// jobs. Canceling ctx interrupts the waiting but doesn't cancel the jobs.
func (r *Registry) Run(
	ctx context.Context, ex sqlutil.InternalExecutor, jobs []jobspb.JobID,
) error {
	if len(jobs) == 0 {
		return nil
	}
	log.Infof(ctx, "scheduled jobs %+v", jobs)
	if err := r.NotifyToAdoptJobs(ctx); err != nil {
		return err
	}
	err := r.WaitForJobs(ctx, ex, jobs)
	if err != nil {
		return err
	}
	return nil
}

// NewJob creates a new Job.
func (r *Registry) NewJob(record Record, jobID jobspb.JobID) *Job {
	job := &Job{
		id:        jobID,
		registry:  r,
		createdBy: record.CreatedBy,
	}
	job.mu.payload = jobspb.Payload{
		Description:   record.Description,
		Statement:     record.Statements,
		UsernameProto: record.Username.EncodeProto(),
		DescriptorIDs: record.DescriptorIDs,
		Details:       jobspb.WrapPayloadDetails(record.Details),
		Noncancelable: record.NonCancelable,
	}
	job.mu.progress = jobspb.Progress{
		Details:       jobspb.WrapProgressDetails(record.Progress),
		RunningStatus: string(record.RunningStatus),
	}
	return job
}

// CreateJobWithTxn creates a job to be started later with StartJob. It stores
// the job in the jobs table, marks it pending and gives the current node a
// lease.
func (r *Registry) CreateJobWithTxn(
	ctx context.Context, record Record, jobID jobspb.JobID, txn *kv.Txn,
) (*Job, error) {
	j := r.NewJob(record, jobID)

	s, err := r.sqlInstance.Session(ctx)
	if errors.Is(err, sqlliveness.NotStartedError) {
		if r.startUsingSQLLivenessAdoption(ctx) {
			err = errors.WithAssertionFailure(err)
		} else {
			err = nil
		}
	}
	if err != nil {
		return nil, errors.Wrap(err, "error getting live session")
	}
	if !r.startUsingSQLLivenessAdoption(ctx) {
		// TODO(spaskob): remove in 20.2 as this code path is only needed while
		// migrating to 20.2 cluster.
		if err := j.deprecatedInsert(
			ctx, txn, jobID, r.deprecatedNewLease(), s,
		); err != nil {
			return nil, err
		}
		return j, nil
	}
	j.sessionID = s.ID()
	start := timeutil.Now()
	if txn != nil {
		start = txn.ReadTimestamp().GoTime()
	}
	j.mu.progress.ModifiedMicros = timeutil.ToUnixMicros(start)
	payloadBytes, err := protoutil.Marshal(&j.mu.payload)
	if err != nil {
		return nil, err
	}
	progressBytes, err := protoutil.Marshal(&j.mu.progress)
	if err != nil {
		return nil, err
	}
	if _, err = j.registry.ex.Exec(ctx, "job-row-insert", txn, `
INSERT INTO system.jobs (id, status, payload, progress, claim_session_id, claim_instance_id)
VALUES ($1, $2, $3, $4, $5, $6)`, jobID, StatusRunning, payloadBytes, progressBytes, s.ID().UnsafeBytes(), r.ID(),
	); err != nil {
		return nil, err
	}

	return j, nil
}

const invalidNodeID = 0

// CreateAdoptableJobWithTxn creates a job which will be adopted for execution
// at a later time by some node in the cluster.
func (r *Registry) CreateAdoptableJobWithTxn(
	ctx context.Context, record Record, jobID jobspb.JobID, txn *kv.Txn,
) (*Job, error) {
	j := r.NewJob(record, jobID)

	// We create a job record with an invalid lease to force the registry (on some node
	// in the cluster) to adopt this job at a later time.
	lease := &jobspb.Lease{NodeID: invalidNodeID}

	if err := j.deprecatedInsert(
		ctx, txn, jobID, lease, nil,
	); err != nil {
		return nil, err
	}
	return j, nil
}

// CreateStartableJobWithTxn creates a job to be started later, after the
// creating txn commits. The method uses the passed txn to write the job in the
// jobs table, marks it pending and gives the current node a lease. It
// additionally registers the job with the Registry which will prevent the
// Registry from adopting the job after the transaction commits. The resultsCh
// will be connected to the output of the job and written to after the returned
// StartableJob is started.
//
// The returned job is not associated with the user transaction. The intention
// is that the job will not be modified again in txn. If the transaction is
// committed, the caller must explicitly Start it. If the transaction is rolled
// back then the caller must call CleanupOnRollback to unregister the job from
// the Registry.
//
// When used in a closure that is retryable in the presence of transaction
// restarts, the job ID must be stable across retries to avoid leaking tracing
// spans and registry entries. The intended usage is to define the ID and
// *StartableJob outside the closure. The StartableJob referred to will remain
// the same if the method is called with the same job ID and has already been
// initialized with a tracing span and registered; otherwise, a new one will be
// allocated, and sj will point to it. The point is to ensure that the tracing
// span is created and the job registered exactly once, if and only if the
// transaction commits. This is a fragile API.
func (r *Registry) CreateStartableJobWithTxn(
	ctx context.Context, sj **StartableJob, jobID jobspb.JobID, txn *kv.Txn, record Record,
) error {
	alreadyInitialized := *sj != nil
	if alreadyInitialized {
		if jobID != (*sj).Job.ID() {
			log.Fatalf(ctx,
				"attempted to rewrite startable job for ID %d with unexpected ID %d",
				(*sj).Job.ID(), jobID,
			)
		}
	}

	j, err := r.CreateJobWithTxn(ctx, record, jobID, txn)
	if err != nil {
		return err
	}
	resumer, err := r.createResumer(j, r.settings)
	if err != nil {
		return err
	}

	var resumerCtx context.Context
	var cancel func()
	var span *tracing.Span
	var execDone chan struct{}
	if !alreadyInitialized {
		// Construct a context which contains a tracing span that follows from the
		// span in the parent context. We don't directly use the parent span because
		// we want independent lifetimes and cancellation. For the same reason, we
		// don't use the Context returned by ForkSpan.
		resumerCtx, cancel = r.makeCtx()
		_, span = tracing.ForkSpan(ctx, "job")
		if span != nil {
			resumerCtx = tracing.ContextWithSpan(resumerCtx, span)
		}

		if r.startUsingSQLLivenessAdoption(ctx) {
			r.mu.Lock()
			defer r.mu.Unlock()
			if _, alreadyRegistered := r.mu.adoptedJobs[jobID]; alreadyRegistered {
				log.Fatalf(ctx, "job %d: was just created but found in registered adopted jobs", jobID)
			}
			r.mu.adoptedJobs[jobID] = &adoptedJob{sid: j.sessionID, cancel: cancel}
		} else {
			// TODO(spaskob): remove in 20.2 as this code path is only needed while
			// migrating to 20.2 cluster.
			if err := r.deprecatedRegister(jobID, cancel); err != nil {
				return err
			}
		}

		execDone = make(chan struct{})
	}

	if !alreadyInitialized {
		*sj = &StartableJob{}
		(*sj).resumerCtx = resumerCtx
		(*sj).cancel = cancel
		(*sj).span = span
		(*sj).execDone = execDone
	}
	(*sj).Job = j
	(*sj).resumer = resumer
	(*sj).txn = txn
	return nil
}

// LoadJob loads an existing job with the given jobID from the system.jobs
// table.
func (r *Registry) LoadJob(ctx context.Context, jobID jobspb.JobID) (*Job, error) {
	return r.LoadJobWithTxn(ctx, jobID, nil)
}

// LoadJobWithTxn does the same as above, but using the transaction passed in
// the txn argument. Passing a nil transaction is equivalent to calling LoadJob
// in that a transaction will be automatically created.
func (r *Registry) LoadJobWithTxn(
	ctx context.Context, jobID jobspb.JobID, txn *kv.Txn,
) (*Job, error) {
	j := &Job{
		id:       jobID,
		registry: r,
	}
	if err := j.load(ctx, txn); err != nil {
		return nil, err
	}
	return j, nil
}

// UpdateJobWithTxn calls the Update method on an existing job with jobID, using
// a transaction passed in the txn argument. Passing a nil transaction means
// that a txn will be automatically created.
func (r *Registry) UpdateJobWithTxn(
	ctx context.Context, jobID jobspb.JobID, txn *kv.Txn, updateFunc UpdateFn,
) error {
	j := &Job{
		id:       jobID,
		registry: r,
	}
	return j.Update(ctx, txn, updateFunc)
}

// DefaultCancelInterval is a reasonable interval at which to poll this node
// for liveness failures and cancel running jobs.
var DefaultCancelInterval = 10 * time.Second

// DefaultAdoptInterval is a reasonable interval at which to poll system.jobs
// for jobs with expired leases.
//
// DefaultAdoptInterval is mutable for testing. NB: Updates to this value after
// Registry.Start has been called will not have any effect.
var DefaultAdoptInterval = 30 * time.Second

// TestingSetAdoptAndCancelIntervals can be used to accelerate job adoption and
// state changes.
func TestingSetAdoptAndCancelIntervals(adopt, cancel time.Duration) (cleanup func()) {
	prevAdopt := DefaultAdoptInterval
	prevCancel := DefaultCancelInterval
	DefaultAdoptInterval = adopt
	DefaultCancelInterval = cancel
	return func() {
		DefaultAdoptInterval = prevAdopt
		DefaultCancelInterval = prevCancel
	}
}

var maxAdoptionsPerLoop = envutil.EnvOrDefaultInt(`COCKROACH_JOB_ADOPTIONS_PER_PERIOD`, 10)

// maxGCInterval is the maximum duration for how often we check for and delete job
// records older than the retention limit.
const maxGCInterval = 1 * time.Hour

// Start polls the current node for liveness failures and cancels all registered
// jobs if it observes a failure. Otherwise it starts all the main daemons of
// registry that poll the jobs table and start/cancel/gc jobs.
func (r *Registry) Start(
	ctx context.Context, stopper *stop.Stopper, cancelInterval, adoptInterval time.Duration,
) error {

	every := log.Every(time.Second)
	withSession := func(
		f func(ctx context.Context, s sqlliveness.Session),
	) func(ctx context.Context) {
		return func(ctx context.Context) {
			if !r.startUsingSQLLivenessAdoption(ctx) {
				return
			}
			s, err := r.sqlInstance.Session(ctx)
			if err != nil {
				if log.ExpensiveLogEnabled(ctx, 2) || (ctx.Err() == nil && every.ShouldLog()) {
					log.Errorf(ctx, "error getting live session: %s", err)
				}
				return
			}

			log.VEventf(ctx, 1, "registry live claim (instance_id: %s, sid: %s)", r.ID(), s.ID())
			f(ctx, s)
		}
	}

	removeClaimsFromDeadSessions := func(ctx context.Context, s sqlliveness.Session) {
		if _, err := r.ex.QueryRowEx(
			ctx, "expire-sessions", nil,
			sessiondata.InternalExecutorOverride{User: security.RootUserName()}, `
UPDATE system.jobs
   SET claim_session_id = NULL
 WHERE claim_session_id <> $1
   AND status IN `+claimableStatusTupleString+`
   AND NOT crdb_internal.sql_liveness_is_alive(claim_session_id)`,
			s.ID().UnsafeBytes(),
		); err != nil {
			log.Errorf(ctx, "error expiring job sessions: %s", err)
		}
	}
	servePauseAndCancelRequests := func(ctx context.Context, s sqlliveness.Session) {
		if err := r.servePauseAndCancelRequests(ctx, s); err != nil {
			log.Errorf(ctx, "failed to serve pause and cancel requests: %v", err)
		}
	}
	cancelLoopTask := withSession(func(ctx context.Context, s sqlliveness.Session) {
		removeClaimsFromDeadSessions(ctx, s)
		r.maybeCancelJobs(ctx, s)
		servePauseAndCancelRequests(ctx, s)
	})
	claimJobs := withSession(func(ctx context.Context, s sqlliveness.Session) {
		if err := r.claimJobs(ctx, s); err != nil {
			log.Errorf(ctx, "error claiming jobs: %s", err)
		}
	})
	processClaimedJobs := withSession(func(ctx context.Context, s sqlliveness.Session) {
		if r.adoptionDisabled(ctx) {
			log.Warningf(ctx, "canceling all adopted jobs due to liveness failure")
			r.cancelAllAdoptedJobs()
			return
		}
		if err := r.processClaimedJobs(ctx, s); err != nil {
			log.Errorf(ctx, "error processing claimed jobs: %s", err)
		}
	})
	maybeAdoptJobsDeprecated := func(ctx context.Context, randomizeJobOrder bool) {
		if r.adoptionDisabled(ctx) {
			r.deprecatedCancelAll(ctx)
			return
		}
		if err := r.deprecatedMaybeAdoptJob(ctx, r.nl, randomizeJobOrder); err != nil {
			log.Errorf(ctx, "error while adopting jobs: %s", err)
		}
	}

	if err := stopper.RunAsyncTask(context.Background(), "jobs/cancel", func(ctx context.Context) {
		// Calling maybeCancelJobs once at the start ensures we have an up-to-date
		// liveness epoch before we wait out the first cancelInterval.
		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()
		r.maybeCancelJobsDeprecated(ctx, r.nl)
		cancelLoopTask(ctx)
		for {
			select {
			case <-r.stopper.ShouldQuiesce():
				log.Warningf(ctx, "canceling all adopted jobs due to stopper quiescing")
				r.deprecatedCancelAll(ctx)
				r.cancelAllAdoptedJobs()
				return
			case <-time.After(cancelInterval):
				r.maybeCancelJobsDeprecated(ctx, r.nl)
				cancelLoopTask(ctx)
			}
		}
	}); err != nil {
		return err
	}
	if err := stopper.RunAsyncTask(context.Background(), "jobs/gc", func(ctx context.Context) {
		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		settingChanged := make(chan struct{}, 1)
		gcSetting.SetOnChange(&r.settings.SV, func() {
			select {
			case settingChanged <- struct{}{}:
			default:
			}
		})
		gcInterval := func() time.Duration {
			if setting := gcSetting.Get(&r.settings.SV); setting < maxGCInterval {
				return setting
			}
			return maxGCInterval
		}
		timer := timeutil.NewTimer()
		lastGC := timeutil.Now()
		// We'll jitter the first cleanup run to avoid contention in case multiple
		// nodes restart at once.

		const jitter = 1 / 6
		jitterFraction := 1 + (2*rand.Float64()-1)*jitter // 1 + [-1/6, +1/6)
		jitterredDuration := float64(gcInterval()) * jitterFraction
		timer.Reset(time.Duration(jitterredDuration))
		defer cancel()
		for {
			select {
			case <-settingChanged:
				timer.Reset(timeutil.Until(lastGC.Add(gcInterval())))
			case <-stopper.ShouldQuiesce():
				return
			case <-timer.C:
				timer.Read = true
				old := timeutil.Now().Add(-1 * gcSetting.Get(&r.settings.SV))
				if err := r.cleanupOldJobs(ctx, old); err != nil {
					log.Warningf(ctx, "error cleaning up old job records: %v", err)
				}
				lastGC = timeutil.Now()
				timer.Reset(gcInterval())
			}
		}
	}); err != nil {
		return err
	}
	return stopper.RunAsyncTask(context.Background(), "jobs/adopt", func(ctx context.Context) {
		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()
		timer := timeutil.NewTimer()
		defer timer.Stop()
		timer.Reset(adoptInterval)
		for {
			select {
			case <-stopper.ShouldQuiesce():
				return
			case shouldClaim := <-r.adoptionCh:
				// Try to adopt the most recently created job.
				if r.startUsingSQLLivenessAdoption(ctx) {
					if shouldClaim {
						claimJobs(ctx)
					}
					processClaimedJobs(ctx)
				} else {
					// TODO(spaskob): remove in 20.2 as this code path is only needed while
					// migrating to 20.2 cluster.
					maybeAdoptJobsDeprecated(ctx, false /* randomizeJobOrder */)
				}
			case <-timer.C:
				timer.Read = true
				if r.startUsingSQLLivenessAdoption(ctx) {
					claimJobs(ctx)
					processClaimedJobs(ctx)
				} else {
					// TODO(spaskob): remove in 20.2 as this code path is only needed while
					// migrating to 20.2 cluster.
					maybeAdoptJobsDeprecated(ctx, true /* randomizeJobOrder */)
				}
				timer.Reset(adoptInterval)
			}
		}
	})
}

func (r *Registry) maybeCancelJobs(ctx context.Context, s sqlliveness.Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// If the cluster is finalized, kill any remaining legacy jobs. They will be
	// re-adopted with the new epoch based leasing.
	r.deprecatedCancelAllLocked(ctx)

	for id, aj := range r.mu.adoptedJobs {
		if aj.sid != s.ID() {
			log.Warningf(ctx, "job %d: running without having a live claim; killed.", id)
			aj.cancel()
			delete(r.mu.adoptedJobs, id)
		}
	}
}

// TODO(spaskob): remove in 20.2 as this code path is only needed while
// migrating to 20.2 cluster.
func (r *Registry) maybeCancelJobsDeprecated(
	ctx context.Context, nlw optionalnodeliveness.Container,
) {
	nl, ok := nlw.Optional(54251)
	if !ok {
		// At most one container is running on behalf of a SQL tenant, so it must be
		// this one, and there's no point canceling anything.
		//
		// TODO(ajwerner): don't rely on this. Instead fix this issue:
		// https://github.com/cockroachdb/cockroach/issues/47892
		return
	}
	liveness, ok := nl.Self()
	if !ok {
		if nodeLivenessLogLimiter.ShouldLog() {
			log.Warning(ctx, "own liveness record not found")
		}
		// Conservatively assume our lease has expired. Abort all jobs.
		r.deprecatedCancelAll(ctx)
		return
	}

	// If we haven't persisted a liveness record within the leniency
	// interval, we'll cancel all of our jobs.
	if !liveness.IsLive(r.lenientNow()) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.deprecatedCancelAllLocked(ctx)
		r.mu.deprecatedEpoch = liveness.Epoch
		return
	}
}

const cleanupPageSize = 100

func (r *Registry) cleanupOldJobs(ctx context.Context, olderThan time.Time) error {
	var maxID jobspb.JobID
	for {
		var done bool
		var err error
		done, maxID, err = r.cleanupOldJobsPage(ctx, olderThan, maxID, cleanupPageSize)
		if err != nil || done {
			return err
		}
	}
}

// cleanupOldJobsPage deletes up to cleanupPageSize job rows with ID > minID.
// minID is supposed to be the maximum ID returned by the previous page (0 if no
// previous page).
func (r *Registry) cleanupOldJobsPage(
	ctx context.Context, olderThan time.Time, minID jobspb.JobID, pageSize int,
) (done bool, maxID jobspb.JobID, retErr error) {
	const stmt = "SELECT id, payload, status, created FROM system.jobs " +
		"WHERE created < $1 AND id > $2 " +
		"ORDER BY id " + // the ordering is important as we keep track of the maximum ID we've seen
		"LIMIT $3"
	it, err := r.ex.QueryIterator(ctx, "gc-jobs", nil /* txn */, stmt, olderThan, minID, pageSize)
	if err != nil {
		return false, 0, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()
	toDelete := tree.NewDArray(types.Int)
	oldMicros := timeutil.ToUnixMicros(olderThan)

	var ok bool
	var numRows int
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		numRows++
		row := it.Cur()
		payload, err := UnmarshalPayload(row[1])
		if err != nil {
			return false, 0, err
		}
		remove := false
		switch Status(*row[2].(*tree.DString)) {
		case StatusSucceeded, StatusCanceled, StatusFailed:
			remove = payload.FinishedMicros < oldMicros
		}
		if remove {
			toDelete.Array = append(toDelete.Array, row[0])
		}
	}
	if err != nil {
		return false, 0, err
	}
	if numRows == 0 {
		return true, 0, nil
	}

	log.VEventf(ctx, 2, "read potentially expired jobs: %d", numRows)
	if len(toDelete.Array) > 0 {
		log.Infof(ctx, "cleaning up expired job records: %d", len(toDelete.Array))
		const stmt = `DELETE FROM system.jobs WHERE id = ANY($1)`
		var nDeleted int
		if nDeleted, err = r.ex.Exec(
			ctx, "gc-jobs", nil /* txn */, stmt, toDelete,
		); err != nil {
			return false, 0, errors.Wrap(err, "deleting old jobs")
		}
		if nDeleted != len(toDelete.Array) {
			return false, 0, errors.AssertionFailedf("asked to delete %d rows but %d were actually deleted",
				len(toDelete.Array), nDeleted)
		}
	}
	// If we got as many rows as we asked for, there might be more.
	morePages := numRows == pageSize
	// Track the highest ID we encounter, so it can serve as the bottom of the
	// next page.
	lastRow := it.Cur()
	maxID = jobspb.JobID(*(lastRow[0].(*tree.DInt)))
	return !morePages, maxID, nil
}

// getJobFn attempts to get a resumer from the given job id. If the job id
// does not have a resumer then it returns an error message suitable for users.
func (r *Registry) getJobFn(
	ctx context.Context, txn *kv.Txn, id jobspb.JobID,
) (*Job, Resumer, error) {
	job, err := r.LoadJobWithTxn(ctx, id, txn)
	if err != nil {
		return nil, nil, err
	}
	resumer, err := r.createResumer(job, r.settings)
	if err != nil {
		return job, nil, errors.Errorf("job %d is not controllable", id)
	}
	return job, resumer, nil
}

// CancelRequested marks the job as cancel-requested using the specified txn (may be nil).
func (r *Registry) CancelRequested(ctx context.Context, txn *kv.Txn, id jobspb.JobID) error {
	job, _, err := r.getJobFn(ctx, txn, id)
	if err != nil {
		// Special case schema change jobs to mark the job as canceled.
		if job != nil {
			payload := job.Payload()
			// TODO(mjibson): Use an unfortunate workaround to enable canceling of
			// schema change jobs by comparing the string description. When a schema
			// change job fails or is canceled, a new job is created with the ROLL BACK
			// prefix. These rollback jobs cannot be canceled. We could add a field to
			// the payload proto to indicate if this job is cancelable or not, but in
			// a split version cluster an older node could pick up the schema change
			// and fail to clear/set that field appropriately. Thus it seems that the
			// safest way for now (i.e., without a larger jobs/schema change refactor)
			// is to hack this up with a string comparison.
			if payload.Type() == jobspb.TypeSchemaChange && !strings.HasPrefix(payload.Description, "ROLL BACK") {
				return job.cancelRequested(ctx, txn, nil)
			}
		}
		return err
	}
	return job.cancelRequested(ctx, txn, nil)
}

// PauseRequested marks the job with id as paused-requested using the specified txn (may be nil).
func (r *Registry) PauseRequested(ctx context.Context, txn *kv.Txn, id jobspb.JobID) error {
	job, resumer, err := r.getJobFn(ctx, txn, id)
	if err != nil {
		return err
	}
	var onPauseRequested onPauseRequestFunc
	if pr, ok := resumer.(PauseRequester); ok {
		onPauseRequested = pr.OnPauseRequest
	}
	return job.pauseRequested(ctx, txn, onPauseRequested)
}

// Succeeded marks the job with id as succeeded.
func (r *Registry) Succeeded(ctx context.Context, txn *kv.Txn, id jobspb.JobID) error {
	job, _, err := r.getJobFn(ctx, txn, id)
	if err != nil {
		return err
	}
	return job.succeeded(ctx, txn, nil)
}

// Failed marks the job with id as failed.
func (r *Registry) Failed(
	ctx context.Context, txn *kv.Txn, id jobspb.JobID, causingError error,
) error {
	job, _, err := r.getJobFn(ctx, txn, id)
	if err != nil {
		return err
	}
	return job.failed(ctx, txn, causingError, nil)
}

// Unpause changes the paused job with id to running or reverting using the
// specified txn (may be nil).
func (r *Registry) Unpause(ctx context.Context, txn *kv.Txn, id jobspb.JobID) error {
	job, _, err := r.getJobFn(ctx, txn, id)
	if err != nil {
		return err
	}
	return job.unpaused(ctx, txn)
}

// Resumer is a resumable job, and is associated with a Job object. Jobs can be
// paused or canceled at any time. Jobs should call their CheckStatus() or
// Progressed() method, which will return an error if the job has been paused or
// canceled.
//
// Resumers are created through registered Constructor functions.
//
type Resumer interface {
	// Resume is called when a job is started or resumed. execCtx is a sql.JobExecCtx.
	Resume(ctx context.Context, execCtx interface{}) error

	// OnFailOrCancel is called when a job fails or is cancel-requested.
	//
	// This method will be called when a registry notices the cancel request,
	// which is not guaranteed to run on the node where the job is running. So it
	// cannot assume that any other methods have been called on this Resumer
	// object.
	OnFailOrCancel(ctx context.Context, execCtx interface{}) error
}

// PauseRequester is an extension of Resumer which allows job implementers to inject
// logic during the transaction which moves a job to PauseRequested.
type PauseRequester interface {
	Resumer

	// OnPauseRequest is called in the transaction that moves a job to PauseRequested.
	// If an error is returned, the pause request will fail. execCtx is a
	// sql.JobExecCtx.
	OnPauseRequest(ctx context.Context, execCtx interface{}, txn *kv.Txn, details *jobspb.Progress) error
}

// JobResultsReporter is an interface for reporting the results of the job execution.
// Resumer implementations may also implement this interface if they wish to return
// data to the user upon successful completion.
type JobResultsReporter interface {
	// ReportResults sends job results on the specified channel.
	// This method will only be called if Resume() ran successfully to completion.
	ReportResults(ctx context.Context, resultsCh chan<- tree.Datums) error
}

// Constructor creates a resumable job of a certain type. The Resumer is
// created on the coordinator each time the job is started/resumed, so it can
// hold state. The Resume method is always ran, and can set state on the Resumer
// that can be used by the other methods.
type Constructor func(job *Job, settings *cluster.Settings) Resumer

var constructors = make(map[jobspb.Type]Constructor)

// RegisterConstructor registers a Resumer constructor for a certain job type.
func RegisterConstructor(typ jobspb.Type, fn Constructor) {
	constructors[typ] = fn
}

func (r *Registry) createResumer(job *Job, settings *cluster.Settings) (Resumer, error) {
	payload := job.Payload()
	fn := constructors[payload.Type()]
	if fn == nil {
		return nil, errors.Errorf("no resumer is available for %s", payload.Type())
	}
	if wrapper := r.TestingResumerCreationKnobs[payload.Type()]; wrapper != nil {
		return wrapper(fn(job, settings)), nil
	}
	return fn(job, settings), nil
}

type retryJobError string

// retryJobErrorSentinel exists so the errors returned from NewRetryJobError can
// be marked with it, allowing more robust detection of retry errors even if
// they are wrapped, etc. This was originally introduced to deal with injected
// retry errors from testing knobs.
var retryJobErrorSentinel = retryJobError("")

// NewRetryJobError creates a new error that, if returned by a Resumer,
// indicates to the jobs registry that the job should be restarted in the
// background.
func NewRetryJobError(s string) error {
	return errors.Mark(retryJobError(s), retryJobErrorSentinel)
}

func (r retryJobError) Error() string {
	return string(r)
}

// stepThroughStateMachine implements the state machine of the job lifecycle.
// The job is executed with the ctx, so ctx must only be canceled if the job
// should also be canceled. resultsCh is passed to the resumable func and should
// be closed by the caller after errCh sends a value. errCh returns an error if
// the job was not completed with success. status is the current job status.
func (r *Registry) stepThroughStateMachine(
	ctx context.Context, execCtx interface{}, resumer Resumer, job *Job, status Status, jobErr error,
) error {
	payload := job.Payload()
	jobType := payload.Type()
	log.Infof(ctx, "%s job %d: stepping through state %s with error: %+v", jobType, job.ID(), status, jobErr)
	jm := r.metrics.JobMetrics[jobType]
	switch status {
	case StatusRunning:
		if jobErr != nil {
			return errors.NewAssertionErrorWithWrappedErrf(jobErr,
				"job %d: resuming with non-nil error", job.ID())
		}
		resumeCtx := logtags.AddTag(ctx, "job", job.ID())
		if payload.StartedMicros == 0 {
			if err := job.started(ctx, nil /* txn */); err != nil {
				return err
			}
		}
		var err error
		func() {
			jm.CurrentlyRunning.Inc(1)
			defer jm.CurrentlyRunning.Dec(1)
			err = resumer.Resume(resumeCtx, execCtx)
		}()
		if err == nil {
			jm.ResumeCompleted.Inc(1)
			return r.stepThroughStateMachine(ctx, execCtx, resumer, job, StatusSucceeded, nil)
		}
		if resumeCtx.Err() != nil {
			// The context was canceled. Tell the user, but don't attempt to
			// mark the job as failed because it can be resumed by another node.
			//
			// TODO(ajwerner): We'll also end up here if the job was canceled or
			// paused. We should make this error clearer.
			jm.ResumeRetryError.Inc(1)
			return errors.Errorf("job %d: node liveness error: restarting in background", job.ID())
		}
		// TODO(spaskob): enforce a limit on retries.
		// TODO(spaskob,lucy): Add metrics on job retries. Consider having a backoff
		// mechanism (possibly combined with a retry limit).
		if errors.Is(err, retryJobErrorSentinel) {
			jm.ResumeRetryError.Inc(1)
			return errors.Errorf("job %d: %s: restarting in background", job.ID(), err)
		}
		jm.ResumeFailed.Inc(1)
		if sErr := (*InvalidStatusError)(nil); errors.As(err, &sErr) {
			if sErr.status != StatusCancelRequested && sErr.status != StatusPauseRequested {
				return errors.NewAssertionErrorWithWrappedErrf(sErr,
					"job %d: unexpected status %s provided for a running job", job.ID(), sErr.status)
			}
			return sErr
		}
		return r.stepThroughStateMachine(ctx, execCtx, resumer, job, StatusReverting, err)
	case StatusPauseRequested:
		return errors.Errorf("job %s", status)
	case StatusCancelRequested:
		return errors.Errorf("job %s", status)
	case StatusPaused:
		return errors.NewAssertionErrorWithWrappedErrf(jobErr,
			"job %d: unexpected status %s provided to state machine", job.ID(), status)
	case StatusCanceled:
		if err := job.canceled(ctx, nil /* txn */, nil /* fn */); err != nil {
			// If we can't transactionally mark the job as canceled then it will be
			// restarted during the next adopt loop and reverting will be retried.
			return errors.Wrapf(err, "job %d: could not mark as canceled: %v", job.ID(), jobErr)
		}
		telemetry.Inc(TelemetryMetrics[jobType].Canceled)
		return errors.WithSecondaryError(errors.Errorf("job %s", status), jobErr)
	case StatusSucceeded:
		if jobErr != nil {
			return errors.NewAssertionErrorWithWrappedErrf(jobErr,
				"job %d: successful bu unexpected error provided", job.ID())
		}
		if err := job.succeeded(ctx, nil /* txn */, nil /* fn */); err != nil {
			// If it didn't succeed, we consider the job as failed and need to go
			// through reverting state first.
			// TODO(spaskob): this is silly, we should remove the OnSuccess hooks and
			// execute them in resume so that the client can handle these errors
			// better.
			return r.stepThroughStateMachine(ctx, execCtx, resumer, job, StatusReverting, errors.Wrapf(err, "could not mark job %d as succeeded", job.ID()))
		}
		telemetry.Inc(TelemetryMetrics[jobType].Successful)
		return nil
	case StatusReverting:
		if err := job.reverted(ctx, nil /* txn */, jobErr, nil /* fn */); err != nil {
			// If we can't transactionally mark the job as reverting then it will be
			// restarted during the next adopt loop and it will be retried.
			return errors.Wrapf(err, "job %d: could not mark as reverting: %s", job.ID(), jobErr)
		}
		onFailOrCancelCtx := logtags.AddTag(ctx, "job", job.ID())
		var err error
		func() {
			jm.CurrentlyRunning.Inc(1)
			defer jm.CurrentlyRunning.Dec(1)
			err = resumer.OnFailOrCancel(onFailOrCancelCtx, execCtx)
		}()
		if successOnFailOrCancel := err == nil; successOnFailOrCancel {
			jm.FailOrCancelCompleted.Inc(1)
			// If the job has failed with any error different than canceled we
			// mark it as Failed.
			nextStatus := StatusFailed
			if HasErrJobCanceled(jobErr) {
				nextStatus = StatusCanceled
			}
			return r.stepThroughStateMachine(ctx, execCtx, resumer, job, nextStatus, jobErr)
		}
		if onFailOrCancelCtx.Err() != nil {
			jm.FailOrCancelRetryError.Inc(1)
			// The context was canceled. Tell the user, but don't attempt to
			// mark the job as failed because it can be resumed by another node.
			return errors.Errorf("job %d: node liveness error: restarting in background", job.ID())
		}
		if errors.Is(err, retryJobErrorSentinel) {
			jm.FailOrCancelRetryError.Inc(1)
			return errors.Errorf("job %d: %s: restarting in background", job.ID(), err)
		}
		jm.FailOrCancelFailed.Inc(1)
		if sErr := (*InvalidStatusError)(nil); errors.As(err, &sErr) {
			if sErr.status != StatusPauseRequested {
				return errors.NewAssertionErrorWithWrappedErrf(jobErr,
					"job %d: unexpected status %s provided for a reverting job", job.ID(), sErr.status)
			}
			return sErr
		}
		return r.stepThroughStateMachine(ctx, execCtx, resumer, job, StatusFailed,
			errors.Wrapf(err, "job %d: cannot be reverted, manual cleanup may be required", job.ID()))
	case StatusFailed:
		if jobErr == nil {
			return errors.AssertionFailedf("job %d: has StatusFailed but no error was provided", job.ID())
		}
		if err := job.failed(ctx, nil /* txn */, jobErr, nil /* fn */); err != nil {
			// If we can't transactionally mark the job as failed then it will be
			// restarted during the next adopt loop and reverting will be retried.
			return errors.Wrapf(err, "job %d: could not mark as failed: %s", job.ID(), jobErr)
		}
		telemetry.Inc(TelemetryMetrics[jobType].Failed)
		return jobErr
	default:
		return errors.NewAssertionErrorWithWrappedErrf(jobErr,
			"job %d: has unsupported status %s", job.ID(), status)
	}
}

func (r *Registry) adoptionDisabled(ctx context.Context) bool {
	if r.preventAdoptionFile != "" {
		if _, err := os.Stat(r.preventAdoptionFile); err != nil {
			if !oserror.IsNotExist(err) {
				log.Warningf(ctx, "error checking if job adoption is currently disabled: %v", err)
			}
			return false
		}
		log.Warningf(ctx, "job adoption is currently disabled by existence of %s", r.preventAdoptionFile)
		return true
	}
	return false
}

func (r *Registry) cancelAllAdoptedJobs() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, aj := range r.mu.adoptedJobs {
		aj.cancel()
	}
	r.mu.adoptedJobs = make(map[jobspb.JobID]*adoptedJob)
}

func (r *Registry) unregister(jobID jobspb.JobID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cancel, ok := r.mu.deprecatedJobs[jobID]
	// It is possible for a job to be double unregistered. unregister is always
	// called at the end of resume. But it can also be called during deprecatedCancelAll
	// and in the adopt loop under certain circumstances.
	if ok {
		cancel()
		delete(r.mu.deprecatedJobs, jobID)
		return
	}
	aj, ok := r.mu.adoptedJobs[jobID]
	// It is possible for a job to be double unregistered. unregister is always
	// called at the end of resume. But it can also be called during deprecatedCancelAll
	// and in the adopt loop under certain circumstances.
	if ok {
		aj.cancel()
		delete(r.mu.adoptedJobs, jobID)
	}
}

// TestingNudgeAdoptionQueue is used by tests to tell the registry that there is
// a job to be adopted.
func (r *Registry) TestingNudgeAdoptionQueue() {
	r.adoptionCh <- claimAndResumeClaimedJobs
}

// TestingCreateAndStartJob creates and asynchronously starts a job from record.
// An error is returned if the job type has not been registered with
// RegisterConstructor. The ctx passed to this function is not the context the
// job will be started with (canceling ctx will not cause the job to cancel).
func TestingCreateAndStartJob(
	ctx context.Context, r *Registry, db *kv.DB, record Record,
) (*StartableJob, error) {
	var rj *StartableJob
	jobID := r.MakeJobID()
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		return r.CreateStartableJobWithTxn(ctx, &rj, jobID, txn, record)
	}); err != nil {
		if rj != nil {
			if cleanupErr := rj.CleanupOnRollback(ctx); cleanupErr != nil {
				log.Warningf(ctx, "failed to cleanup StartableJob: %v", cleanupErr)
			}
		}
		return nil, err
	}
	err := rj.Start(ctx)
	if err != nil {
		return nil, err
	}
	return rj, nil
}
