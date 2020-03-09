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
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storagepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	opentracing "github.com/opentracing/opentracing-go"
)

const defaultLeniencySetting = 60 * time.Second

var (
	nodeLivenessLogLimiter = log.Every(5 * time.Second)
	// LeniencySetting is the amount of time to defer any attempts to
	// reschedule a job.  Visible for testing.
	LeniencySetting = settings.RegisterDurationSetting(
		"jobs.registry.leniency",
		"the amount of time to defer any attempts to reschedule a job",
		defaultLeniencySetting)
	gcSetting = settings.RegisterDurationSetting(
		"jobs.retention_time",
		"the amount of time to retain records for completed jobs before",
		time.Hour*24*14)
)

// NodeLiveness is the subset of storage.NodeLiveness's interface needed
// by Registry.
type NodeLiveness interface {
	Self() (storagepb.Liveness, error)
	GetLivenesses() []storagepb.Liveness
}

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
	ac         log.AmbientContext
	stopper    *stop.Stopper
	db         *kv.DB
	ex         sqlutil.InternalExecutor
	clock      *hlc.Clock
	nodeID     *base.NodeIDContainer
	settings   *cluster.Settings
	planFn     planHookMaker
	metrics    Metrics
	adoptionCh chan struct{}

	// if non-empty, indicates path to file that prevents any job adoptions.
	preventAdoptionFile string

	mu struct {
		syncutil.Mutex
		// epoch is present to support older nodes that are not using
		// the timestamp-based approach to determine when to steal jobs.
		// TODO: Remove this and deprecate Lease.Epoch proto field
		epoch int64
		// jobs holds a map from job id to its context cancel func. This should
		// be populated with jobs that are currently being run (and owned) by
		// this registry. Calling the func will cancel the context the job was
		// started/resumed with. This should only be called by the registry when
		// it is attempting to halt its own jobs due to liveness problems. Jobs
		// are normally canceled on any node by the CANCEL JOB statement, which is
		// propagated to jobs via the .Progressed call. This function should not be
		// used to cancel a job in that way.
		jobs map[int64]context.CancelFunc
	}

	TestingResumerCreationKnobs map[jobspb.Type]func(Resumer) Resumer
}

// planHookMaker is a wrapper around sql.NewInternalPlanner. It returns an
// *sql.planner as an interface{} due to package dependency cycles. It should
// be cast to that type in the sql package when it is used. Returns a cleanup
// function that must be called once the caller is done with the planner.
//
// TODO(mjibson): Can we do something to avoid passing an interface{} here
// that must be type casted in a Resumer? It cannot be done here because
// PlanHookState lives in the sql package, which would create a dependency
// cycle if listed here. Furthermore, moving PlanHookState into a common
// subpackage like sqlbase is difficult because of the amount of sql-only
// stuff that PlanHookState exports. One other choice is to merge this package
// back into the sql package. There's maybe a better way that I'm unaware of.
type planHookMaker func(opName, user string) (interface{}, func())

// PreventAdoptionFile is the name of the file which, if present in the first
// on-disk store, will prevent the adoption of background jobs by that node.
const PreventAdoptionFile = "DISABLE_STARTING_BACKGROUND_JOBS"

// MakeRegistry creates a new Registry. planFn is a wrapper around
// sql.newInternalPlanner. It returns a sql.PlanHookState, but must be
// coerced into that in the Resumer functions.
func MakeRegistry(
	ac log.AmbientContext,
	stopper *stop.Stopper,
	clock *hlc.Clock,
	db *kv.DB,
	ex sqlutil.InternalExecutor,
	nodeID *base.NodeIDContainer,
	settings *cluster.Settings,
	histogramWindowInterval time.Duration,
	planFn planHookMaker,
	preventAdoptionFile string,
) *Registry {
	r := &Registry{
		ac:                  ac,
		stopper:             stopper,
		clock:               clock,
		db:                  db,
		ex:                  ex,
		nodeID:              nodeID,
		settings:            settings,
		planFn:              planFn,
		preventAdoptionFile: preventAdoptionFile,
		adoptionCh:          make(chan struct{}),
	}
	r.mu.epoch = 1
	r.mu.jobs = make(map[int64]context.CancelFunc)
	r.metrics.InitHooks(histogramWindowInterval)
	return r
}

// MetricsStruct returns the metrics for production monitoring of each job type.
// They're all stored as the `metric.Struct` interface because of dependency
// cycles.
func (r *Registry) MetricsStruct() *Metrics {
	return &r.metrics
}

// CurrentlyRunningJobs returns a slice of the ids of all jobs running on this node.
func (r *Registry) CurrentlyRunningJobs() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	jobs := make([]int64, len(r.mu.jobs))
	i := 0
	for jID := range r.mu.jobs {
		jobs[i] = jID
		i++
	}
	return jobs
}

// lenientNow returns the timestamp after which we should attempt
// to steal a job from a node whose liveness is failing.  This allows
// jobs coordinated by a node which is temporarily saturated to continue.
func (r *Registry) lenientNow() time.Time {
	// We see this in tests.
	var offset time.Duration
	if r.settings == cluster.NoSettings {
		offset = defaultLeniencySetting
	} else {
		offset = LeniencySetting.Get(&r.settings.SV)
	}

	return r.clock.Now().GoTime().Add(-offset)
}

// makeCtx returns a new context from r's ambient context and an associated
// cancel func.
func (r *Registry) makeCtx() (context.Context, func()) {
	return context.WithCancel(r.ac.AnnotateCtx(context.Background()))
}

func (r *Registry) makeJobID() int64 {
	return int64(builtins.GenerateUniqueInt(r.nodeID.Get()))
}

// CreateAndStartJob creates and asynchronously starts a job from record. An
// error is returned if the job type has not been registered with
// RegisterConstructor. The ctx passed to this function is not the context the
// job will be started with (canceling ctx will not cause the job to cancel).
func (r *Registry) CreateAndStartJob(
	ctx context.Context, resultsCh chan<- tree.Datums, record Record,
) (*Job, <-chan error, error) {
	var rj *StartableJob
	if err := r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		rj, err = r.CreateStartableJobWithTxn(ctx, record, txn, resultsCh)
		return err
	}); err != nil {
		return nil, nil, err
	}
	errCh, err := rj.Start(ctx)
	if err != nil {
		return nil, nil, err
	}
	return rj.Job, errCh, nil
}

// Run starts previously unstarted jobs from a list of scheduled
// jobs. Canceling ctx interrupts the waiting but doesn't cancel the jobs.
func (r *Registry) Run(ctx context.Context, ex sqlutil.InternalExecutor, jobs []int64) error {
	if len(jobs) == 0 {
		return nil
	}
	log.Infof(ctx, "scheduled jobs %+v", jobs)
	buf := bytes.Buffer{}
	for i, id := range jobs {
		select {
		case r.adoptionCh <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}

		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf(" (%d)", id))
	}
	// Manually retry instead of using SHOW JOBS WHEN COMPLETE so we have greater
	// control over retries.
	query := fmt.Sprintf(
		"SELECT count(*) FROM [SHOW JOBS VALUES %s] WHERE finished IS NULL", buf.String())
	for r := retry.StartWithCtx(ctx, retry.Options{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		Multiplier:     2,
	}); r.Next(); {
		// We poll the number of queued jobs that aren't finished. As with SHOW JOBS
		// WHEN COMPLETE, if one of the jobs is missing from the jobs table for
		// whatever reason, we'll fail later when we try to load the job.
		row, err := ex.QueryRowEx(
			ctx,
			"poll-show-jobs",
			nil, /* txn */
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			query,
		)
		if err != nil {
			return errors.Wrap(err, "polling for queued jobs to complete")
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
			return errors.Wrapf(err, "Job %d could not be loaded. The job may not have succeeded", jobs[i])
		}
		if j.Payload().Error != "" {
			return errors.New(fmt.Sprintf("Job %d failed with error %s", jobs[i], j.Payload().Error))
		}
	}
	return nil
}

// NewJob creates a new Job.
func (r *Registry) NewJob(record Record) *Job {
	job := &Job{
		registry: r,
	}
	job.mu.payload = jobspb.Payload{
		Description:   record.Description,
		Statement:     record.Statement,
		Username:      record.Username,
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

// CreateJobWithTxn creates a job to be started later with StartJob.
// It stores the job in the jobs table, marks it pending and gives the
// current node a lease.
func (r *Registry) CreateJobWithTxn(ctx context.Context, record Record, txn *kv.Txn) (*Job, error) {
	j := r.NewJob(record)
	if err := j.WithTxn(txn).insert(ctx, r.makeJobID(), r.newLease()); err != nil {
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
func (r *Registry) CreateStartableJobWithTxn(
	ctx context.Context, record Record, txn *kv.Txn, resultsCh chan<- tree.Datums,
) (*StartableJob, error) {
	j, err := r.CreateJobWithTxn(ctx, record, txn)
	if err != nil {
		return nil, err
	}
	// The job itself must not hold on to this transaction. We ensure in Start()
	// that the transaction used to create the job is committed. When jobs hold
	// onto transactions they use the transaction in methods which modify the job.
	// On the whole this pattern is bug-prone and hard to reason about.
	j.WithTxn(nil)
	resumer, err := r.createResumer(j, r.settings)
	if err != nil {
		return nil, err
	}
	resumerCtx, cancel := r.makeCtx()
	if err := r.register(*j.ID(), cancel); err != nil {
		return nil, err
	}
	return &StartableJob{
		Job:        j,
		txn:        txn,
		resumer:    resumer,
		resumerCtx: resumerCtx,
		cancel:     cancel,
		resultsCh:  resultsCh,
	}, nil
}

// LoadJob loads an existing job with the given jobID from the system.jobs
// table.
func (r *Registry) LoadJob(ctx context.Context, jobID int64) (*Job, error) {
	return r.LoadJobWithTxn(ctx, jobID, nil)
}

// LoadJobWithTxn does the same as above, but using the transaction passed in
// the txn argument. Passing a nil transaction is equivalent to calling LoadJob
// in that a transaction will be automatically created.
func (r *Registry) LoadJobWithTxn(ctx context.Context, jobID int64, txn *kv.Txn) (*Job, error) {
	j := &Job{
		id:       &jobID,
		registry: r,
	}
	if err := j.WithTxn(txn).load(ctx); err != nil {
		return nil, err
	}
	return j, nil
}

// DefaultCancelInterval is a reasonable interval at which to poll this node
// for liveness failures and cancel running jobs.
var DefaultCancelInterval = base.DefaultTxnHeartbeatInterval

// DefaultAdoptInterval is a reasonable interval at which to poll system.jobs
// for jobs with expired leases.
//
// DefaultAdoptInterval is mutable for testing. NB: Updates to this value after
// Registry.Start has been called will not have any effect.
var DefaultAdoptInterval = 30 * time.Second

var maxAdoptionsPerLoop = envutil.EnvOrDefaultInt(`COCKROACH_JOB_ADOPTIONS_PER_PERIOD`, 10)

// gcInterval is how often we check for and delete job records older than the
// retention limit.
const gcInterval = 1 * time.Hour

// Start polls the current node for liveness failures and cancels all registered
// jobs if it observes a failure. Otherwise it starts all the main daemons of
// registry that poll the jobs table and start/cancel/gc jobs.
func (r *Registry) Start(
	ctx context.Context,
	stopper *stop.Stopper,
	nl NodeLiveness,
	cancelInterval, adoptInterval time.Duration,
) error {
	// Calling maybeCancelJobs once at the start ensures we have an up-to-date
	// liveness epoch before we wait out the first cancelInterval.
	r.maybeCancelJobs(ctx, nl)

	stopper.RunWorker(context.Background(), func(ctx context.Context) {
		for {
			select {
			case <-stopper.ShouldStop():
				return
			case <-time.After(cancelInterval):
				r.maybeCancelJobs(ctx, nl)
			}
		}
	})

	stopper.RunWorker(context.Background(), func(ctx context.Context) {
		for {
			select {
			case <-stopper.ShouldStop():
				return
			case <-time.After(gcInterval):
				old := timeutil.Now().Add(-1 * gcSetting.Get(&r.settings.SV))
				if err := r.cleanupOldJobs(ctx, old); err != nil {
					log.Warningf(ctx, "error cleaning up old job records: %v", err)
				}
			}
		}
	})

	maybeAdoptJobs := func(ctx context.Context) {
		if r.adoptionDisabled(ctx) {
			r.cancelAll(ctx)
			return
		}
		if err := r.maybeAdoptJob(ctx, nl); err != nil {
			log.Errorf(ctx, "error while adopting jobs: %s", err)
		}
	}

	stopper.RunWorker(context.Background(), func(ctx context.Context) {
		for {
			select {
			case <-stopper.ShouldStop():
				return
			case <-r.adoptionCh:
				maybeAdoptJobs(ctx)
			case <-time.After(adoptInterval):
				maybeAdoptJobs(ctx)
			}
		}
	})
	return nil
}

func (r *Registry) maybeCancelJobs(ctx context.Context, nl NodeLiveness) {
	liveness, err := nl.Self()
	if err != nil {
		if nodeLivenessLogLimiter.ShouldLog() {
			log.Warningf(ctx, "unable to get node liveness: %s", err)
		}
		// Conservatively assume our lease has expired. Abort all jobs.
		r.mu.Lock()
		defer r.mu.Unlock()
		r.cancelAll(ctx)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// If we haven't persisted a liveness record within the leniency
	// interval, we'll cancel all of our jobs.
	if !liveness.IsLive(r.lenientNow()) {
		r.cancelAll(ctx)
		r.mu.epoch = liveness.Epoch
		return
	}

	// Finally, we cancel all jobs if the stopper is quiescing.
	select {
	case <-r.stopper.ShouldQuiesce():
		r.cancelAll(ctx)
	default:
	}
}

// isOrphaned tries to detect if there are no mutations left to be done for the
// job which will make it a candidate for garbage collection. Jobs can be left
// in such inconsistent state if they fail before being removed from the jobs table.
func (r *Registry) isOrphaned(ctx context.Context, payload *jobspb.Payload) (bool, error) {
	if payload.Type() != jobspb.TypeSchemaChange {
		return false, nil
	}
	for _, id := range payload.DescriptorIDs {
		pendingMutations := false
		if err := r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			td, err := sqlbase.GetTableDescFromID(ctx, txn, id)
			if err != nil {
				return err
			}
			hasAnyMutations := len(td.GetMutations()) != 0 || len(td.GetGCMutations()) != 0
			hasDropJob := td.DropJobID != 0
			pendingMutations = hasAnyMutations || hasDropJob
			return nil
		}); err != nil {
			if errors.Is(err, sqlbase.ErrDescriptorNotFound) {
				// Treat missing table descriptors as no longer relevant for the
				// job payload. See
				// https://github.com/cockroachdb/cockroach/45399.
				continue
			}
			return false, err
		}
		if pendingMutations {
			return false, nil
		}
	}
	return true, nil
}

func (r *Registry) cleanupOldJobs(ctx context.Context, olderThan time.Time) error {
	const stmt = `SELECT id, payload, status, created FROM system.jobs WHERE created < $1
		      ORDER BY created LIMIT 1000`
	rows, err := r.ex.Query(ctx, "gc-jobs", nil /* txn */, stmt, olderThan)
	if err != nil {
		return err
	}

	toDelete := tree.NewDArray(types.Int)
	toDelete.Array = make(tree.Datums, 0, len(rows))
	oldMicros := timeutil.ToUnixMicros(olderThan)
	for _, row := range rows {
		payload, err := UnmarshalPayload(row[1])
		if err != nil {
			return err
		}
		remove := false
		switch Status(*row[2].(*tree.DString)) {
		case StatusRunning, StatusPending:
			done, err := r.isOrphaned(ctx, payload)
			if err != nil {
				return err
			}
			remove = done && row[3].(*tree.DTimestamp).Time.Before(olderThan)
		case StatusSucceeded, StatusCanceled, StatusFailed:
			remove = payload.FinishedMicros < oldMicros
		}
		if remove {
			toDelete.Array = append(toDelete.Array, row[0])
		}
	}
	if len(toDelete.Array) > 0 {
		log.Infof(ctx, "cleaning up %d expired job records", len(toDelete.Array))
		const stmt = `DELETE FROM system.jobs WHERE id = ANY($1)`
		var nDeleted int
		if nDeleted, err = r.ex.Exec(
			ctx, "gc-jobs", nil /* txn */, stmt, toDelete,
		); err != nil {
			return errors.Wrap(err, "deleting old jobs")
		}
		if nDeleted != len(toDelete.Array) {
			return errors.Errorf("asked to delete %d rows but %d were actually deleted",
				len(toDelete.Array), nDeleted)
		}
	}
	return nil
}

// getJobFn attempts to get a resumer from the given job id. If the job id
// does not have a resumer then it returns an error message suitable for users.
func (r *Registry) getJobFn(ctx context.Context, txn *kv.Txn, id int64) (*Job, Resumer, error) {
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
func (r *Registry) CancelRequested(ctx context.Context, txn *kv.Txn, id int64) error {
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
				return job.WithTxn(txn).cancelRequested(ctx, nil)
			}
		}
		return err
	}
	return job.WithTxn(txn).cancelRequested(ctx, nil)
}

// PauseRequested marks the job with id as paused-requested using the specified txn (may be nil).
func (r *Registry) PauseRequested(ctx context.Context, txn *kv.Txn, id int64) error {
	job, _, err := r.getJobFn(ctx, txn, id)
	if err != nil {
		return err
	}
	return job.WithTxn(txn).pauseRequested(ctx, nil)
}

// Resume resumes the paused job with id using the specified txn (may be nil).
func (r *Registry) Resume(ctx context.Context, txn *kv.Txn, id int64) error {
	job, _, err := r.getJobFn(ctx, txn, id)
	if err != nil {
		return err
	}
	return job.WithTxn(txn).resumed(ctx)
}

// Resumer is a resumable job, and is associated with a Job object. Jobs can be
// paused or canceled at any time. Jobs should call their CheckStatus() or
// Progressed() method, which will return an error if the job has been paused or
// canceled.
//
// Resumers are created through registered Constructor functions.
//
type Resumer interface {
	// Resume is called when a job is started or resumed. Sending results on the
	// chan will return them to a user, if a user's session is connected. phs
	// is a sql.PlanHookState.
	Resume(ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums) error

	// OnFailOrCancel is called when a job fails or is cancel-requested.
	//
	// This method will be called when a registry notices the cancel request,
	// which is not guaranteed to run on the node where the job is running. So it
	// cannot assume that any other methods have been called on this Resumer
	// object.
	OnFailOrCancel(ctx context.Context, phs interface{}) error
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

// NewRetryJobError creates a new error that, if returned by a Resumer,
// indicates to the jobs registry that the job should be restarted in the
// background.
func NewRetryJobError(s string) error {
	return retryJobError(s)
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
	ctx context.Context,
	phs interface{},
	resumer Resumer,
	resultsCh chan<- tree.Datums,
	job *Job,
	status Status,
	jobErr error,
) error {
	log.Infof(ctx, "job %d: stepping through state %s with error %v", *job.ID(), status, jobErr)
	switch status {
	case StatusRunning:
		if jobErr != nil {
			errorMsg := fmt.Sprintf("job %d: resuming with non-nil error: %v", *job.ID(), jobErr)
			return errors.NewAssertionErrorWithWrappedErrf(jobErr, errorMsg)
		}
		resumeCtx := logtags.AddTag(ctx, "job", *job.ID())
		err := resumer.Resume(resumeCtx, phs, resultsCh)
		if err == nil {
			return r.stepThroughStateMachine(ctx, phs, resumer, resultsCh, job, StatusSucceeded, nil)
		}
		if resumeCtx.Err() != nil {
			// The context was canceled. Tell the user, but don't attempt to
			// mark the job as failed because it can be resumed by another node.
			return errors.Errorf("job %d: node liveness error: restarting in background", *job.ID())
		}
		// TODO(spaskob): enforce a limit on retries.
		if e, ok := err.(retryJobError); ok {
			return errors.Errorf("job %d: %s: restarting in background", *job.ID(), e)
		}
		if err, ok := errors.Cause(err).(*InvalidStatusError); ok {
			if err.status != StatusCancelRequested && err.status != StatusPauseRequested {
				errorMsg := fmt.Sprintf("job %d: unexpected status %s provided for a running job", *job.ID(), err.status)
				return errors.NewAssertionErrorWithWrappedErrf(jobErr, errorMsg)
			}
			return err
		}
		return r.stepThroughStateMachine(ctx, phs, resumer, resultsCh, job, StatusReverting, err)
	case StatusPauseRequested:
		return errors.Errorf("job %s", status)
	case StatusCancelRequested:
		return errors.Errorf("job %s", status)
	case StatusPaused:
		errorMsg := fmt.Sprintf("job %d: unexpected status %s provided to state machine", *job.ID(), status)
		return errors.NewAssertionErrorWithWrappedErrf(jobErr, errorMsg)
	case StatusCanceled:
		if err := job.canceled(ctx, nil); err != nil {
			// If we can't transactionally mark the job as canceled then it will be
			// restarted during the next adopt loop and reverting will be retried.
			return errors.Wrapf(err, "job %d: could not mark as canceled: %s", *job.ID(), jobErr)
		}
		return errors.Errorf("job %s", status)
	case StatusSucceeded:
		if jobErr != nil {
			errorMsg := fmt.Sprintf("job %d: successful bu unexpected error provided", *job.ID())
			return errors.NewAssertionErrorWithWrappedErrf(jobErr, errorMsg)
		}
		if err := job.Succeeded(ctx, nil); err != nil {
			// If it didn't succeed, we consider the job as failed and need to go
			// through reverting state first.
			// TODO(spaskob): this is silly, we should remove the OnSuccess hooks and
			// execute them in resume so that the client can handle these errors
			// better.
			return r.stepThroughStateMachine(ctx, phs, resumer, resultsCh, job, StatusReverting, errors.Wrapf(err, "could not mark job %d as succeeded", *job.ID()))
		}
		return nil
	case StatusReverting:
		if err := job.Reverted(ctx, jobErr, nil); err != nil {
			// If we can't transactionally mark the job as reverting then it will be
			// restarted during the next adopt loop and it will be retried.
			return errors.Wrapf(err, "job %d: could not mark as reverting: %s", *job.ID(), jobErr)
		}
		onFailOrCancelCtx := logtags.AddTag(ctx, "job", *job.ID())
		err := resumer.OnFailOrCancel(onFailOrCancelCtx, phs)
		if successOnFailOrCancel := err == nil; successOnFailOrCancel {
			// If the job has failed with any error different than canceled we
			// mark it as Failed.
			nextStatus := StatusFailed
			if errors.Is(errJobCanceled, jobErr) {
				nextStatus = StatusCanceled
			}
			return r.stepThroughStateMachine(ctx, phs, resumer, resultsCh, job, nextStatus, jobErr)
		}
		if onFailOrCancelCtx.Err() != nil {
			// The context was canceled. Tell the user, but don't attempt to
			// mark the job as failed because it can be resumed by another node.
			return errors.Errorf("job %d: node liveness error: restarting in background", *job.ID())
		}
		if e, ok := err.(retryJobError); ok {
			return errors.Errorf("job %d: %s: restarting in background", *job.ID(), e)
		}
		if err, ok := errors.Cause(err).(*InvalidStatusError); ok {
			if err.status != StatusPauseRequested {
				errorMsg := fmt.Sprintf("job %d: unexpected status %s provided for a reverting job", job.ID(), err.status)
				return errors.NewAssertionErrorWithWrappedErrf(jobErr, errorMsg)
			}
			return err
		}
		return r.stepThroughStateMachine(ctx, phs, resumer, resultsCh, job, StatusFailed, errors.Wrapf(err, "job %d: cannot be reverted, manual cleanup may be required", *job.ID()))
	case StatusFailed:
		if jobErr == nil {
			errorMsg := fmt.Sprintf("job %d: has StatusFailed but no error was provided", *job.ID())
			return errors.NewAssertionErrorWithWrappedErrf(jobErr, errorMsg)
		}
		if err := job.Failed(ctx, jobErr, nil); err != nil {
			// If we can't transactionally mark the job as failed then it will be
			// restarted during the next adopt loop and reverting will be retried.
			return errors.Wrapf(err, "job %d: could not mark as failed: %s", *job.ID(), jobErr)
		}
		return jobErr
	default:
		return errors.AssertionFailedf("job %d: has unsupported status %s", *job.ID(), status)
	}
}

// resume starts or resumes a job. If no error is returned then the job was
// asynchronously executed. The job is executed with the ctx, so ctx must
// only by canceled if the job should also be canceled. resultsCh is passed
// to the resumable func and should be closed by the caller after errCh sends
// a value.
func (r *Registry) resume(
	ctx context.Context, resumer Resumer, resultsCh chan<- tree.Datums, job *Job,
) (<-chan error, error) {
	errCh := make(chan error, 1)
	taskName := fmt.Sprintf(`job-%d`, *job.ID())
	if err := r.stopper.RunAsyncTask(ctx, taskName, func(ctx context.Context) {
		// Bookkeeping.
		payload := job.Payload()
		phs, cleanup := r.planFn("resume-"+taskName, payload.Username)
		defer cleanup()
		spanName := fmt.Sprintf(`%s-%d`, payload.Type(), *job.ID())
		var span opentracing.Span
		ctx, span = r.ac.AnnotateCtxWithSpan(ctx, spanName)
		defer span.Finish()

		// Run the actual job.
		status, err := job.CurrentStatus(ctx)
		if err == nil {
			var finalResumeError error
			if job.Payload().FinalResumeError != nil {
				finalResumeError = errors.DecodeError(ctx, *job.Payload().FinalResumeError)
			}
			err = r.stepThroughStateMachine(ctx, phs, resumer, resultsCh, job, status, finalResumeError)
			if err != nil {
				if errors.HasAssertionFailure(err) {
					log.ReportOrPanic(ctx, nil, err.Error())
				}
				log.Errorf(ctx, "job %d: adoption completed with error %v", *job.ID(), err)
			}
			status, err := job.CurrentStatus(ctx)
			if err != nil {
				log.Errorf(ctx, "job %d: failed querying status: %v", *job.ID(), err)
			} else {
				log.Infof(ctx, "job %d: status %s after adoption finished", *job.ID(), status)
			}
		}
		r.unregister(*job.ID())
		errCh <- err
	}); err != nil {
		return nil, err
	}
	return errCh, nil
}

func (r *Registry) adoptionDisabled(ctx context.Context) bool {
	if r.preventAdoptionFile != "" {
		if _, err := os.Stat(r.preventAdoptionFile); err != nil {
			if !os.IsNotExist(err) {
				log.Warning(ctx, "error checking if job adoption is currently disabled", err)
			}
			return false
		}
		log.Warningf(ctx, "job adoption is currently disabled by existence of %s", r.preventAdoptionFile)
		return true
	}
	return false
}

func (r *Registry) maybeAdoptJob(ctx context.Context, nl NodeLiveness) error {
	const stmt = `
SELECT id, payload, progress IS NULL, status
FROM system.jobs
WHERE status IN ($1, $2, $3, $4, $5) ORDER BY created DESC`
	rows, err := r.ex.Query(
		ctx, "adopt-job", nil /* txn */, stmt,
		StatusPending, StatusRunning, StatusCancelRequested, StatusPauseRequested, StatusReverting,
	)
	if err != nil {
		return errors.Wrap(err, "failed querying for jobs")
	}

	type nodeStatus struct {
		isLive bool
	}
	nodeStatusMap := map[roachpb.NodeID]*nodeStatus{
		// 0 is not a valid node ID, but we treat it as an always-dead node so that
		// the empty lease (Lease{}) is always considered expired.
		0: {isLive: false},
	}
	{
		// We subtract the leniency interval here to artificially
		// widen the range of times over which the job registry will
		// consider the node to be alive.  We rely on the fact that
		// only a live node updates its own expiration.  Thus, the
		// expiration time can be used as a reasonable measure of
		// when the node was last seen.
		now := r.lenientNow()
		for _, liveness := range nl.GetLivenesses() {
			nodeStatusMap[liveness.NodeID] = &nodeStatus{
				isLive: liveness.IsLive(now),
			}

			// Don't try to start any more jobs unless we're really live,
			// otherwise we'd just immediately cancel them.
			if liveness.NodeID == r.nodeID.Get() {
				if !liveness.IsLive(r.clock.Now().GoTime()) {
					return errors.Errorf(
						"trying to adopt jobs on node %d which is not live", r.nodeID.Get())
				}
			}
		}
	}

	if log.V(3) {
		log.Infof(ctx, "evaluating %d jobs for adoption", len(rows))
	}

	var adopted int
	for _, row := range rows {
		if adopted >= maxAdoptionsPerLoop {
			// Leave excess jobs for other nodes to get their fair share.
			break
		}

		id := (*int64)(row[0].(*tree.DInt))

		payload, err := UnmarshalPayload(row[1])
		if err != nil {
			return err
		}

		status := Status(tree.MustBeDString(row[3]))
		if log.V(3) {
			log.Infof(ctx, "job %d: evaluating for adoption with status `%s` and lease %v",
				*id, status, payload.Lease)
		}

		if payload.Lease == nil {
			// If the lease is missing, it simply means the job does not yet support
			// resumability.
			if log.V(2) {
				log.Infof(ctx, "job %d: skipping: nil lease", *id)
			}
			continue
		}

		// If the job has no progress it is from a 2.0 cluster. If the entire cluster
		// has been upgraded to 2.1 then we know nothing is running the job and it
		// can be safely failed.
		if nullProgress, ok := row[2].(*tree.DBool); ok && bool(*nullProgress) {
			log.Warningf(ctx, "job %d predates cluster upgrade and must be re-run", id)
			versionErr := errors.New("job predates cluster upgrade and must be re-run")
			payload.Error = versionErr.Error()
			payloadBytes, err := protoutil.Marshal(payload)
			if err != nil {
				return err
			}

			// We can't use job.update here because it fails while attempting to unmarshal
			// the progress. Setting the status to failed is idempotent so we don't care
			// if multiple nodes execute this.
			const updateStmt = `UPDATE system.jobs SET status = $1, payload = $2 WHERE id = $3`
			updateArgs := []interface{}{StatusFailed, payloadBytes, *id}
			err = r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				_, err := r.ex.Exec(ctx, "job-update", txn, updateStmt, updateArgs...)
				return err
			})
			if err != nil {
				log.Warningf(ctx, "job %d: has no progress but unable to mark failed: %s", id, err)
			}
			continue
		}

		r.mu.Lock()
		_, runningOnNode := r.mu.jobs[*id]
		r.mu.Unlock()

		if notLeaseHolder := payload.Lease.NodeID != r.nodeID.Get(); notLeaseHolder {
			// Another node holds the lease on the job, see if we should steal it.
			if runningOnNode {
				// If we are currently running a job that another node has the lease on,
				// stop running it.
				log.Warningf(ctx, "job %d: node %d owns lease; canceling", *id, payload.Lease.NodeID)
				r.unregister(*id)
				continue
			}
			nodeStatus, ok := nodeStatusMap[payload.Lease.NodeID]
			if !ok {
				// This case should never happen.
				log.ReportOrPanic(ctx, nil, "job %d: skipping: no liveness record for the job's node %d",
					log.Safe(*id), payload.Lease.NodeID)
				continue
			}
			if nodeStatus.isLive {
				if log.V(2) {
					log.Infof(ctx, "job %d: skipping: another node is live and holds the lease", *id)
				}
				continue
			}
		}
		// Below we know that this node holds the lease on the job.
		job := &Job{id: id, registry: r}
		resumeCtx, cancel := r.makeCtx()

		if pauseRequested := status == StatusPauseRequested; pauseRequested {
			if err := job.Paused(ctx, func(context.Context, *kv.Txn) error {
				r.unregister(*id)
				return nil
			}); err != nil {
				log.Errorf(ctx, "job %d: could not set to paused: %v", *id, err)
				continue
			}
			log.Infof(ctx, "job %d: paused", *id)
			continue
		}

		if cancelRequested := status == StatusCancelRequested; cancelRequested {
			if err := job.Reverted(ctx, errJobCanceled, func(context.Context, *kv.Txn) error {
				// Unregister the job in case it is running on the node.
				// Unregister is a no-op for jobs that are not running.
				r.unregister(*id)
				return nil
			}); err != nil {
				log.Errorf(ctx, "job %d: could not set to reverting: %v", *id, err)
				continue
			}
			log.Infof(ctx, "job %d: canceled: the job is now reverting", *id)
		} else if currentlyRunning := r.register(*id, cancel) != nil; currentlyRunning {
			if log.V(3) {
				log.Infof(ctx, "job %d: skipping: the job is already running/reverting on this node", *id)
			}
			continue
		}

		// Check if job status has changed in the meanwhile.
		currentStatus, err := job.CurrentStatus(ctx)
		if err != nil {
			return err
		}
		if status != currentStatus {
			continue
		}
		// Adopt job and resume/revert it.
		if err := job.adopt(ctx, payload.Lease); err != nil {
			r.unregister(*id)
			return errors.Wrap(err, "unable to acquire lease")
		}

		resultsCh := make(chan tree.Datums)
		resumer, err := r.createResumer(job, r.settings)
		if err != nil {
			r.unregister(*id)
			return err
		}
		log.Infof(ctx, "job %d: resuming execution", *id)
		errCh, err := r.resume(resumeCtx, resumer, resultsCh, job)
		if err != nil {
			r.unregister(*id)
			return err
		}
		go func() {
			// Drain and ignore results.
			for range resultsCh {
			}
		}()
		go func() {
			// Wait for the job to finish. No need to print the error because if there
			// was one it's been set in the job status already.
			<-errCh
			close(resultsCh)
		}()

		adopted++
	}

	return nil
}

func (r *Registry) newLease() *jobspb.Lease {
	nodeID := r.nodeID.Get()
	if nodeID == 0 {
		panic("jobs.Registry has empty node ID")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return &jobspb.Lease{NodeID: nodeID, Epoch: r.mu.epoch}
}

func (r *Registry) cancelAll(ctx context.Context) {
	r.mu.AssertHeld()
	for jobID, cancel := range r.mu.jobs {
		log.Warningf(ctx, "job %d: canceling due to liveness failure", jobID)
		cancel()
	}
	r.mu.jobs = make(map[int64]context.CancelFunc)
}

// register registers an about to be resumed job in memory so that it can be
// killed and that no one else tries to resume it. This essentially works as a
// barrier that only one function can cross and try to resume the job.
func (r *Registry) register(jobID int64, cancel func()) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// We need to prevent different routines trying to adopt and resume the job.
	if _, alreadyRegistered := r.mu.jobs[jobID]; alreadyRegistered {
		return errors.Errorf("job %d: already registered", jobID)
	}
	r.mu.jobs[jobID] = cancel
	return nil
}

func (r *Registry) unregister(jobID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cancel, ok := r.mu.jobs[jobID]
	// It is possible for a job to be double unregistered. unregister is always
	// called at the end of resume. But it can also be called during cancelAll
	// and in the adopt loop under certain circumstances.
	if ok {
		cancel()
		delete(r.mu.jobs, jobID)
	}
}
