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
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/server/tracedumper"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/logtags"
)

// adoptedJobs represents a the epoch and cancelation of a job id being run
// by the registry.
type adoptedJob struct {
	sid    sqlliveness.SessionID
	isIdle bool
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
	serverCtx context.Context

	ac       log.AmbientContext
	stopper  *stop.Stopper
	db       *kv.DB
	ex       sqlutil.InternalExecutor
	clock    *hlc.Clock
	nodeID   *base.SQLIDContainer
	settings *cluster.Settings
	execCtx  jobExecCtxMaker
	metrics  Metrics
	td       *tracedumper.TraceDumper
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

		// adoptedJobs holds a map from job id to its context cancel func and epoch.
		// It contains the jobs that are adopted and probably being run. One exception is
		// jobs scheduled inside a transaction, they will show in this map but will
		// only be run when the transaction commits.
		adoptedJobs map[jobspb.JobID]*adoptedJob

		// waiting is a set of jobs for which we're waiting to complete. In general,
		// we expect these jobs to have been started with a claim by this instance.
		// That may not have lasted to completion. Separately a goroutine will be
		// passively polling for these jobs to complete. If they complete locally,
		// the waitingSet will be updated appropriately.
		waiting jobWaitingSets
	}

	// withSessionEvery ensures that logging when failing to get a live session
	// is not too loud.
	withSessionEvery log.EveryN

	TestingResumerCreationKnobs map[jobspb.Type]func(Resumer) Resumer
}

// jobExecCtxMaker is a wrapper around sql.NewInternalPlanner. It returns an
// *sql.planner as an interface{} due to package dependency cycles. It should
// be cast to that type in the sql package when it is used. Returns a cleanup
// function that must be called once the caller is done with the planner.
//
// TODO(mjibson): Can we do something to avoid passing an interface{} here
// that must be cast in a Resumer? It cannot be done here because
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
	ctx context.Context,
	ac log.AmbientContext,
	stopper *stop.Stopper,
	clock *hlc.Clock,
	db *kv.DB,
	ex sqlutil.InternalExecutor,
	nodeID *base.SQLIDContainer,
	sqlInstance sqlliveness.Instance,
	settings *cluster.Settings,
	histogramWindowInterval time.Duration,
	execCtxFn jobExecCtxMaker,
	preventAdoptionFile string,
	td *tracedumper.TraceDumper,
	knobs *TestingKnobs,
) *Registry {
	r := &Registry{
		serverCtx:           ctx,
		ac:                  ac,
		stopper:             stopper,
		clock:               clock,
		db:                  db,
		ex:                  ex,
		nodeID:              nodeID,
		sqlInstance:         sqlInstance,
		settings:            settings,
		execCtx:             execCtxFn,
		preventAdoptionFile: preventAdoptionFile,
		td:                  td,
		// Use a non-zero buffer to allow queueing of notifications.
		// The writing method will use a default case to avoid blocking
		// if a notification is already queued.
		adoptionCh:       make(chan adoptionNotice, 1),
		withSessionEvery: log.Every(time.Second),
	}
	if knobs != nil {
		r.knobs = *knobs
		if knobs.TimeSource != nil {
			r.clock = knobs.TimeSource
		}
	}
	r.mu.adoptedJobs = make(map[jobspb.JobID]*adoptedJob)
	r.mu.waiting = make(map[jobspb.JobID]map[*waitingSet]struct{})
	r.metrics.init(histogramWindowInterval)
	return r
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
	jobs := make([]jobspb.JobID, 0, len(r.mu.adoptedJobs))
	for jID := range r.mu.adoptedJobs {
		jobs = append(jobs, jID)
	}
	return jobs
}

// ID returns a unique during the lifetime of the registry id that is
// used for keying sqlliveness claims held by the registry.
func (r *Registry) ID() base.SQLInstanceID {
	return r.nodeID.SQLInstanceID()
}

// makeCtx returns a new context from r's ambient context and an associated
// cancel func.
func (r *Registry) makeCtx() (context.Context, func()) {
	ctx := r.ac.AnnotateCtx(context.Background())
	// AddTags and not WithTags, so that we combine the tags with those
	// filled by AnnotateCtx.
	// TODO(knz): This may not be necessary if the AmbientContext had
	// all the tags already.
	// See: https://github.com/cockroachdb/cockroach/issues/72815
	ctx = logtags.AddTags(ctx, logtags.FromContext(r.serverCtx))
	return context.WithCancel(ctx)
}

// MakeJobID generates a new job ID.
func (r *Registry) MakeJobID() jobspb.JobID {
	return jobspb.JobID(builtins.GenerateUniqueInt(r.nodeID.SQLInstanceID()))
}

// newJob creates a new Job.
func (r *Registry) newJob(record Record) *Job {
	job := &Job{
		id:        record.JobID,
		registry:  r,
		createdBy: record.CreatedBy,
	}
	job.mu.payload = r.makePayload(&record)
	job.mu.progress = r.makeProgress(&record)
	job.mu.status = StatusRunning
	return job
}

// makePayload creates a Payload structure based on the given Record.
func (r *Registry) makePayload(record *Record) jobspb.Payload {
	return jobspb.Payload{
		Description:   record.Description,
		Statement:     record.Statements,
		UsernameProto: record.Username.EncodeProto(),
		DescriptorIDs: record.DescriptorIDs,
		Details:       jobspb.WrapPayloadDetails(record.Details),
		Noncancelable: record.NonCancelable,
	}
}

// makeProgress creates a Progress structure based on the given Record.
func (r *Registry) makeProgress(record *Record) jobspb.Progress {
	return jobspb.Progress{
		Details:       jobspb.WrapProgressDetails(record.Progress),
		RunningStatus: string(record.RunningStatus),
	}
}

// CreateJobsWithTxn creates jobs in fixed-size batches. There must be at least
// one job to create, otherwise the function returns an error. The function
// returns the IDs of the jobs created.
func (r *Registry) CreateJobsWithTxn(
	ctx context.Context, txn *kv.Txn, records []*Record,
) ([]jobspb.JobID, error) {
	created := make([]jobspb.JobID, 0, len(records))
	for toCreate := records; len(toCreate) > 0; {
		const maxBatchSize = 100
		batchSize := len(toCreate)
		if batchSize > maxBatchSize {
			batchSize = maxBatchSize
		}
		createdInBatch, err := r.createJobsInBatchWithTxn(ctx, txn, toCreate[:batchSize])
		if err != nil {
			return nil, err
		}
		created = append(created, createdInBatch...)
		toCreate = toCreate[batchSize:]
	}
	return created, nil
}

// createJobsInBatchWithTxn creates a batch of jobs from given records in a
// transaction.
func (r *Registry) createJobsInBatchWithTxn(
	ctx context.Context, txn *kv.Txn, records []*Record,
) ([]jobspb.JobID, error) {
	s, err := r.sqlInstance.Session(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "error getting live session")
	}
	start := timeutil.Now()
	if txn != nil {
		start = txn.ReadTimestamp().GoTime()
	}
	modifiedMicros := timeutil.ToUnixMicros(start)
	stmt, args, jobIDs, err := r.batchJobInsertStmt(s.ID(), records, modifiedMicros)
	if err != nil {
		return nil, err
	}
	if _, err = r.ex.Exec(
		ctx, "job-rows-batch-insert", txn, stmt, args...,
	); err != nil {
		return nil, err
	}
	return jobIDs, nil
}

// batchJobInsertStmt creates an INSERT statement and its corresponding arguments
// for batched jobs creation.
func (r *Registry) batchJobInsertStmt(
	sessionID sqlliveness.SessionID, records []*Record, modifiedMicros int64,
) (string, []interface{}, []jobspb.JobID, error) {
	instanceID := r.ID()
	const numColumns = 6
	columns := [numColumns]string{`id`, `status`, `payload`, `progress`, `claim_session_id`, `claim_instance_id`}
	marshalPanic := func(m protoutil.Message) []byte {
		data, err := protoutil.Marshal(m)
		if err != nil {
			panic(err)
		}
		return data
	}
	valueFns := map[string]func(*Record) interface{}{
		`id`:                func(rec *Record) interface{} { return rec.JobID },
		`status`:            func(rec *Record) interface{} { return StatusRunning },
		`claim_session_id`:  func(rec *Record) interface{} { return sessionID.UnsafeBytes() },
		`claim_instance_id`: func(rec *Record) interface{} { return instanceID },
		`payload`: func(rec *Record) interface{} {
			payload := r.makePayload(rec)
			return marshalPanic(&payload)
		},
		`progress`: func(rec *Record) interface{} {
			progress := r.makeProgress(rec)
			progress.ModifiedMicros = modifiedMicros
			return marshalPanic(&progress)
		},
	}
	appendValues := func(rec *Record, vals *[]interface{}) (err error) {
		defer func() {
			switch r := recover(); r.(type) {
			case nil:
			case error:
				err = errors.CombineErrors(err, errors.Wrapf(r.(error), "encoding job %d", rec.JobID))
			default:
				panic(r)
			}
		}()
		for _, c := range columns {
			*vals = append(*vals, valueFns[c](rec))
		}
		return nil
	}
	args := make([]interface{}, 0, len(records)*numColumns)
	jobIDs := make([]jobspb.JobID, 0, len(records))
	var buf strings.Builder
	buf.WriteString(`INSERT INTO system.jobs (`)
	buf.WriteString(strings.Join(columns[:numColumns], ", "))
	buf.WriteString(`) VALUES `)
	argIdx := 1
	for i, rec := range records {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("(")
		for j := range columns {
			if j > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString("$")
			buf.WriteString(strconv.Itoa(argIdx))
			argIdx++
		}
		buf.WriteString(")")
		if err := appendValues(rec, &args); err != nil {
			return "", nil, nil, err
		}
		jobIDs = append(jobIDs, rec.JobID)
	}
	return buf.String(), args, jobIDs, nil
}

// CreateJobWithTxn creates a job to be started later with StartJob. It stores
// the job in the jobs table, marks it pending and gives the current node a
// lease.
func (r *Registry) CreateJobWithTxn(
	ctx context.Context, record Record, jobID jobspb.JobID, txn *kv.Txn,
) (*Job, error) {
	// TODO(sajjad): Clean up the interface - remove jobID from the params as
	// Record now has JobID field.
	record.JobID = jobID
	j := r.newJob(record)

	s, err := r.sqlInstance.Session(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "error getting live session")
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

// CreateAdoptableJobWithTxn creates a job which will be adopted for execution
// at a later time by some node in the cluster.
func (r *Registry) CreateAdoptableJobWithTxn(
	ctx context.Context, record Record, jobID jobspb.JobID, txn *kv.Txn,
) (*Job, error) {
	// TODO(sajjad): Clean up the interface - remove jobID from the params as
	// Record now has JobID field.
	record.JobID = jobID
	j := r.newJob(record)
	if err := j.runInTxn(ctx, txn, func(ctx context.Context, txn *kv.Txn) error {
		// Note: although the following uses ReadTimestamp and
		// ReadTimestamp can diverge from the value of now() throughout a
		// transaction, this may be OK -- we merely required ModifiedMicro
		// to be equal *or greater* than previously inserted timestamps
		// computed by now(). For now ReadTimestamp can only move forward
		// and the assertion ReadTimestamp >= now() holds at all times.
		j.mu.progress.ModifiedMicros = timeutil.ToUnixMicros(txn.ReadTimestamp().GoTime())
		payloadBytes, err := protoutil.Marshal(&j.mu.payload)
		if err != nil {
			return err
		}
		progressBytes, err := protoutil.Marshal(&j.mu.progress)
		if err != nil {
			return err
		}
		// Set createdByType and createdByID to NULL if we don't know them.
		var createdByType, createdByID interface{}
		if j.createdBy != nil {
			createdByType = j.createdBy.Name
			createdByID = j.createdBy.ID
		}

		// Insert the job row, but do not set a `claim_session_id`. By not
		// setting the claim, the job can be adopted by any node and will
		// be adopted by the node which next runs the adoption loop.
		const stmt = `INSERT
  INTO system.jobs (
                    id,
                    status,
                    payload,
                    progress,
                    created_by_type,
                    created_by_id
                   )
VALUES ($1, $2, $3, $4, $5, $6);`
		_, err = j.registry.ex.Exec(ctx, "job-insert", txn, stmt,
			jobID, StatusRunning, payloadBytes, progressBytes, createdByType, createdByID)
		return err
	}); err != nil {
		return nil, errors.Wrap(err, "CreateAdoptableJobInTxn")
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
	var execDone chan struct{}
	if !alreadyInitialized {
		// Using a new context allows for independent lifetimes and cancellation.
		resumerCtx, cancel = r.makeCtx()

		if alreadyAdopted := r.addAdoptedJob(jobID, j.sessionID, cancel); alreadyAdopted {
			log.Fatalf(
				ctx,
				"job %d: was just created but found in registered adopted jobs",
				jobID,
			)
		}
		execDone = make(chan struct{})
	}

	if !alreadyInitialized {
		*sj = &StartableJob{}
		(*sj).resumerCtx = resumerCtx
		(*sj).cancel = cancel
		(*sj).execDone = execDone
	}
	(*sj).Job = j
	(*sj).resumer = resumer
	(*sj).txn = txn
	return nil
}

// LoadJob loads an existing job with the given jobID from the system.jobs
// table.
//
// WARNING: Avoid new uses of this function. The returned Job allows
// for mutation even if the instance no longer holds a valid claim on
// the job.
//
// TODO(ssd): Remove this API and replace it with a safer API.
func (r *Registry) LoadJob(ctx context.Context, jobID jobspb.JobID) (*Job, error) {
	return r.LoadJobWithTxn(ctx, jobID, nil)
}

// LoadClaimedJob loads an existing job with the given jobID from the
// system.jobs table. The job must have already been claimed by this
// Registry.
func (r *Registry) LoadClaimedJob(ctx context.Context, jobID jobspb.JobID) (*Job, error) {
	j, err := r.getClaimedJob(jobID)
	if err != nil {
		return nil, err
	}
	if err := j.load(ctx, nil); err != nil {
		return nil, err
	}
	return j, nil
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
// that a txn will be automatically created. The useReadLock parameter will
// have the update acquire an exclusive lock on the job row when reading. This
// can help eliminate restarts in the face of concurrent updates at the cost of
// locking the row from readers. Most updates of a job do not expect contention
// and may do extra work and thus should not do locking. Cases where the job
// is used to coordinate resources from multiple nodes may benefit from locking.
func (r *Registry) UpdateJobWithTxn(
	ctx context.Context, jobID jobspb.JobID, txn *kv.Txn, useReadLock bool, updateFunc UpdateFn,
) error {
	j := &Job{
		id:       jobID,
		registry: r,
	}
	return j.update(ctx, txn, useReadLock, updateFunc)
}

// TODO (sajjad): make maxAdoptionsPerLoop a cluster setting.
var maxAdoptionsPerLoop = envutil.EnvOrDefaultInt(`COCKROACH_JOB_ADOPTIONS_PER_PERIOD`, 10)

const removeClaimsQuery = `
UPDATE system.jobs
   SET claim_session_id = NULL
 WHERE claim_session_id in (
SELECT claim_session_id
 WHERE claim_session_id <> $1
   AND status IN ` + claimableStatusTupleString + `
   AND NOT crdb_internal.sql_liveness_is_alive(claim_session_id)
 FETCH FIRST $2 ROWS ONLY)
`

type withSessionFunc func(ctx context.Context, s sqlliveness.Session)

func (r *Registry) withSession(ctx context.Context, f withSessionFunc) {
	s, err := r.sqlInstance.Session(ctx)
	if err != nil {
		if log.ExpensiveLogEnabled(ctx, 2) ||
			(ctx.Err() == nil && r.withSessionEvery.ShouldLog()) {
			log.Errorf(ctx, "error getting live session: %s", err)
		}
		return
	}

	log.VEventf(ctx, 1, "registry live claim (instance_id: %s, sid: %s)", r.ID(), s.ID())
	f(ctx, s)
}

// Start polls the current node for liveness failures and cancels all registered
// jobs if it observes a failure. Otherwise it starts all the main daemons of
// registry that poll the jobs table and start/cancel/gc jobs.
func (r *Registry) Start(ctx context.Context, stopper *stop.Stopper) error {
	wrapWithSession := func(f withSessionFunc) func(ctx context.Context) {
		return func(ctx context.Context) { r.withSession(ctx, f) }
	}

	// removeClaimsFromDeadSessions queries the jobs table for non-terminal
	// jobs and nullifies their claims if the claims are owned by known dead sessions.
	removeClaimsFromDeadSessions := func(ctx context.Context, s sqlliveness.Session) {
		if err := r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			// Run the expiration transaction at low priority to ensure that it does
			// not contend with foreground reads. Note that the adoption and cancellation
			// queries also use low priority so they will interact nicely.
			if err := txn.SetUserPriority(roachpb.MinUserPriority); err != nil {
				return errors.WithAssertionFailure(err)
			}
			_, err := r.ex.ExecEx(
				ctx, "expire-sessions", nil,
				sessiondata.InternalExecutorOverride{User: security.RootUserName()},
				removeClaimsQuery,
				s.ID().UnsafeBytes(),
				cancellationsUpdateLimitSetting.Get(&r.settings.SV),
			)
			return err
		}); err != nil {
			log.Errorf(ctx, "error expiring job sessions: %s", err)
		}
	}
	// servePauseAndCancelRequests queries tho pause-requested and cancel-requested
	// jobs that this node has claimed and sets their states to paused or cancel
	// respectively, and then stops the execution of those jobs.
	servePauseAndCancelRequests := func(ctx context.Context, s sqlliveness.Session) {
		if err := r.servePauseAndCancelRequests(ctx, s); err != nil {
			log.Errorf(ctx, "failed to serve pause and cancel requests: %v", err)
		}
	}
	cancelLoopTask := wrapWithSession(func(ctx context.Context, s sqlliveness.Session) {
		removeClaimsFromDeadSessions(ctx, s)
		r.maybeCancelJobs(ctx, s)
		servePauseAndCancelRequests(ctx, s)
	})
	// claimJobs iterates the set of jobs which are not currently claimed and
	// claims jobs up to maxAdoptionsPerLoop.
	claimJobs := wrapWithSession(func(ctx context.Context, s sqlliveness.Session) {
		r.metrics.AdoptIterations.Inc(1)
		if err := r.claimJobs(ctx, s); err != nil {
			log.Errorf(ctx, "error claiming jobs: %s", err)
		}
	})
	// processClaimedJobs iterates the jobs claimed by the current node that
	// are in the running or reverting state, and then it starts those jobs if
	// they are not already running.
	processClaimedJobs := wrapWithSession(func(ctx context.Context, s sqlliveness.Session) {
		if r.adoptionDisabled(ctx) {
			log.Warningf(ctx, "canceling all adopted jobs due to liveness failure")
			r.cancelAllAdoptedJobs()
			return
		}
		if err := r.processClaimedJobs(ctx, s); err != nil {
			log.Errorf(ctx, "error processing claimed jobs: %s", err)
		}
	})

	if err := stopper.RunAsyncTask(ctx, "jobs/cancel", func(ctx context.Context) {
		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		cancelLoopTask(ctx)
		lc, cleanup := makeLoopController(r.settings, cancelIntervalSetting, r.knobs.IntervalOverrides.Cancel)
		defer cleanup()
		for {
			select {
			case <-lc.updated:
				lc.onUpdate()
			case <-r.stopper.ShouldQuiesce():
				log.Warningf(ctx, "canceling all adopted jobs due to stopper quiescing")
				r.cancelAllAdoptedJobs()
				return
			case <-lc.timer.C:
				lc.timer.Read = true
				cancelLoopTask(ctx)
				lc.onExecute()
			}
		}
	}); err != nil {
		return err
	}
	if err := stopper.RunAsyncTask(ctx, "jobs/gc", func(ctx context.Context) {
		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		lc, cleanup := makeLoopController(r.settings, gcIntervalSetting, r.knobs.IntervalOverrides.Gc)
		defer cleanup()

		// Retention duration of terminal job records.
		retentionDuration := func() time.Duration {
			if r.knobs.IntervalOverrides.RetentionTime != nil {
				return *r.knobs.IntervalOverrides.RetentionTime
			}
			return retentionTimeSetting.Get(&r.settings.SV)
		}

		for {
			select {
			case <-lc.updated:
				lc.onUpdate()
			case <-stopper.ShouldQuiesce():
				return
			case <-lc.timer.C:
				lc.timer.Read = true
				old := timeutil.Now().Add(-1 * retentionDuration())
				if err := r.cleanupOldJobs(ctx, old); err != nil {
					log.Warningf(ctx, "error cleaning up old job records: %v", err)
				}
				lc.onExecute()
			}
		}
	}); err != nil {
		return err
	}
	return stopper.RunAsyncTask(ctx, "jobs/adopt", func(ctx context.Context) {
		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()
		lc, cleanup := makeLoopController(r.settings, adoptIntervalSetting, r.knobs.IntervalOverrides.Adopt)
		defer cleanup()
		for {
			select {
			case <-lc.updated:
				lc.onUpdate()
			case <-stopper.ShouldQuiesce():
				return
			case shouldClaim := <-r.adoptionCh:
				// Try to adopt the most recently created job.
				if shouldClaim {
					claimJobs(ctx)
				}
				processClaimedJobs(ctx)
			case <-lc.timer.C:
				lc.timer.Read = true
				claimJobs(ctx)
				processClaimedJobs(ctx)
				lc.onExecute()
			}
		}
	})
}

func (r *Registry) maybeCancelJobs(ctx context.Context, s sqlliveness.Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, aj := range r.mu.adoptedJobs {
		if aj.sid != s.ID() {
			log.Warningf(ctx, "job %d: running without having a live claim; killed.", id)
			aj.cancel()
			delete(r.mu.adoptedJobs, id)
		}
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

// TODO (sajjad): Why are we returning column 'created' in this query? It's not
// being used.
const expiredJobsQuery = "SELECT id, payload, status, created FROM system.jobs " +
	"WHERE (created < $1) AND (id > $2) " +
	"ORDER BY id " + // the ordering is important as we keep track of the maximum ID we've seen
	"LIMIT $3"

// cleanupOldJobsPage deletes up to cleanupPageSize job rows with ID > minID.
// minID is supposed to be the maximum ID returned by the previous page (0 if no
// previous page).
func (r *Registry) cleanupOldJobsPage(
	ctx context.Context, olderThan time.Time, minID jobspb.JobID, pageSize int,
) (done bool, maxID jobspb.JobID, retErr error) {
	it, err := r.ex.QueryIterator(ctx, "gc-jobs", nil /* txn */, expiredJobsQuery, olderThan, minID, pageSize)
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
		log.Infof(ctx, "attempting to clean up %d expired job records", len(toDelete.Array))
		const stmt = `DELETE FROM system.jobs WHERE id = ANY($1)`
		var nDeleted int
		if nDeleted, err = r.ex.Exec(
			ctx, "gc-jobs", nil /* txn */, stmt, toDelete,
		); err != nil {
			return false, 0, errors.Wrap(err, "deleting old jobs")
		}
		log.Infof(ctx, "cleaned up %d expired job records", nDeleted)
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
func (r *Registry) PauseRequested(
	ctx context.Context, txn *kv.Txn, id jobspb.JobID, reason string,
) error {
	job, resumer, err := r.getJobFn(ctx, txn, id)
	if err != nil {
		return err
	}
	var onPauseRequested onPauseRequestFunc
	if pr, ok := resumer.(PauseRequester); ok {
		onPauseRequested = pr.OnPauseRequest
	}
	return job.PauseRequested(ctx, txn, onPauseRequested, reason)
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
	onExecutionFailed := func(cause error) error {
		log.InfofDepth(
			ctx, 1,
			"job %d: %s execution encountered retriable error: %+v",
			job.ID(), status, cause,
		)
		start := job.getRunStats().LastRun
		end := r.clock.Now().GoTime()
		return newRetriableExecutionError(
			r.nodeID.SQLInstanceID(), status, start, end, cause,
		)
	}
	switch status {
	case StatusRunning:
		if jobErr != nil {
			return errors.NewAssertionErrorWithWrappedErrf(jobErr,
				"job %d: resuming with non-nil error", job.ID())
		}
		resumeCtx := logtags.AddTag(ctx, "job", job.ID())

		if err := job.started(ctx, nil /* txn */); err != nil {
			return err
		}

		var err error
		func() {
			jm.CurrentlyRunning.Inc(1)
			defer jm.CurrentlyRunning.Dec(1)
			err = resumer.Resume(resumeCtx, execCtx)
		}()

		r.MarkIdle(job, false)

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

		if errors.Is(err, errPauseSelfSentinel) {
			if err := r.PauseRequested(ctx, nil, job.ID(), err.Error()); err != nil {
				return err
			}
			return errPauseSelfSentinel
		}
		// TODO(spaskob): enforce a limit on retries.

		if nonCancelableRetry := job.Payload().Noncancelable && !IsPermanentJobError(err); nonCancelableRetry ||
			errors.Is(err, errRetryJobSentinel) {
			jm.ResumeRetryError.Inc(1)
			if nonCancelableRetry {
				err = errors.Wrapf(err, "non-cancelable")
			}
			return onExecutionFailed(err)
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
			return errors.WithSecondaryError(
				errors.Wrapf(err, "job %d: could not mark as canceled", job.ID()),
				jobErr,
			)
		}
		telemetry.Inc(TelemetryMetrics[jobType].Canceled)
		r.removeFromWaitingSets(job.ID())
		return errors.WithSecondaryError(errors.Errorf("job %s", status), jobErr)
	case StatusSucceeded:
		if jobErr != nil {
			return errors.NewAssertionErrorWithWrappedErrf(jobErr,
				"job %d: successful but unexpected error provided", job.ID())
		}
		err := job.succeeded(ctx, nil /* txn */, nil /* fn */)
		switch {
		case err == nil:
			telemetry.Inc(TelemetryMetrics[jobType].Successful)
			r.removeFromWaitingSets(job.ID())
		default:
			// If we can't transactionally mark the job as succeeded then it will be
			// restarted during the next adopt loop and it will be retried.
			err = errors.Wrapf(err, "job %d: could not mark as succeeded", job.ID())
		}
		return err
	case StatusReverting:
		if err := job.reverted(ctx, nil /* txn */, jobErr, nil /* fn */); err != nil {
			// If we can't transactionally mark the job as reverting then it will be
			// restarted during the next adopt loop and it will be retried.
			return errors.WithSecondaryError(
				errors.Wrapf(err, "job %d: could not mark as reverting", job.ID()),
				jobErr,
			)
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
		jm.FailOrCancelRetryError.Inc(1)
		if onFailOrCancelCtx.Err() != nil {
			// The context was canceled. Tell the user, but don't attempt to
			// mark the job as failed because it can be resumed by another node.
			return errors.Errorf("job %d: node liveness error: restarting in background", job.ID())
		}
		return onExecutionFailed(err)
	case StatusFailed:
		if jobErr == nil {
			return errors.AssertionFailedf("job %d: has StatusFailed but no error was provided", job.ID())
		}
		if err := job.failed(ctx, nil /* txn */, jobErr, nil /* fn */); err != nil {
			// If we can't transactionally mark the job as failed then it will be
			// restarted during the next adopt loop and reverting will be retried.
			return errors.WithSecondaryError(
				errors.Wrapf(err, "job %d: could not mark as failed", job.ID()),
				jobErr,
			)
		}
		telemetry.Inc(TelemetryMetrics[jobType].Failed)
		r.removeFromWaitingSets(job.ID())
		return jobErr
	case StatusRevertFailed:
		// TODO(sajjad): Remove StatusRevertFailed and related code in other places in v22.1.
		// v21.2 modified all reverting jobs to retry instead of go to revert-failed. Therefore,
		// revert-failed state is not reachable after 21.2.
		if jobErr == nil {
			return errors.AssertionFailedf("job %d: has StatusRevertFailed but no error was provided",
				job.ID())
		}
		if err := job.revertFailed(ctx, nil /* txn */, jobErr, nil /* fn */); err != nil {
			// If we can't transactionally mark the job as failed then it will be
			// restarted during the next adopt loop and reverting will be retried.
			return errors.WithSecondaryError(
				errors.Wrapf(err, "job %d: could not mark as revert field", job.ID()),
				jobErr,
			)
		}
		return jobErr
	default:
		return errors.NewAssertionErrorWithWrappedErrf(jobErr,
			"job %d: has unsupported status %s", job.ID(), status)
	}
}

func (r *Registry) adoptionDisabled(ctx context.Context) bool {
	if r.knobs.DisableAdoptions {
		return true
	}
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

// MarkIdle marks a currently adopted job as Idle.
// A single job should not toggle its idleness more than twice per-minute as it
// is logged and may write to persisted job state in the future.
func (r *Registry) MarkIdle(job *Job, isIdle bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if aj, ok := r.mu.adoptedJobs[job.ID()]; ok {
		payload := job.Payload()
		jobType := payload.Type()
		jm := r.metrics.JobMetrics[jobType]
		if aj.isIdle != isIdle {
			log.Infof(r.serverCtx, "%s job %d: toggling idleness to %+v", jobType, job.ID(), isIdle)
			if isIdle {
				jm.CurrentlyIdle.Inc(1)
			} else {
				jm.CurrentlyIdle.Dec(1)
			}
			aj.isIdle = isIdle
		}
	}
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
	if aj, ok := r.mu.adoptedJobs[jobID]; ok {
		aj.cancel()
		delete(r.mu.adoptedJobs, jobID)
	}
}

func (r *Registry) cancelRegisteredJobContext(jobID jobspb.JobID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if aj, ok := r.mu.adoptedJobs[jobID]; ok {
		aj.cancel()
	}
}

func (r *Registry) getClaimedJob(jobID jobspb.JobID) (*Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	aj, ok := r.mu.adoptedJobs[jobID]
	if !ok {
		return nil, &JobNotFoundError{jobID: jobID}
	}
	return &Job{
		id:        jobID,
		sessionID: aj.sid,
		registry:  r,
	}, nil
}

// RetryInitialDelay returns the value of retryInitialDelaySetting cluster setting,
// in seconds, which is the initial delay in exponential-backoff delay calculation.
func (r *Registry) RetryInitialDelay() float64 {
	if r.knobs.IntervalOverrides.RetryInitialDelay != nil {
		return r.knobs.IntervalOverrides.RetryInitialDelay.Seconds()
	}
	return retryInitialDelaySetting.Get(&r.settings.SV).Seconds()
}

// RetryMaxDelay returns the value of retryMaxDelaySetting cluster setting,
// in seconds, which is the maximum delay between retries of a job.
func (r *Registry) RetryMaxDelay() float64 {
	if r.knobs.IntervalOverrides.RetryMaxDelay != nil {
		return r.knobs.IntervalOverrides.RetryMaxDelay.Seconds()
	}
	return retryMaxDelaySetting.Get(&r.settings.SV).Seconds()
}

// maybeRecordExecutionFailure will record a
// RetriableExecutionFailureError into the job payload.
func (r *Registry) maybeRecordExecutionFailure(ctx context.Context, err error, j *Job) {
	var efe *retriableExecutionError
	if !errors.As(err, &efe) {
		return
	}

	updateErr := j.Update(ctx, nil, func(
		txn *kv.Txn, md JobMetadata, ju *JobUpdater,
	) error {
		pl := md.Payload
		{ // Append the entry to the log
			maxSize := int(executionErrorsMaxEntrySize.Get(&r.settings.SV))
			pl.RetriableExecutionFailureLog = append(pl.RetriableExecutionFailureLog,
				efe.toRetriableExecutionFailure(ctx, maxSize))
		}
		{ // Maybe truncate the log.
			maxEntries := int(executionErrorsMaxEntriesSetting.Get(&r.settings.SV))
			log := &pl.RetriableExecutionFailureLog
			if len(*log) > maxEntries {
				*log = (*log)[len(*log)-maxEntries:]
			}
		}
		ju.UpdatePayload(pl)
		return nil
	})
	if ctx.Err() != nil {
		return
	}
	if updateErr != nil {
		log.Warningf(ctx, "failed to record error for job %d: %v: %v", j.ID(), err, err)
	}
}

// CheckPausepoint returns a PauseRequestError if the named pause-point is
// set.
//
// This can be called in the middle of some job implementation to effectively
// define a 'breakpoint' which, when reached, will cause the job to pause. This
// can be very useful in allowing inspection of the persisted job state at that
// point, without worrying about catching it before the job progresses further
// or completes. These pause points can be set or removed at runtime.
func (r *Registry) CheckPausepoint(name string) error {
	s := debugPausepoints.Get(&r.settings.SV)
	if s == "" {
		return nil
	}
	for _, point := range strings.Split(s, ",") {
		if name == point {
			return MarkPauseRequestError(errors.Newf("pause point %q hit", name))
		}
	}
	return nil
}

// TestingIsJobIdle returns true if the job is adopted and currently idle.
func (r *Registry) TestingIsJobIdle(jobID jobspb.JobID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	adoptedJob := r.mu.adoptedJobs[jobID]
	return adoptedJob != nil && adoptedJob.isIdle
}
