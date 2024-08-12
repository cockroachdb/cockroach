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
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/pprofutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/logtags"
)

// adoptedJobs represents the epoch and cancellation of a job id being run by
// the registry.
type adoptedJob struct {
	session sqlliveness.Session
	isIdle  bool
	// Reference to the Resumer that is currently running the job.
	resumer Resumer
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

	ac        log.AmbientContext
	stopper   *stop.Stopper
	clock     *hlc.Clock
	clusterID *base.ClusterIDContainer
	nodeID    *base.SQLIDContainer
	settings  *cluster.Settings
	execCtx   jobExecCtxMaker
	metrics   Metrics
	knobs     TestingKnobs

	// adoptionChan is used to nudge the registry to resume claimed jobs and
	// potentially attempt to claim jobs.
	adoptionCh  chan adoptionNotice
	sqlInstance sqlliveness.Instance

	// db is used by the jobs subsystem to manage job records.
	//
	// This isql.DB is instantiated with special parameters that are
	// tailored to job management. It is not suitable for execution of
	// SQL queries by job resumers.
	//
	// Instead resumer functions should reach for the isql.DB that comes
	// from the SQl executor config.
	db isql.DB

	// if non-empty, indicates path to file that prevents any job adoptions.
	preventAdoptionFile     string
	preventAdoptionLogEvery log.EveryN

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

		// draining indicates whether this node is draining or
		// not. It is set by the drain server when the drain
		// process starts.
		draining bool

		// numDrainWait is the number of jobs that are still
		// processing drain request.
		numDrainWait int

		// ingestingJobs is a map of jobs which are actively ingesting on this node
		// including via a processor.
		ingestingJobs map[jobspb.JobID]struct{}
	}

	// drainRequested signaled to indicate that this registry will shut
	// down soon.  It's an opportunity for currently running jobs
	// to detect this (OnDrain) and to have a bit of time to do cleanup/shutdown
	// in an orderly fashion, prior to resumer context being canceled.
	// The registry will no longer adopt new jobs once this channel closed.
	drainRequested chan struct{}
	// jobDrained signaled to indicate that the job watching drainRequested channel
	// completed its drain logic.
	jobDrained chan struct{}

	// drainJobs closed when registry should drain/cancel all active
	// jobs and should no longer adopt new jobs.
	drainJobs chan struct{}

	startedControllerTasksWG sync.WaitGroup

	// withSessionEvery ensures that logging when failing to get a live session
	// is not too loud.
	withSessionEvery log.EveryN

	// test only overrides for resumer creation.
	creationKnobs syncutil.Map[jobspb.Type, func(Resumer) Resumer]
}

// UpdateJobWithTxn calls the Update method on an existing job with
// jobID, using a transaction passed in the txn argument. Passing a
// nil transaction means that a txn will be automatically created.
func (r *Registry) UpdateJobWithTxn(
	ctx context.Context, jobID jobspb.JobID, txn isql.Txn, updateFunc UpdateFn,
) error {
	job := Job{registry: r, id: jobID}
	return job.WithTxn(txn).Update(ctx, updateFunc)
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
type jobExecCtxMaker func(ctx context.Context, opName string, user username.SQLUsername) (interface{}, func())

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
	clusterID *base.ClusterIDContainer,
	nodeID *base.SQLIDContainer,
	sqlInstance sqlliveness.Instance,
	settings *cluster.Settings,
	histogramWindowInterval time.Duration,
	execCtxFn jobExecCtxMaker,
	preventAdoptionFile string,
	knobs *TestingKnobs,
) *Registry {
	r := &Registry{
		serverCtx:               ctx,
		ac:                      ac,
		stopper:                 stopper,
		clock:                   clock,
		clusterID:               clusterID,
		nodeID:                  nodeID,
		sqlInstance:             sqlInstance,
		settings:                settings,
		execCtx:                 execCtxFn,
		preventAdoptionFile:     preventAdoptionFile,
		preventAdoptionLogEvery: log.Every(time.Minute),
		// Use a non-zero buffer to allow queueing of notifications.
		// The writing method will use a default case to avoid blocking
		// if a notification is already queued.
		adoptionCh:       make(chan adoptionNotice, 1),
		withSessionEvery: log.Every(time.Second),
		drainJobs:        make(chan struct{}),
		drainRequested:   make(chan struct{}),
		jobDrained:       make(chan struct{}, 1),
	}
	if knobs != nil {
		r.knobs = *knobs
		if knobs.TimeSource != nil {
			r.clock = knobs.TimeSource
		}
	}
	r.mu.adoptedJobs = make(map[jobspb.JobID]*adoptedJob)
	r.mu.waiting = make(map[jobspb.JobID]map[*waitingSet]struct{})
	r.metrics.init(histogramWindowInterval, settings)
	return r
}

// SetInternalDB sets the DB that will be used by the job registry
// executor. We expose this separately from the constructor to avoid a circular
// dependency.
func (r *Registry) SetInternalDB(db isql.DB) {
	r.db = db
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

// A static Job ID must be sufficiently small to avoid collisions with
// IDs generated by MakeJobID.
const (
	// KeyVisualizerJobID A static job ID is used to easily check if the
	// Key Visualizer job already exists.
	KeyVisualizerJobID = jobspb.JobID(100)

	// JobMetricsPollerJobID A static job ID is used for the job metrics polling job.
	JobMetricsPollerJobID = jobspb.JobID(101)

	// SqlActivityUpdaterJobID A static job ID is used for the SQL activity tables.
	SqlActivityUpdaterJobID = jobspb.JobID(103)

	// MVCCStatisticsJobID A static job ID used for the MVCC statistics update
	// job.
	MVCCStatisticsJobID = jobspb.JobID(104)
)

// MakeJobID generates a new job ID.
func (r *Registry) MakeJobID() jobspb.JobID {
	return jobspb.JobID(builtins.GenerateUniqueInt(
		builtins.ProcessUniqueID(r.nodeID.SQLInstanceID()),
	))
}

// newJob creates a new Job.
func (r *Registry) newJob(ctx context.Context, record Record) (*Job, error) {
	job := &Job{
		id:        record.JobID,
		registry:  r,
		createdBy: record.CreatedBy,
	}
	payload, err := r.makePayload(ctx, &record)
	if err != nil {
		return nil, err
	}
	job.mu.payload = payload
	job.mu.progress = r.makeProgress(&record)
	job.mu.status = StatusRunning
	return job, nil
}

// makePayload creates a Payload structure based on the given Record.
func (r *Registry) makePayload(ctx context.Context, record *Record) (jobspb.Payload, error) {
	if record.Username.Undefined() {
		return jobspb.Payload{}, errors.AssertionFailedf("job record missing username; could not make payload")
	}
	return jobspb.Payload{
		Description:            record.Description,
		Statement:              record.Statements,
		UsernameProto:          record.Username.EncodeProto(),
		DescriptorIDs:          record.DescriptorIDs,
		Details:                jobspb.WrapPayloadDetails(record.Details),
		Noncancelable:          record.NonCancelable,
		CreationClusterVersion: r.settings.Version.ActiveVersion(ctx).Version,
		CreationClusterID:      r.clusterID.Get(),
		MaximumPTSAge:          record.MaximumPTSAge,
	}, nil
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
	ctx context.Context, txn isql.Txn, records []*Record,
) ([]jobspb.JobID, error) {
	created := make([]jobspb.JobID, 0, len(records))
	for toCreate := records; len(toCreate) > 0; {
		const maxBatchSize = 100
		batchSize := len(toCreate)
		if batchSize > maxBatchSize {
			batchSize = maxBatchSize
		}
		createdInBatch, err := createJobsInBatchWithTxn(ctx, r, txn, toCreate[:batchSize])
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
func createJobsInBatchWithTxn(
	ctx context.Context, r *Registry, txn isql.Txn, records []*Record,
) ([]jobspb.JobID, error) {
	s, err := r.sqlInstance.Session(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "error getting live session")
	}
	start := txn.KV().ReadTimestamp().GoTime()
	modifiedMicros := timeutil.ToUnixMicros(start)

	jobs := make([]*Job, len(records))
	for i, record := range records {
		j, err := r.newJob(ctx, *record)
		if err != nil {
			return nil, err
		}
		jobs[i] = j
	}

	stmt, args, jobIDs, err := batchJobInsertStmt(ctx, r, s.ID(), jobs, modifiedMicros)
	if err != nil {
		return nil, err
	}
	_, err = txn.ExecEx(
		ctx, "job-rows-batch-insert", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		stmt, args...,
	)
	if err != nil {
		return nil, err
	}

	if err := batchJobWriteToJobInfo(ctx, txn, jobs, modifiedMicros); err != nil {
		return nil, err
	}

	return jobIDs, nil
}

func batchJobWriteToJobInfo(
	ctx context.Context, txn isql.Txn, jobs []*Job, modifiedMicros int64,
) error {
	for _, j := range jobs {
		infoStorage := j.InfoStorage(txn)
		payload := j.Payload()
		var payloadBytes, progressBytes []byte
		var err error
		if payloadBytes, err = protoutil.Marshal(&payload); err != nil {
			return err
		}
		if err := infoStorage.WriteLegacyPayload(ctx, payloadBytes); err != nil {
			return err
		}
		progress := j.Progress()
		if progressBytes, err = protoutil.Marshal(&progress); err != nil {
			return err
		}
		progress.ModifiedMicros = modifiedMicros
		if err := infoStorage.WriteLegacyProgress(ctx, progressBytes); err != nil {
			return err
		}
	}

	return nil
}

// batchJobInsertStmt creates an INSERT statement and its corresponding arguments
// for batched jobs creation.
func batchJobInsertStmt(
	ctx context.Context,
	r *Registry,
	sessionID sqlliveness.SessionID,
	jobs []*Job,
	modifiedMicros int64,
) (string, []interface{}, []jobspb.JobID, error) {
	created, err := tree.MakeDTimestamp(timeutil.FromUnixMicros(modifiedMicros), time.Microsecond)
	if err != nil {
		return "", nil, nil, errors.NewAssertionErrorWithWrappedErrf(err, "failed to make timestamp for creation of job")
	}
	instanceID := r.ID()
	columns := []string{`id`, `created`, `status`, `claim_session_id`, `claim_instance_id`, `job_type`}
	valueFns := map[string]func(*Job) (interface{}, error){
		`id`:                func(job *Job) (interface{}, error) { return job.ID(), nil },
		`created`:           func(job *Job) (interface{}, error) { return created, nil },
		`status`:            func(job *Job) (interface{}, error) { return StatusRunning, nil },
		`claim_session_id`:  func(job *Job) (interface{}, error) { return sessionID.UnsafeBytes(), nil },
		`claim_instance_id`: func(job *Job) (interface{}, error) { return instanceID, nil },
		`job_type`: func(job *Job) (interface{}, error) {
			payload := job.Payload()
			return payload.Type().String(), nil
		},
	}
	appendValues := func(job *Job, vals *[]interface{}) (err error) {
		defer func() {
			switch r := recover(); r.(type) {
			case nil:
			case error:
				err = errors.CombineErrors(err, errors.Wrapf(r.(error), "encoding job %d", job.ID()))
			default:
				panic(r)
			}
		}()
		for j := range columns {
			c := columns[j]
			val, err := valueFns[c](job)
			if err != nil {
				return err
			}
			*vals = append(*vals, val)
		}
		return nil
	}
	args := make([]interface{}, 0, len(jobs)*len(columns))
	jobIDs := make([]jobspb.JobID, 0, len(jobs))
	var buf strings.Builder
	buf.WriteString(`INSERT INTO system.jobs (`)
	buf.WriteString(strings.Join(columns, ", "))
	buf.WriteString(`) VALUES `)
	argIdx := 1
	for i, job := range jobs {
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
		if err := appendValues(job, &args); err != nil {
			return "", nil, nil, err
		}
		jobIDs = append(jobIDs, job.ID())
	}
	return buf.String(), args, jobIDs, nil
}

// CreateJobWithTxn creates a job to be started later with StartJob. It stores
// the job in the jobs table, marks it pending and gives the current node a
// lease.
func (r *Registry) CreateJobWithTxn(
	ctx context.Context, record Record, jobID jobspb.JobID, txn isql.Txn,
) (*Job, error) {
	// TODO(sajjad): Clean up the interface - remove jobID from the params as
	// Record now has JobID field.
	record.JobID = jobID
	j, err := r.newJob(ctx, record)
	if err != nil {
		return nil, err
	}
	do := func(ctx context.Context, txn isql.Txn) error {
		s, err := r.sqlInstance.Session(ctx)
		if err != nil {
			return errors.Wrap(err, "error getting live session")
		}
		j.session = s
		start := timeutil.Now()
		if txn != nil {
			start = txn.KV().ReadTimestamp().GoTime()
		}
		jobType := j.mu.payload.Type()
		j.mu.progress.ModifiedMicros = timeutil.ToUnixMicros(start)
		payloadBytes, err := protoutil.Marshal(&j.mu.payload)
		if err != nil {
			return err
		}
		progressBytes, err := protoutil.Marshal(&j.mu.progress)
		if err != nil {
			return err
		}

		created, err := tree.MakeDTimestamp(start, time.Microsecond)
		if err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err, "failed to construct job created timestamp")
		}

		cols := []string{"id", "created", "status", "claim_session_id", "claim_instance_id", "job_type"}
		vals := []interface{}{jobID, created, StatusRunning, s.ID().UnsafeBytes(), r.ID(), jobType.String()}
		totalNumCols := len(cols)
		numCols := totalNumCols
		placeholders := func() string {
			var p strings.Builder
			for i := 0; i < numCols; i++ {
				if i > 0 {
					p.WriteByte(',')
				}
				p.WriteByte('$')
				p.WriteString(strconv.Itoa(i + 1))
			}
			return p.String()
		}
		// We need to override the database in case we're in a situation where the
		// database in question is being dropped.
		override := sessiondata.NodeUserSessionDataOverride
		override.Database = catconstants.SystemDatabaseName
		insertStmt := fmt.Sprintf(`INSERT INTO system.jobs (%s) VALUES (%s)`,
			strings.Join(cols[:numCols], ","), placeholders())
		_, err = txn.ExecEx(
			ctx, "job-row-insert", txn.KV(),
			override,
			insertStmt, vals[:numCols]...,
		)
		if err != nil {
			return err
		}

		infoStorage := j.InfoStorage(txn)
		if err := infoStorage.WriteLegacyPayload(ctx, payloadBytes); err != nil {
			return err
		}
		if err := infoStorage.WriteLegacyProgress(ctx, progressBytes); err != nil {
			return err
		}

		return nil
	}

	run := r.db.Txn
	if txn != nil {
		run = func(
			ctx context.Context, f func(context.Context, isql.Txn) error,
			_ ...isql.TxnOption,
		) error {
			return f(ctx, txn)
		}
	}
	if err := run(ctx, do); err != nil {
		return nil, err
	}
	return j, nil
}

// CreateIfNotExistAdoptableJobWithTxn checks if a job already exists in
// the system.jobs table, and if it does not it will create the job. The job
// will be adopted for execution at a later time by some node in the cluster.
func (r *Registry) CreateIfNotExistAdoptableJobWithTxn(
	ctx context.Context, record Record, txn isql.Txn,
) error {
	if record.JobID == 0 {
		return fmt.Errorf("invalid record.JobID value: %d", record.JobID)
	}

	if txn == nil {
		return fmt.Errorf("txn is required for job: %d", record.JobID)
	}

	// Make sure job with id doesn't already exist in system.jobs.
	// Use a txn to avoid race conditions
	row, err := txn.QueryRowEx(
		ctx,
		"check if job exists",
		txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		"SELECT id FROM system.jobs WHERE id = $1",
		record.JobID,
	)
	if err != nil {
		return err
	}

	// If there isn't a row for the job, create the job.
	if row == nil {
		if _, err = r.CreateAdoptableJobWithTxn(ctx, record, record.JobID, txn); err != nil {
			return err
		}
	}

	return nil
}

// CreateAdoptableJobWithTxn creates a job which will be adopted for execution
// at a later time by some node in the cluster.
func (r *Registry) CreateAdoptableJobWithTxn(
	ctx context.Context, record Record, jobID jobspb.JobID, txn isql.Txn,
) (*Job, error) {
	// TODO(sajjad): Clean up the interface - remove jobID from the params as
	// Record now has JobID field.
	record.JobID = jobID
	j, err := r.newJob(ctx, record)
	if err != nil {
		return nil, err
	}
	do := func(ctx context.Context, txn isql.Txn) error {
		// Note: although the following uses ReadTimestamp and
		// ReadTimestamp can diverge from the value of now() throughout a
		// transaction, this may be OK -- we merely required ModifiedMicro
		// to be equal *or greater* than previously inserted timestamps
		// computed by now(). For now ReadTimestamp can only move forward
		// and the assertion ReadTimestamp >= now() holds at all times.
		j.mu.progress.ModifiedMicros = timeutil.ToUnixMicros(txn.KV().ReadTimestamp().GoTime())
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
		typ := j.mu.payload.Type().String()

		cols := []string{"id", "status", "created_by_type", "created_by_id", "job_type"}
		placeholders := []string{"$1", "$2", "$3", "$4", "$5"}
		values := []interface{}{jobID, StatusRunning, createdByType, createdByID, typ}
		nCols := len(cols)
		// Insert the job row, but do not set a `claim_session_id`. By not
		// setting the claim, the job can be adopted by any node and will
		// be adopted by the node which next runs the adoption loop.
		stmt := fmt.Sprintf(
			`INSERT INTO system.jobs (%s) VALUES (%s);`,
			strings.Join(cols[:nCols], ","), strings.Join(placeholders[:nCols], ","),
		)
		_, err = txn.ExecEx(ctx, "job-insert", txn.KV(), sessiondata.InternalExecutorOverride{
			User:     username.NodeUserName(),
			Database: catconstants.SystemDatabaseName,
		}, stmt, values[:nCols]...)
		if err != nil {
			return err
		}

		infoStorage := j.InfoStorage(txn)
		if err := infoStorage.WriteLegacyPayload(ctx, payloadBytes); err != nil {
			return err
		}
		if err := infoStorage.WriteLegacyProgress(ctx, progressBytes); err != nil {
			return err
		}

		return nil
	}
	run := r.db.Txn
	if txn != nil {
		run = func(
			ctx context.Context, f func(context.Context, isql.Txn) error,
			_ ...isql.TxnOption,
		) error {
			return f(ctx, txn)
		}
	}
	if err := run(ctx, do); err != nil {
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
	ctx context.Context, sj **StartableJob, jobID jobspb.JobID, txn isql.Txn, record Record,
) error {
	if txn == nil {
		return errors.AssertionFailedf("cannot create a startable job without a txn")
	}
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
	resumer, err := r.createResumer(j)
	if err != nil {
		return err
	}

	var resumerCtx context.Context
	var cancel func()
	var execDone chan struct{}
	if !alreadyInitialized {
		// Using a new context allows for independent lifetimes and cancellation.
		resumerCtx, cancel = r.makeCtx()

		if alreadyAdopted := r.addAdoptedJob(jobID, j.session, cancel, resumer); alreadyAdopted {
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
	(*sj).txn = txn.KV()
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
	if err := j.NoTxn().load(ctx); err != nil {
		return nil, err
	}
	return j, nil
}

// LoadJobWithTxn does the same as above, but using the transaction passed in
// the txn argument. Passing a nil transaction is equivalent to calling LoadJob
// in that a transaction will be automatically created.
func (r *Registry) LoadJobWithTxn(
	ctx context.Context, jobID jobspb.JobID, txn isql.Txn,
) (*Job, error) {
	j := &Job{
		id:       jobID,
		registry: r,
	}
	if err := j.WithTxn(txn).load(ctx); err != nil {
		return nil, err
	}
	return j, nil
}

// TODO (sajjad): make maxAdoptionsPerLoop a cluster setting.
var maxAdoptionsPerLoop = envutil.EnvOrDefaultInt(`COCKROACH_JOB_ADOPTIONS_PER_PERIOD`, 10)

const removeClaimsForDeadSessionsQuery = `
UPDATE system.jobs
   SET claim_session_id = NULL
 WHERE claim_session_id in (
SELECT claim_session_id
 WHERE claim_session_id <> $1
   AND status IN ` + claimableStatusTupleString + `
   AND NOT crdb_internal.sql_liveness_is_alive(claim_session_id)
 FETCH FIRST $2 ROWS ONLY)
`
const removeClaimsForSessionQuery = `
UPDATE system.jobs
   SET claim_session_id = NULL
 WHERE claim_session_id in (
SELECT claim_session_id
 WHERE claim_session_id = $1
   AND status IN ` + claimableStatusTupleString + `
)`

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
	if r.knobs.DisableRegistryLifecycleManagent {
		return nil
	}

	// Since the job polling system is outside user control, exclude it from cost
	// accounting and control. Individual jobs are not part of this exclusion.
	ctx = multitenant.WithTenantCostControlExemption(ctx)

	wrapWithSession := func(f withSessionFunc) func(ctx context.Context) {
		return func(ctx context.Context) { r.withSession(ctx, f) }
	}

	// removeClaimsFromDeadSessions queries the jobs table for non-terminal
	// jobs and nullifies their claims if the claims are owned by known dead sessions.
	removeClaimsFromDeadSessions := func(ctx context.Context, s sqlliveness.Session) {
		if err := r.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			// Run the expiration transaction at low priority to ensure that it does
			// not contend with foreground reads. Note that the adoption and cancellation
			// queries also use low priority so they will interact nicely.
			if err := txn.KV().SetUserPriority(roachpb.MinUserPriority); err != nil {
				return errors.WithAssertionFailure(err)
			}
			_, err := txn.ExecEx(
				ctx, "expire-sessions", txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				removeClaimsForDeadSessionsQuery,
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
	logDisabledAdoptionLimiter := log.Every(time.Minute)
	claimJobs := wrapWithSession(func(ctx context.Context, s sqlliveness.Session) {
		if r.adoptionDisabled(ctx) {
			if logDisabledAdoptionLimiter.ShouldLog() {
				log.Warningf(ctx, "job adoption is disabled, registry will not claim any jobs")
			}
			return
		}
		r.metrics.AdoptIterations.Inc(1)
		if err := r.claimJobs(ctx, s); err != nil {
			log.Errorf(ctx, "error claiming jobs: %s", err)
		}
	})
	// removeClaimsFromJobs queries the jobs table for non-terminal jobs and
	// nullifies their claims if the claims are owned by the current session.
	removeClaimsFromSession := func(ctx context.Context, s sqlliveness.Session) {
		if err := r.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			// Run the expiration transaction at low priority to ensure that it does
			// not contend with foreground reads. Note that the adoption and cancellation
			// queries also use low priority so they will interact nicely.
			if err := txn.KV().SetUserPriority(roachpb.MinUserPriority); err != nil {
				return errors.WithAssertionFailure(err)
			}
			_, err := txn.ExecEx(
				ctx, "remove-claims-for-session", txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				removeClaimsForSessionQuery, s.ID().UnsafeBytes(),
			)
			return err
		}); err != nil {
			log.Errorf(ctx, "error expiring job sessions: %s", err)
		}
	}
	// processClaimedJobs iterates the jobs claimed by the current node that
	// are in the running or reverting state, and then it starts those jobs if
	// they are not already running.
	logDisabledClaimLimiter := log.Every(time.Minute)
	processClaimedJobs := wrapWithSession(func(ctx context.Context, s sqlliveness.Session) {
		// If job adoption is disabled for the registry then we remove our claim on
		// all adopted job, and cancel them.
		if r.adoptionDisabled(ctx) {
			if logDisabledClaimLimiter.ShouldLog() {
				log.Warningf(ctx, "job adoptions is disabled, canceling all adopted "+
					"jobs due to liveness failure")
			}
			removeClaimsFromSession(ctx, s)
			r.cancelAllAdoptedJobs()
			return
		}
		if err := r.processClaimedJobs(ctx, s); err != nil {
			log.Errorf(ctx, "error processing claimed jobs: %s", err)
		}
	})

	r.startedControllerTasksWG.Add(1)
	if err := stopper.RunAsyncTask(ctx, "jobs/cancel", func(ctx context.Context) {
		defer r.startedControllerTasksWG.Done()

		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		cancelLoopTask(ctx)
		lc := makeLoopController(r.settings, cancelIntervalSetting, r.knobs.IntervalOverrides.Cancel)
		defer lc.cleanup()
		for {
			select {
			case <-lc.updated:
				lc.onUpdate()
			case <-r.stopper.ShouldQuiesce():
				// Note: the jobs are cancelled by virtue of being run with a
				// WithCancelOnQuesce context. See the resumeJob() function.
				return
			case <-r.drainJobs:
				log.Warningf(ctx, "canceling all adopted jobs due to graceful drain request")
				r.cancelAllAdoptedJobs()
				return
			case <-lc.timer.C:
				lc.timer.Read = true
				cancelLoopTask(ctx)
				lc.onExecute()
			}
		}
	}); err != nil {
		r.startedControllerTasksWG.Done()
		return err
	}

	r.startedControllerTasksWG.Add(1)
	if err := stopper.RunAsyncTask(ctx, "jobs/gc", func(ctx context.Context) {
		defer r.startedControllerTasksWG.Done()

		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		lc := makeLoopController(r.settings, gcIntervalSetting, r.knobs.IntervalOverrides.Gc)
		defer lc.cleanup()

		// Retention duration of terminal job records.
		retentionDuration := func() time.Duration {
			if r.knobs.IntervalOverrides.RetentionTime != nil {
				return *r.knobs.IntervalOverrides.RetentionTime
			}
			return RetentionTimeSetting.Get(&r.settings.SV)
		}

		for {
			select {
			case <-lc.updated:
				lc.onUpdate()
			case <-stopper.ShouldQuiesce():
				return
			case <-r.drainJobs:
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
		r.startedControllerTasksWG.Done()
		return err
	}

	r.startedControllerTasksWG.Add(1)
	if err := stopper.RunAsyncTask(ctx, "jobs/adopt", func(ctx context.Context) {
		defer r.startedControllerTasksWG.Done()

		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()
		lc := makeLoopController(r.settings, adoptIntervalSetting, r.knobs.IntervalOverrides.Adopt)
		defer lc.cleanup()
		for {
			select {
			case <-lc.updated:
				lc.onUpdate()
			case <-stopper.ShouldQuiesce():
				return
			case <-r.drainRequested:
				return
			case <-r.drainJobs:
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
	}); err != nil {
		r.startedControllerTasksWG.Done()
		return err
	}
	return nil
}

func (r *Registry) maybeCancelJobs(ctx context.Context, s sqlliveness.Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, aj := range r.mu.adoptedJobs {
		if aj.session.ID() != s.ID() {
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

// AbandonedJobInfoRowsCleanupQuery is used by the CLI command
// job-cleanup-job-info to delete jobs that were abandoned because of
// previous CRDB bugs. It is exposed here for testing.
const AbandonedJobInfoRowsCleanupQuery = `
	DELETE
FROM system.job_info
WHERE written < $1 AND job_id NOT IN (SELECT id FROM system.jobs)
LIMIT $2`

// The ordering is important as we keep track of the maximum ID we've seen.
const expiredJobsQueryWithJobInfoTable = `
WITH
latestpayload AS (
    SELECT job_id, value
    FROM system.job_info AS payload
    WHERE job_id > $2 AND info_key = 'legacy_payload'
    ORDER BY written desc
),
jobpage AS (
    SELECT id, status
    FROM system.jobs
    WHERE (created < $1) and (id > $2)
    ORDER BY id
    LIMIT $3
)
SELECT distinct (id), latestpayload.value AS payload, status
FROM jobpage AS j
INNER JOIN latestpayload ON j.id = latestpayload.job_id`

// cleanupOldJobsPage deletes up to cleanupPageSize job rows with ID > minID.
// minID is supposed to be the maximum ID returned by the previous page (0 if no
// previous page).
func (r *Registry) cleanupOldJobsPage(
	ctx context.Context, olderThan time.Time, minID jobspb.JobID, pageSize int,
) (done bool, maxID jobspb.JobID, retErr error) {
	query := expiredJobsQueryWithJobInfoTable

	it, err := r.db.Executor().QueryIterator(ctx, "gc-jobs", nil, /* txn */
		query, olderThan, minID, pageSize)
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
		log.VEventf(ctx, 2, "attempting to clean up %d expired job records", len(toDelete.Array))
		const stmt = `DELETE FROM system.jobs WHERE id = ANY($1)`
		const infoStmt = `DELETE FROM system.job_info WHERE job_id = ANY($1)`
		var nDeleted, nDeletedInfos int
		if nDeleted, err = r.db.Executor().Exec(
			ctx, "gc-jobs", nil /* txn */, stmt, toDelete,
		); err != nil {
			log.Warningf(ctx, "error cleaning up %d jobs: %v", len(toDelete.Array), err)
			return false, 0, errors.Wrap(err, "deleting old jobs")
		}
		nDeletedInfos, err = r.db.Executor().Exec(
			ctx, "gc-job-infos", nil /* txn */, infoStmt, toDelete,
		)
		if err != nil {
			return false, 0, errors.Wrap(err, "deleting old job infos")
		}
		if nDeleted > 0 {
			log.Infof(ctx, "cleaned up %d expired job records and %d expired info records", nDeleted, nDeletedInfos)
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

// DeleteTerminalJobByID deletes the given job ID if it is in a
// terminal state. If it is is in a non-terminal state, an error is
// returned.
func (r *Registry) DeleteTerminalJobByID(ctx context.Context, id jobspb.JobID) error {
	return r.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		row, err := txn.QueryRow(ctx, "get-job-status", txn.KV(),
			"SELECT status FROM system.jobs WHERE id = $1", id)
		if err != nil {
			return err
		}
		if row == nil {
			return nil
		}
		status := Status(*row[0].(*tree.DString))
		switch status {
		case StatusSucceeded, StatusCanceled, StatusFailed:
			_, err := txn.Exec(
				ctx, "delete-job", txn.KV(), "DELETE FROM system.jobs WHERE id = $1", id,
			)
			if err != nil {
				return err
			}
			_, err = txn.Exec(
				ctx, "delete-job-info", txn.KV(), "DELETE FROM system.job_info WHERE job_id = $1", id,
			)
			return err
		default:
			return errors.Newf("job %d has non-terminal status: %q", id, status)
		}
	})
}

// PauseRequested marks the job with id as paused-requested using the specified txn (may be nil).
func (r *Registry) PauseRequested(
	ctx context.Context, txn isql.Txn, id jobspb.JobID, reason string,
) error {
	return r.UpdateJobWithTxn(ctx, id, txn, func(txn isql.Txn, md JobMetadata, ju *JobUpdater) error {
		return ju.PauseRequestedWithFunc(ctx, txn, md, nil /* fn */, reason)
	})
}

// Unpause changes the paused job with id to running or reverting using the
// specified txn (may be nil).
func (r *Registry) Unpause(ctx context.Context, txn isql.Txn, id jobspb.JobID) error {
	return r.UpdateJobWithTxn(ctx, id, txn, func(txn isql.Txn, md JobMetadata, ju *JobUpdater) error {
		return ju.Unpaused(ctx, md)
	})
}

// UnsafeFailed marks the job with id as failed. Use outside of the
// job system is discouraged.
//
// This function does not stop a currently running Resumer.
func (r *Registry) UnsafeFailed(
	ctx context.Context, txn isql.Txn, id jobspb.JobID, causingError error,
) error {
	job, err := r.LoadJobWithTxn(ctx, id, txn)
	if err != nil {
		return err
	}
	return job.WithTxn(txn).failed(ctx, causingError)
}

// Succeeded marks the job with id as succeeded.
//
// Exported for testing purposes only.
func (r *Registry) Succeeded(ctx context.Context, txn isql.Txn, id jobspb.JobID) error {
	job, err := r.LoadJobWithTxn(ctx, id, txn)
	if err != nil {
		return err
	}
	return job.WithTxn(txn).succeeded(ctx, nil)
}

// Resumer is a resumable job, and is associated with a Job object. Jobs can be
// paused or canceled at any time. Jobs should call their CheckStatus() or
// Progressed() method, which will return an error if the job has been paused or
// canceled.
//
// Resumers are created through registered Constructor functions.
type Resumer interface {
	// Resume is called when a job is started or resumed. execCtx is a sql.JobExecCtx.
	//
	// The jobs infrastructure will react to the return value. If no error is
	// returned, the job is marked as successful. If an error is returned, the
	// handling depends on the type of the error and whether this job is
	// cancelable or not:
	// - if ctx has been canceled, the job record is not updated in any way. It
	//   will be retried later.
	// - a "pause request error" (see MarkPauseRequestError), the job moves to the
	//   paused status.
	// - retriable errors (see MarkAsRetryJobError) cause the job execution to be
	//   retried; Resume() will eventually be called again (perhaps on a different
	//   node).
	// - if the job is cancelable, all other errors cause the job to go through
	//   the reversion process.
	// - if the job is non-cancelable, "permanent errors" (see
	//   MarkAsPermanentJobError) cause the job to go through the reversion
	//   process. Other errors are treated as retriable.
	Resume(ctx context.Context, execCtx interface{}) error

	// OnFailOrCancel is called when a job fails or is cancel-requested.
	//
	// This method will be called when a registry notices the cancel request,
	// which is not guaranteed to run on the node where the job is running. So it
	// cannot assume that any other methods have been called on this Resumer
	// object.
	OnFailOrCancel(ctx context.Context, execCtx interface{}, jobErr error) error

	// CollectProfile is called when a job has been requested to collect a job
	// profile. This profile may contain any information that provides enhanced
	// observability into a job's execution.
	CollectProfile(ctx context.Context, execCtx interface{}) error
}

// RegisterOption is the template for options passed to the RegisterConstructor
// function.
type RegisterOption func(opts *registerOptions)

// DisablesTenantCostControl allows job implementors to exclude their job's
// Storage I/O costs (i.e. from reads/writes) from tenant accounting, based on
// this principle:
//
//	Jobs that are not triggered by user actions should be exempted from cost
//	control.
//
// For example, SQL stats compaction, span reconciler, and long-running
// migration jobs are not triggered by user actions, and so should be exempted.
// However, backup jobs are triggered by user BACKUP requests and should be
// costed. Even auto stats jobs should be costed, since the user could choose to
// disable auto stats.
//
// NOTE: A cost control exemption does not exclude CPU or Egress costs from
// accounting, since those cannot be attributed to individual jobs.
var DisablesTenantCostControl = func(opts *registerOptions) {
	opts.disableTenantCostControl = true
	opts.hasTenantCostControlOption = true
}

// UsesTenantCostControl indicates that resumed jobs should include their
// Storage I/O costs in tenant accounting. See DisablesTenantCostControl comment
// for more details.
var UsesTenantCostControl = func(opts *registerOptions) {
	opts.disableTenantCostControl = false
	opts.hasTenantCostControlOption = true
}

// WithJobMetrics returns a RegisterOption which Will configure jobs of this
// type to use specified metrics
func WithJobMetrics(m metric.Struct) RegisterOption {
	return func(opts *registerOptions) {
		opts.metrics = m
	}
}

// registerOptions are passed to RegisterConstructor and control how a job
// resumer is created and configured.
type registerOptions struct {
	// disableTenantCostControl is true when a job's Storage I/O costs should
	// be excluded from tenant accounting. See DisablesTenantCostControl comment.
	disableTenantCostControl bool

	// hasTenantCostControlOption is true if either DisablesTenantCostControl or
	// UsesTenantCostControl was specified as an option. RegisterConstructor will
	// panic if this is false.
	hasTenantCostControlOption bool

	// metrics allow jobs to register job specific metrics.
	metrics metric.Struct
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

// The constructors and options are protected behind a mutex because
// the unit tests in this package register constructors/options
// concurrently with the initialization of test servers, where jobs
// can get created and adopted.
var globalMu = struct {
	syncutil.Mutex

	constructors map[jobspb.Type]Constructor
	options      map[jobspb.Type]registerOptions
}{
	constructors: make(map[jobspb.Type]Constructor),
	options:      make(map[jobspb.Type]registerOptions),
}

func getRegisterOptions(typ jobspb.Type) (registerOptions, bool) {
	globalMu.Lock()
	defer globalMu.Unlock()
	opts, ok := globalMu.options[typ]
	return opts, ok
}

// TestingClearConstructors clears all previously registered
// constructors. This is useful in tests when you want to ensure that
// the job system will only run a particular job.
//
// The returned function should be called at the end of the test to
// restore the constructors.
func TestingClearConstructors() func() {
	globalMu.Lock()
	defer globalMu.Unlock()

	oldConstructors := globalMu.constructors
	oldOptions := globalMu.options

	globalMu.constructors = make(map[jobspb.Type]Constructor)
	globalMu.options = make(map[jobspb.Type]registerOptions)
	return func() {
		globalMu.Lock()
		defer globalMu.Unlock()
		globalMu.constructors = oldConstructors
		globalMu.options = oldOptions
	}

}

// TestingRegisterConstructor is like RegisterConstructor but returns a cleanup function
// resets the registration for the given type.
func TestingRegisterConstructor(typ jobspb.Type, fn Constructor, opts ...RegisterOption) func() {
	globalMu.Lock()
	defer globalMu.Unlock()

	var cleanupFn func()
	if origConstructorFn, found := globalMu.constructors[typ]; found {
		origOpts := globalMu.options[typ]
		cleanupFn = func() {
			globalMu.Lock()
			defer globalMu.Unlock()
			globalMu.constructors[typ] = origConstructorFn
			globalMu.options[typ] = origOpts
		}
	} else {
		cleanupFn = func() {
			globalMu.Lock()
			defer globalMu.Unlock()
			delete(globalMu.constructors, typ)
			delete(globalMu.options, typ)
		}
	}
	registerConstructorLocked(typ, fn, opts...)
	return cleanupFn
}

// RegisterConstructor registers a Resumer constructor for a certain job type.
//
// NOTE: You must pass either jobs.UsesTenantCostControl or
// jobs.DisablesTenantCostControl as an option, or this method will panic; see
// comments for these options for more details on how to use them. We want
// engineers to explicitly pass one of these options so that they will be
// prompted to think about which is appropriate for their new job type.
func RegisterConstructor(typ jobspb.Type, fn Constructor, opts ...RegisterOption) {
	globalMu.Lock()
	defer globalMu.Unlock()
	registerConstructorLocked(typ, fn, opts...)
}

func registerConstructorLocked(typ jobspb.Type, fn Constructor, opts ...RegisterOption) {
	globalMu.constructors[typ] = fn

	// Apply all options to the struct.
	var resOpts registerOptions
	for _, opt := range opts {
		opt(&resOpts)
	}
	if !resOpts.hasTenantCostControlOption {
		panic("when registering a new job type, either jobs.DisablesTenantCostControl " +
			"or jobs.UsesTenantCostControl is required; see comments for these options to learn more")
	}
	globalMu.options[typ] = resOpts
}

func (r *Registry) createResumer(job *Job) (Resumer, error) {
	payload := job.Payload()
	fn, err := r.resumerConstructorForPayload(&payload)
	if err != nil {
		return nil, err
	}
	return fn(job, r.settings), nil
}

func (r *Registry) resumerConstructorForPayload(payload *jobspb.Payload) (Constructor, error) {
	fn := func() Constructor {
		globalMu.Lock()
		defer globalMu.Unlock()
		return globalMu.constructors[payload.Type()]
	}()
	if fn == nil {
		return nil, errors.Errorf("no resumer is available for %s", payload.Type())
	}
	if wrapper, ok := r.creationKnobs.Load(payload.Type()); ok {
		return func(job *Job, settings *cluster.Settings) Resumer {
			return (*wrapper)(fn(job, settings))
		}, nil
	}
	return fn, nil
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
	if jobErr != nil {
		isExpectedError := pgerror.HasCandidateCode(jobErr) || HasErrJobCanceled(jobErr)
		if isExpectedError {
			log.Infof(ctx, "%s job %d: stepping through state %s with error: %v", jobType, job.ID(), status, jobErr)
		} else {
			log.Errorf(ctx, "%s job %d: stepping through state %s with unexpected error: %+v", jobType, job.ID(), status, jobErr)
		}
	} else {
		if jobType == jobspb.TypeAutoCreateStats {
			log.VInfof(ctx, 1, "%s job %d: stepping through state %s", jobType, job.ID(), status)
		} else {
			log.Infof(ctx, "%s job %d: stepping through state %s", jobType, job.ID(), status)
		}
	}
	jm := r.metrics.JobMetrics[jobType]
	onExecutionFailed := func(cause error) error {
		log.ErrorfDepth(
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
		resumeCtx := logtags.AddTag(ctx, "job",
			fmt.Sprintf("%s id=%d", jobType, job.ID()))
		// Adding all tags as pprof labels (including the one we just added for job
		// type and id).
		resumeCtx, undo := pprofutil.SetProfilerLabelsFromCtxTags(resumeCtx)
		defer undo()

		if err := job.NoTxn().started(ctx); err != nil {
			return err
		}

		var err error
		func() {
			jm.CurrentlyRunning.Inc(1)
			r.metrics.RunningNonIdleJobs.Inc(1)
			defer func() {
				jm.CurrentlyRunning.Dec(1)
				r.metrics.RunningNonIdleJobs.Dec(1)
			}()
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
		if IsLeaseRelocationError(err) {
			return err
		}

		if pauseOnErrErr := r.CheckPausepoint("after_exec_error"); pauseOnErrErr != nil {
			err = errors.WithSecondaryError(pauseOnErrErr, err)
		}

		if errors.Is(err, errPauseSelfSentinel) {
			if err := r.PauseRequested(ctx, nil, job.ID(), err.Error()); err != nil {
				return err
			}
			return errors.Wrap(err, PauseRequestExplained)
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
		if err := job.NoTxn().canceled(ctx); err != nil {
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
		err := job.NoTxn().succeeded(ctx, nil /* fn */)
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
		if err := job.NoTxn().reverted(ctx, jobErr, nil /* fn */); err != nil {
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
			r.metrics.RunningNonIdleJobs.Inc(1)
			defer func() {
				jm.CurrentlyRunning.Dec(1)
				r.metrics.RunningNonIdleJobs.Dec(1)
			}()
			err = resumer.OnFailOrCancel(onFailOrCancelCtx, execCtx, jobErr)
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
		if err := job.NoTxn().failed(ctx, jobErr); err != nil {
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
		if err := job.NoTxn().revertFailed(ctx, jobErr, nil /* fn */); err != nil {
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
		if r.preventAdoptionLogEvery.ShouldLog() {
			log.Warningf(ctx, "job adoption is currently disabled by existence of %s", r.preventAdoptionFile)
		}
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
			log.VEventf(r.serverCtx, 2, "%s job %d: toggling idleness to %+v", jobType, job.ID(), isIdle)
			if isIdle {
				r.metrics.RunningNonIdleJobs.Dec(1)
				jm.CurrentlyIdle.Inc(1)
			} else {
				r.metrics.RunningNonIdleJobs.Inc(1)
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

func (r *Registry) cancelRegisteredJobContext(jobID jobspb.JobID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	aj, ok := r.mu.adoptedJobs[jobID]
	if ok {
		aj.cancel()
	}
	return ok
}

// GetResumerForClaimedJob returns the resumer of the jobID if the registry has
// a claim on the job.
func (r *Registry) GetResumerForClaimedJob(jobID jobspb.JobID) (Resumer, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	aj, ok := r.mu.adoptedJobs[jobID]
	if !ok {
		return nil, &JobNotFoundError{jobID: jobID}
	}
	return aj.resumer, nil
}

func (r *Registry) getClaimedJob(jobID jobspb.JobID) (*Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	aj, ok := r.mu.adoptedJobs[jobID]
	if !ok {
		return nil, &JobNotFoundError{jobID: jobID}
	}
	return &Job{
		id:       jobID,
		session:  aj.session,
		registry: r,
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

	updateErr := j.NoTxn().Update(ctx, func(
		txn isql.Txn, md JobMetadata, ju *JobUpdater,
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
		log.Warningf(ctx, "failed to record error for job %d: %v: %v", j.ID(), err, updateErr)
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

func (r *Registry) RelocateLease(
	ctx context.Context,
	txn isql.Txn,
	id jobspb.JobID,
	destID base.SQLInstanceID,
	destSession sqlliveness.SessionID,
) (sentinel error, failure error) {
	if _, err := r.db.Executor().Exec(ctx, "job-relocate-coordinator", txn.KV(),
		"UPDATE system.jobs SET claim_instance_id = $2, claim_session_id = $3 WHERE id = $1",
		id, destID, destSession.UnsafeBytes(),
	); err != nil {
		return nil, errors.Wrapf(err, "failed to relocate job coordinator to %d", destID)
	}

	return errors.Mark(errors.Newf("execution of job %d relocated to %d", id, destID), errJobLeaseNotHeld), nil
}

// IsLeaseRelocationError returns true if the error indicates lease relocation.
func IsLeaseRelocationError(err error) bool {
	return errors.Is(err, errJobLeaseNotHeld)
}

// IsDraining returns true if the job system has been informed that
// the local node is draining.
//
// Jobs that depend on distributed SQL infrastructure may choose to
// exit early in this case.
func (r *Registry) IsDraining() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.mu.draining
}

// WaitForRegistryShutdown waits for all background job registry tasks to complete.
func (r *Registry) WaitForRegistryShutdown(ctx context.Context) {
	log.Infof(ctx, "starting to wait for job registry to shut down")
	defer log.Infof(ctx, "job registry tasks successfully shut down")
	r.startedControllerTasksWG.Wait()
}

// DrainRequested informs the job system that this node is being drained.
// Waits for the jobs to complete their drain logic (or context cancellation)
// prior to returning.  After this function returns, no new jobs will be adopted,
// and all running jobs will be canceled.
// WaitForRegistryShutdown can then be used to wait for those tasks to complete.
func (r *Registry) DrainRequested(ctx context.Context) {
	r.mu.Lock()
	alreadyDraining := r.mu.draining
	numWait := r.mu.numDrainWait
	r.mu.draining = true
	r.mu.Unlock()

	if alreadyDraining {
		return
	}

	close(r.drainRequested)
	defer close(r.drainJobs)

	if numWait == 0 {
		return
	}

	for numWait > 0 {
		select {
		case <-ctx.Done():
			return
		case <-r.stopper.ShouldQuiesce():
			return
		case <-r.jobDrained:
			r.mu.Lock()
			numWait = r.mu.numDrainWait
			r.mu.Unlock()
		}
	}
}

// OnDrain returns a channel that can be selected on to detect when the job
// registry begins draining.
// The caller must invoke returned function when drain completes.
func (r *Registry) OnDrain() (<-chan struct{}, func()) {
	r.mu.Lock()
	r.mu.numDrainWait++
	r.mu.Unlock()

	return r.drainRequested, func() {
		r.mu.Lock()
		r.mu.numDrainWait--
		r.mu.Unlock()

		select {
		case r.jobDrained <- struct{}{}:
		default:
		}
	}
}

// TestingIsJobIdle returns true if the job is adopted and currently idle.
func (r *Registry) TestingIsJobIdle(jobID jobspb.JobID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	adoptedJob := r.mu.adoptedJobs[jobID]
	return adoptedJob != nil && adoptedJob.isIdle
}

// MarkAsIngesting records a given jobID as actively ingesting data on the node
// in which this Registry resides, either directly or via a processor running on
// behalf of a job being run by a different registry. The returned function is
// to be called when this node is no longer ingesting for that jobID, typically
// by deferring it when calling this method. See IsIngesting.
func (r *Registry) MarkAsIngesting(jobID catpb.JobID) func() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.mu.ingestingJobs == nil {
		r.mu.ingestingJobs = make(map[catpb.JobID]struct{})
	}
	r.mu.ingestingJobs[jobID] = struct{}{}
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(r.mu.ingestingJobs, jobID)
	}
}

// IsIngesting returns true if a given job has indicated it is ingesting at this
// time on this node. See MarkAsIngesting.
func (r *Registry) IsIngesting(jobID catpb.JobID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.mu.ingestingJobs[jobID]
	return ok
}
