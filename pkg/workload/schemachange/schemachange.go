// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package schemachange implements the schemachange workload.
package schemachange

import (
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// This workload executes batches of schema changes asynchronously. Each
// batch is executed in a separate transaction and transactions run in
// parallel. Batches are drawn from a pre-defined distribution.
// Currently all schema change ops are equally likely to be chosen. This
// includes table creation but note that the tables contain no data.
//
// Example usage:
// `bin/workload run schemachange --init --concurrency=2 --verbose=0 --max-ops-per-worker=1000`
// will execute up to 1000 schema change operations per txn in two concurrent txns.
//
// TODO(peter): This is still work in progress, we need to
// - support more than 1 database
// - reference sequences in column defaults
// - create foreign keys
// - support `ADD CONSTRAINT`
//
//For example, an attempt to do something we don't support should be swallowed (though if we can detect that maybe we should just not do it, e.g). It will be hard to use this test for anything more than liveness detection until we go through the tedious process of classifying errors.:

const (
	defaultMaxOpsPerWorker                 = 5
	defaultErrorRate                       = 10
	defaultEnumPct                         = 10
	defaultMaxSourceTables                 = 3
	defaultSequenceOwnedByPct              = 25
	defaultFkParentInvalidPct              = 5
	defaultFkChildInvalidPct               = 5
	defaultDeclarativeSchemaChangerPct     = 75
	defaultDeclarativeSchemaMaxStmtsPerTxn = 1
)

type schemaChangeCounter struct {
	// success and error keep track of the number of
	// successful and erroneous schema transactions.
	success, error prometheus.Counter
}

type schemaChange struct {
	flags                           workload.Flags
	connFlags                       *workload.ConnFlags
	maxOpsPerWorker                 int
	errorRate                       int
	enumPct                         int
	verbose                         int
	dryRun                          bool
	maxSourceTables                 int
	sequenceOwnedByPct              int
	logFilePath                     string
	logFile                         *os.File
	dumpLogsOnce                    *sync.Once
	declarativeStatementsEnabled    atomic.Bool
	workers                         []*schemaChangeWorker
	fkParentInvalidPct              int
	fkChildInvalidPct               int
	declarativeSchemaChangerPct     int
	declarativeSchemaMaxStmtsPerTxn int
	traceFilePath                   string
	schemaWorkloadResultAnnotator   *schemaWorkloadResultAnnotator
	reg                             *histogram.Registry
	scCounter                       schemaChangeCounter
}

var schemaChangeMeta = workload.Meta{
	Name:        `schemachange`,
	Description: `schemachange randomly generates concurrent schema changes`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		s := &schemaChange{}
		s.flags.FlagSet = pflag.NewFlagSet(`schemachange`, pflag.ContinueOnError)
		s.flags.IntVar(&s.maxOpsPerWorker, `max-ops-per-worker`, defaultMaxOpsPerWorker,
			`Number of operations to execute in a single transaction`)
		s.flags.IntVar(&s.errorRate, `error-rate`, defaultErrorRate,
			`Percentage of times to intentionally cause errors due to either existing or non-existing names`)
		s.flags.IntVar(&s.enumPct, `enum-pct`, defaultEnumPct,
			`Percentage of times when picking a type that an enum type is picked`)
		s.flags.IntVarP(&s.verbose, `verbose`, `v`, 0, ``)
		s.flags.BoolVarP(&s.dryRun, `dry-run`, `n`, false, ``)
		s.flags.IntVar(&s.maxSourceTables, `max-source-tables`, defaultMaxSourceTables,
			`Maximum tables or views that a newly created tables or views can depend on`)
		s.flags.IntVar(&s.sequenceOwnedByPct, `seq-owned-pct`, defaultSequenceOwnedByPct,
			`Percentage of times that a sequence is owned by column upon creation.`)
		s.flags.StringVar(&s.logFilePath, `txn-log`, "",
			`If provided, transactions will be written to this file in JSON form`)
		s.flags.StringVar(&s.traceFilePath, `trace-file`, "",
			`The file to write OTeL traces to. Defaults to schemachange-workload.{timestamp}.otlp.ndjson.gz`)
		s.flags.IntVar(&s.fkParentInvalidPct, `fk-parent-invalid-pct`, defaultFkParentInvalidPct,
			`Percentage of times to choose an invalid parent column in a fk constraint.`)
		s.flags.IntVar(&s.fkChildInvalidPct, `fk-child-invalid-pct`, defaultFkChildInvalidPct,
			`Percentage of times to choose an invalid child column in a fk constraint.`)
		s.flags.IntVar(&s.declarativeSchemaChangerPct, `declarative-schema-changer-pct`,
			defaultDeclarativeSchemaChangerPct,
			`Percentage (between 0 and 100) of schema change statements handled by declarative schema changer, if supported.`)
		s.flags.IntVar(&s.declarativeSchemaMaxStmtsPerTxn, `declarative-schema-changer-stmt-per-txn`,
			defaultDeclarativeSchemaMaxStmtsPerTxn,
			`Number of statements per-txn used by the declarative schema changer.`)

		s.connFlags = workload.NewConnFlags(&s.flags)
		return s
	},
}

func init() {
	workload.Register(schemaChangeMeta)
}

func setupSchemaChangePromCounter(reg prometheus.Registerer) schemaChangeCounter {
	f := promauto.With(reg)
	return schemaChangeCounter{
		success: f.NewCounter(
			prometheus.CounterOpts{
				Namespace: histogram.PrometheusNamespace,
				Subsystem: schemaChangeMeta.Name,
				Name:      "schema_change_success",
				Help:      "The total number of successful schema change transactions.",
			},
		),
		error: f.NewCounter(
			prometheus.CounterOpts{
				Namespace: histogram.PrometheusNamespace,
				Subsystem: schemaChangeMeta.Name,
				Name:      "schema_change_errors",
				Help:      "The total number of unexpected failures.",
			},
		),
	}
}

// Meta implements the workload.Generator interface.
func (s *schemaChange) Meta() workload.Meta { return schemaChangeMeta }

// Flags implements the workload.Flagser interface.
func (s *schemaChange) Flags() workload.Flags { return s.flags }

// ConnFlags implements the ConnFlagser interface.
func (s *schemaChange) ConnFlags() *workload.ConnFlags { return s.connFlags }

// Tables implements the workload.Generator interface.
func (s *schemaChange) Tables() []workload.Table { return nil }

// Ops implements the workload.Opser interface.
func (s *schemaChange) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (_ workload.QueryLoad, err error) {
	// Initialize tracing ahead of everything else. The Ops function is used for
	// managing the life cycle of this workload so we keep tracing localized to
	// this function.
	tracerProvider, err := s.initTracerProvider()
	// Initialize workload result annotator to compute trace metrics on the workload performance.
	s.schemaWorkloadResultAnnotator = &schemaWorkloadResultAnnotator{}
	// Initialize prometheus counters to export metrics for schema change workload.
	if s.reg == nil {
		// Check for nil to ensure idempotency - Ops might be invoked multiple times with the same
		// registry. We should set up counters only once.
		s.reg = reg
		s.scCounter = setupSchemaChangePromCounter(reg.Registerer())
	}
	if err != nil {
		return workload.QueryLoad{}, err
	}
	tracerProvider.RegisterSpanProcessor(s.schemaWorkloadResultAnnotator)
	tracer := tracerProvider.Tracer("schemachange")

	// NB: The schemaChange.Ops span ends when this function returns, NOT when
	// the workload is done.
	ctx, span := tracer.Start(ctx, "schemaChange.Ops")
	defer func() { EndSpan(span, err) }()

	cfg := workload.NewMultiConnPoolCfgFromFlags(s.connFlags)
	// We will need double the concurrency, since we need watch
	// dog connections. There is a danger of the pool emptying on
	// termination (since we will cancel schema changes).
	cfg.MaxConnsPerPool *= 2
	cfg.MaxTotalConnections *= 2
	// Disallow connection lifetime jittering and allow long
	// life times for this test. Schema changes can drag for
	// a long time on this workload and we have our own health
	// checks for progress.
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = time.Hour
	cfg.QueryTracer = &PGXTracer{tracer: tracer}
	if err := s.setClusterSettings(ctx, urls[0]); err != nil {
		return workload.QueryLoad{}, err
	}
	pool, err := workload.NewMultiConnPool(ctx, cfg, urls...)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	watchDogPool, err := workload.NewMultiConnPool(ctx, cfg, urls...)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	stdoutLog := makeAtomicLog(os.Stdout)
	// Use NewPseudoRand here because we want to print out the global seed used by
	// the workload. Using NewTestRand() here would only let us see the per-test
	// seed that is derived from the global seed.
	_, seed := randutil.NewPseudoRand()
	stdoutLog.printLn(fmt.Sprintf("using random seed: %d", seed))
	// A separate weighting is constructed of only schema changes supported by the
	// declarative schema changer. This will be used to make a per-worker deck
	// that has equal weights, only for supported schema changes.
	declarativeOpWeights := make([]int, len(opWeights))
	for idx, weight := range opWeights {
		if _, ok := opDeclarativeVersion[opType(idx)]; ok {
			declarativeOpWeights[idx] = weight
		}
	}

	ql := workload.QueryLoad{
		Close: func(_ context.Context) error {
			// Create a new context for shutting down the tracer provider. The
			// provided context may be cancelled depending on why the workload is
			// shutting down and we always want to provide a period of time to flush
			// traces.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			pool.Close()
			watchDogPool.Close()

			closeErr := s.closeJSONLogFile()
			shutdownErr := tracerProvider.Shutdown(ctx)
			s.schemaWorkloadResultAnnotator.logWorkloadStats(stdoutLog)
			return errors.CombineErrors(closeErr, shutdownErr)
		},
	}

	var artifactsLog *atomicLog
	if s.logFilePath != "" {
		err := s.initJSONLogFile(s.logFilePath)
		if err != nil {
			return workload.QueryLoad{}, err
		}
		artifactsLog = makeAtomicLog(s.logFile)
	}
	s.dumpLogsOnce = &sync.Once{}

	for i := 0; i < s.connFlags.Concurrency; i++ {

		// Different worker goroutines are not allowed to share RNGs. We use a
		// different seed for each worker so that each one generates different
		// operations.
		workerRng := randutil.NewTestRandWithSeed(seed + int64(i))

		// Each worker needs its own sequence number generator and operation deck so
		// that the names of generated objects and operations are deterministic
		// across runs.
		seqNum, err := s.initSeqNum(ctx, pool, i)
		if err != nil {
			return workload.QueryLoad{}, err
		}
		ops := newDeck(workerRng, opWeights...)
		declarativeOps := newDeck(workerRng, declarativeOpWeights...)

		opGeneratorParams := operationGeneratorParams{
			workerID:           i,
			seqNum:             seqNum,
			errorRate:          s.errorRate,
			enumPct:            s.enumPct,
			rng:                workerRng,
			ops:                ops,
			declarativeOps:     declarativeOps,
			maxSourceTables:    s.maxSourceTables,
			sequenceOwnedByPct: s.sequenceOwnedByPct,
			fkParentInvalidPct: s.fkParentInvalidPct,
			fkChildInvalidPct:  s.fkChildInvalidPct,
		}

		w := &schemaChangeWorker{
			id:              i,
			workload:        s,
			dryRun:          s.dryRun,
			maxOpsPerWorker: s.maxOpsPerWorker,
			pool:            pool,
			watchDogPool:    watchDogPool,
			hists:           reg.GetHandle(),
			opGen:           makeOperationGenerator(&opGeneratorParams),
			logger: &logger{
				verbose: s.verbose,
				currentLogEntry: &struct {
					mu struct {
						syncutil.Mutex
						entry *LogEntry
					}
				}{},
				stdoutLog:    stdoutLog,
				artifactsLog: artifactsLog,
			},
			isHoldingEntryLocks: false,
			tracer:              tracer,
			scCounter:           &s.scCounter,
		}

		s.workers = append(s.workers, w)

		ql.WorkerFns = append(ql.WorkerFns, w.run)
	}
	return ql, nil
}

// setClusterSettings configures any settings required for the workload ahead
// of starting workers.
func (s *schemaChange) setClusterSettings(ctx context.Context, url string) (err error) {
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return err
	}
	defer func() {
		closeErr := conn.Close(ctx)
		err = errors.WithSecondaryError(err, closeErr)
	}()
	for _, stmt := range []string{
		`SET CLUSTER SETTING sql.defaults.super_regions.enabled = 'on'`,
		`SET CLUSTER SETTING sql.log.all_statements.enabled = 'on'`,

		// This workload is designed to test multiple statements in a transaction.
		`SET CLUSTER SETTING sql.defaults.autocommit_before_ddl.enabled = 'false'`,
	} {
		_, err := conn.Exec(ctx, stmt)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// initSeqName returns the smallest available sequence number to be
// used to generate new unique names. Note that this assumes that no
// other workload is being run at the same time.
// TODO(spaskob): Do we need to protect from workloads running concurrently.
// It's not obvious how the workloads will behave when accessing the same
// cluster.
func (s *schemaChange) initSeqNum(
	ctx context.Context, pool *workload.MultiConnPool, workerID int,
) (int, error) {
	var q = fmt.Sprintf(`
SELECT max(regexp_extract(name, '[0-9]+$')::INT8)
  FROM (
    SELECT name
      FROM (
	           (SELECT table_name FROM [SHOW TABLES]) UNION
						 (SELECT sequence_name FROM [SHOW SEQUENCES]) UNION
						 (SELECT name FROM [SHOW ENUMS]) UNION
	           (SELECT schema_name FROM [SHOW SCHEMAS]) UNION
						 (SELECT column_name FROM information_schema.columns) UNION
						 (SELECT index_name FROM information_schema.statistics) UNION
						 (SELECT function_name FROM [SHOW FUNCTIONS])
           ) AS obj (name)
       )
 WHERE name ~ '^(table|view|seq|enum|schema|udf)_w%[1]d_[0-9]+$'
    OR name ~ '^(col|index)[0-9]+_w%[1]d_[0-9]+$';
`, workerID)
	var maxID gosql.NullInt64
	if err := pool.Get().QueryRow(ctx, q).Scan(&maxID); err != nil {
		return 0, err
	}

	var seqNum int
	if maxID.Valid {
		seqNum = int(maxID.Int64 + 1)
	}
	return seqNum, nil
}

type schemaChangeWorker struct {
	id                  int
	workload            *schemaChange
	dryRun              bool
	maxOpsPerWorker     int
	pool                *workload.MultiConnPool
	watchDogPool        *workload.MultiConnPool
	hists               *histogram.Histograms
	opGen               *operationGenerator
	isHoldingEntryLocks bool
	logger              *logger
	tracer              trace.Tracer
	scCounter           *schemaChangeCounter
}

var (
	errRunInTxnFatalSentinel = errors.New("fatal error when running txn")
	errRunInTxnRbkSentinel   = errors.New("txn needs to rollback")
)

// LogEntry is used to log information about the operations performed, expected errors,
// the worker ID, the corresponding timestamp, and any additional messages or error states.
// Note: LogEntry and its fields must be public so that the json package can encode this struct.
type LogEntry struct {
	// WorkerID identifies the worker executing the operations.
	WorkerID int `json:"workerId"`
	// ClientTimestamp tracks when the operation was executed.
	ClientTimestamp string `json:"clientTimestamp"`
	// Ops a collection of the various types of operations performed.
	Ops []interface{} `json:"ops"`
	// ExpectedExecErrors errors which occur as soon as you run the statement.
	ExpectedExecErrors string `json:"expectedExecErrors"`
	// ExpectedCommitErrors errors which occur only during commit.
	ExpectedCommitErrors string `json:"expectedCommitErrors"`
	// Optional message for errors or if a hook was called.
	Message string `json:"message"`
	// ErrorState holds information on the error's state when an error occurs.
	ErrorState *ErrorState `json:"errorState,omitempty"`
}

type histBin int

const (
	operationOk histBin = iota
	txnOk
	txnCommitError
	txnRollback
)

func (d histBin) String() string {
	return [...]string{"opOk", "txnOk", "txnCmtErr", "txnRbk"}[d]
}

func (w *schemaChangeWorker) recordInHist(elapsed time.Duration, bin histBin) {
	w.hists.Get(bin.String()).Record(elapsed)
}

func (w *schemaChangeWorker) WrapWithErrorState(err error) error {
	previousStmts := make([]string, 0, len(w.opGen.stmtsInTxt))
	for _, stmt := range w.opGen.stmtsInTxt {
		previousStmts = append(previousStmts, stmt.sql)
	}
	return &ErrorState{
		cause:                      err,
		PotentialCommitErrors:      w.opGen.potentialCommitErrors.StringSlice(),
		ExpectedCommitErrors:       w.opGen.expectedCommitErrors.StringSlice(),
		QueriesForGeneratingErrors: w.opGen.GetOpGenLog(),
		PreviousStatements:         previousStmts,
	}
}

func (w *schemaChangeWorker) runInTxn(
	ctx context.Context,
	tx pgx.Tx,
	useDeclarativeSchemaChanger bool,
	workloadMetrics map[string]attribute.Value,
) error {
	w.logger.startLog(w.id)
	w.logger.writeLog("BEGIN")
	numOps := 1 + w.opGen.randIntn(w.maxOpsPerWorker)
	if useDeclarativeSchemaChanger && numOps > w.workload.declarativeSchemaMaxStmtsPerTxn {
		numOps = w.workload.declarativeSchemaMaxStmtsPerTxn
	}

	for i := 0; i < numOps; i++ {
		// Terminating this loop early if there are expected commit errors prevents unexpected commit behavior from being
		// hidden by subsequent operations. Consider the case where there are expected commit errors.
		// It is possible that committing the transaction now will fail the workload because the error does not occur
		// upon committing. If more op functions were to be called, then it is possible that a subsequent op function
		// adds the same errors to the set. Due to the 2nd op, an expected commit error may occur, so the workload
		// will not fail. To prevent the covering up of unexpected behavior as outlined above, no further ops
		// should be generated if there are any errors in the expected commit errors set.
		if !w.opGen.expectedCommitErrors.empty() {
			incWorkloadMetric(numSchemaOpsExpectedFailed, workloadMetrics)
			break
		}

		op, err := w.opGen.randOp(ctx, tx, useDeclarativeSchemaChanger, numOps)
		if pgErr := new(pgconn.PgError); errors.As(err, &pgErr) &&
			pgcode.MakeCode(pgErr.Code) == pgcode.SerializationFailure {
			return errors.Mark(err, errRunInTxnRbkSentinel)
		} else if err != nil && errors.Is(err, errRunInTxnRbkSentinel) {
			// Error was already marked for us.
			return err
		} else if errors.Is(err, context.DeadlineExceeded) {
			// Deadline was encountered while generating the operation, so bail out.
			return errors.Mark(err, errRunInTxnRbkSentinel)
		} else if err != nil {
			return errors.Mark(
				w.WrapWithErrorState(
					errors.Wrap(err, "***UNEXPECTED ERROR; Failed to generate a random operation")),
				errRunInTxnFatalSentinel,
			)
		}

		w.logger.addExpectedErrors(op.expectedExecErrors, w.opGen.expectedCommitErrors)
		w.logger.writeLogOp(op)
		if !w.dryRun {
			start := timeutil.Now()
			err := op.executeStmt(ctx, tx, w.opGen)
			if err != nil {
				// If the error not an instance of pgconn.PgError, then it is unexpected.
				pgErr := new(pgconn.PgError)
				if !errors.As(err, &pgErr) {
					return err
				}

				// Transaction retry errors are acceptable. Allow the transaction
				// to rollback.
				if pgcode.MakeCode(pgErr.Code) == pgcode.SerializationFailure {
					w.recordInHist(timeutil.Since(start), txnRollback)
					return errors.Mark(
						err,
						errRunInTxnRbkSentinel,
					)
				}
				return err
			}
			incWorkloadMetric(numSchemaOpsSucceeded, workloadMetrics)
			w.recordInHist(timeutil.Since(start), operationOk)
		}
		incWorkloadMetric(numSchemaOps, workloadMetrics)
	}
	return nil
}

func (w *schemaChangeWorker) run(ctx context.Context) error {
	connPool := w.pool.Get()
	conn, err := connPool.Acquire(ctx)
	if err != nil {
		return errors.Wrap(err, "cannot get a connection")
	}
	defer conn.Release()
	useDeclarativeSchemaChanger := w.opGen.randIntn(100) < w.workload.declarativeSchemaChangerPct
	if useDeclarativeSchemaChanger {
		if _, err := conn.Exec(ctx, "SET use_declarative_schema_changer='unsafe_always';"); err != nil {
			return err
		}
	} else {
		if _, err := conn.Exec(ctx, "SET use_declarative_schema_changer='off';"); err != nil {
			return err
		}
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return errors.Wrap(err, "cannot get a connection and begin a txn")
	}

	// Initialize workload metrics.
	workloadMetrics := initWorkloadMetrics()
	_, workerSpan := w.tracer.Start(ctx, schemaWorkerSpanName)
	// The worker span is for a single schema change worker and captures workload
	// metrics specific for the schema operations run by the worker.
	defer func() { endSchemaWorkerSpan(workerSpan, workloadMetrics) }()

	// Enable extra schema changes, if they are available this moment.
	if !w.workload.declarativeStatementsEnabled.Load() {
		// Transaction confirmed we are on a new enough version, so set the
		// cluster setting.
		err := tx.Rollback(ctx)
		if err != nil {
			return errors.Wrap(err, "could not rollback before cluster setting")
		}
		_, err = conn.Exec(ctx, `SET CLUSTER SETTING sql.schema.force_declarative_statements="+CREATE SCHEMA, +CREATE SEQUENCE"`)
		if err != nil {
			return errors.Wrap(err, "cannot enable extra schema changes")
		}
		// Restart the txn after the update.
		tx, err = conn.Begin(ctx)
		if err != nil {
			return errors.Wrap(err, "cannot get a connection and begin a txn")
		}
		w.workload.declarativeStatementsEnabled.Store(true)
	}

	// Release log entry locks if holding all.
	defer w.releaseLocksIfHeld()

	// Run between 1 and maxOpsPerWorker schema change operations.
	watchDog := newSchemaChangeWatchDog(w.watchDogPool.Get(), w.logger)
	if err := watchDog.Start(ctx, tx); err != nil {
		return errors.Wrapf(err, "unable to start watch dog")
	}
	defer watchDog.Stop()
	start := timeutil.Now()
	w.opGen.resetTxnState()
	err = w.runInTxn(ctx, tx, useDeclarativeSchemaChanger, workloadMetrics)

	if err != nil {
		// Rollback in all cases to release the txn object and its conn pool. Wrap the original
		// error with a rollback error if necessary.
		if !conn.Conn().IsClosed() {
			if rbkErr := tx.Rollback(ctx); rbkErr != nil {
				err = errors.Mark(
					errors.Wrap(errors.WithSecondaryError(err,
						rbkErr), "***UNEXPECTED ERROR DURING ROLLBACK;"),
					errRunInTxnFatalSentinel,
				)
			}
		}

		w.logger.flushLogWithError(err)
		switch {
		case errors.Is(err, errRunInTxnFatalSentinel):
			w.preErrorHook()
			return err
		case errors.Is(err, errRunInTxnRbkSentinel):
			// Rollbacks are acceptable because all unexpected errors will be
			// of errRunInTxnFatalSentinel.
			return nil
		default:
			w.preErrorHook()
			return errors.Wrapf(err, "***UNEXPECTED ERROR")
		}
	}
	w.logger.writeLog("COMMIT")
	if err = tx.Commit(ctx); err != nil {
		// If the error not an instance of pgconn.PgError, then it is unexpected.
		pgErr := new(pgconn.PgError)
		if !errors.As(err, &pgErr) {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				err = nil
				w.logger.writeLog("WARNING: Connection failed at server")
				return err
			}
			err = errors.Mark(
				errors.Wrap(err, "***UNEXPECTED COMMIT ERROR; Received a non pg error"),
				errRunInTxnFatalSentinel,
			)
			w.logger.flushLogWithError(err)
			w.preErrorHook()
			return err
		}

		// Transaction retry errors are acceptable. Allow the transaction
		// to rollback.
		if pgcode.MakeCode(pgErr.Code) == pgcode.SerializationFailure {
			w.recordInHist(timeutil.Since(start), txnCommitError)
			w.logger.flushLog(fmt.Sprintf("TXN RETRY ERROR; %v", pgErr))
			return nil
		}

		// If the error is an instance of pgcode.TransactionCommittedWithSchemaChangeFailure, then
		// the underlying pgcode needs to be parsed from it.
		if pgErr.Code == pgcode.TransactionCommittedWithSchemaChangeFailure.String() {
			re := regexp.MustCompile(`\([A-Z0-9]{5}\)`)
			underLyingErrorCode := re.FindString(pgErr.Error())
			if underLyingErrorCode != "" {
				pgErr.Code = underLyingErrorCode[1 : len(underLyingErrorCode)-1]
			}
		}

		// Check for any expected errors.
		if !w.opGen.expectedCommitErrors.contains(pgcode.MakeCode(pgErr.Code)) &&
			!w.opGen.potentialCommitErrors.contains(pgcode.MakeCode(pgErr.Code)) {
			err = errors.Mark(
				w.WrapWithErrorState(
					errors.Wrapf(err, "***UNEXPECTED COMMIT ERROR; Received an unexpected commit error")),
				errRunInTxnFatalSentinel,
			)
			w.logger.flushLogWithError(err)
			w.preErrorHook()
			return err
		}

		// Error was anticipated, so it is acceptable.
		w.recordInHist(timeutil.Since(start), txnCommitError)
		w.logger.flushLog("COMMIT; Successfully got expected commit error")
		return nil
	}
	if !w.opGen.expectedCommitErrors.empty() {
		err := w.WrapWithErrorState(errors.Newf("***FAIL; Failed to receive a commit error when at least one commit error was expected"))
		w.logger.flushLogWithError(err)
		w.preErrorHook()
		return errors.Mark(err, errRunInTxnFatalSentinel)
	}

	// If there were no errors while committing the txn.
	w.logger.flushLog("")
	w.recordInHist(timeutil.Since(start), txnOk)
	workloadMetrics[txnCommitted] = attribute.BoolValue(true)
	w.scCounter.success.Inc()
	return nil
}

// preErrorHook is called by a worker whose run() function is going to return an error
// to terminate the workload. This function is used to log transactions that were
// in progress by other workers at the time of the error. It acquires the transaction
// log entry lock for each worker and flushes its logs. It does not release the
// locks so that other workers make no progress between the time that this function ends
// called and the workload terminates.
//
// In the case that the tolerate-errors flag is true, the worker calling this function will
// get restarted. In run(), the worker will release locks if isHoldingEntryLocks is true.
// If restarted, the log file will be closed and unset, so no new entries will be added. However,
// transaction logs will continue to be printed to stdout.
func (w *schemaChangeWorker) preErrorHook() {
	w.workload.dumpLogsOnce.Do(func() {
		for _, worker := range w.workload.workers {
			worker.logger.flushLogAndLock("Flushed by pre-error hook", false)
			worker.logger.artifactsLog = nil
		}
		_ = w.workload.closeJSONLogFile()
		w.isHoldingEntryLocks = true
		// preErrorHook is called for all unexpected errors. So we can use this hook to
		// increase the unexpected error count.
		w.scCounter.error.Inc()
	})
}

func (w *schemaChangeWorker) releaseLocksIfHeld() {
	if w.isHoldingEntryLocks && w.logger.verbose >= 1 {
		for _, worker := range w.workload.workers {
			worker.logger.currentLogEntry.mu.Unlock()
		}
	}
	w.isHoldingEntryLocks = false
}

// startLog initializes the currentLogEntry of the schemaChangeWorker. It is a noop
// if l.verbose < 1.
func (l *logger) startLog(workerID int) {
	if l.verbose < 1 {
		return
	}
	l.currentLogEntry.mu.Lock()
	defer l.currentLogEntry.mu.Unlock()
	l.currentLogEntry.mu.entry = &LogEntry{
		WorkerID:        workerID,
		ClientTimestamp: timeutil.Now().Format("15:04:05.999999"),
	}
}

// writeLog appends an op statement to the currentLogEntry of the schemaChangeWorker.
// It is a noop if l.verbose < 1.
func (l *logger) writeLog(op string) {
	if l.verbose < 1 {
		return
	}
	l.currentLogEntry.mu.Lock()
	defer l.currentLogEntry.mu.Unlock()
	if l.currentLogEntry.mu.entry != nil {
		l.currentLogEntry.mu.entry.Ops = append(l.currentLogEntry.mu.entry.Ops, op)
	}
}

// writeLog appends an op statement to the currentLogEntry of the schemaChangeWorker.
// It is a noop if l.verbose < 1.
func (l *logger) writeLogOp(op *opStmt) {
	if l.verbose < 1 {
		return
	}
	l.currentLogEntry.mu.Lock()
	defer l.currentLogEntry.mu.Unlock()
	if l.currentLogEntry.mu.entry != nil {
		l.currentLogEntry.mu.entry.Ops = append(l.currentLogEntry.mu.entry.Ops, op)
	}
}

// addExpectedErrors sets the expected errors in the currentLogEntry of the schemaChangeWorker.
// It is a noop if l.verbose < 1.
func (l *logger) addExpectedErrors(execErrors errorCodeSet, commitErrors errorCodeSet) {
	if l.verbose < 1 {
		return
	}
	l.currentLogEntry.mu.Lock()
	defer l.currentLogEntry.mu.Unlock()
	if l.currentLogEntry.mu.entry != nil {
		l.currentLogEntry.mu.entry.ExpectedExecErrors = execErrors.String()
		l.currentLogEntry.mu.entry.ExpectedCommitErrors = commitErrors.String()
	}
}

// flushLogWithError outputs the currentLogEntry of the schemaChangeWorker, with
// an error message (any available error state information is also added).
// It is a noop if l.verbose < 0.
func (l *logger) flushLogWithError(err error) {
	if l.verbose < 1 {
		return
	}

	// Fetch and apply the error state to the log entry.
	func() {
		l.currentLogEntry.mu.Lock()
		defer l.currentLogEntry.mu.Unlock()
		if l.currentLogEntry.mu.entry != nil {
			var state *ErrorState
			if errors.As(err, &state) {
				l.currentLogEntry.mu.entry.ErrorState = state
			}
		}
	}()

	l.flushLogAndLock(err.Error(), true)
	defer l.currentLogEntry.mu.Unlock()
}

// flushLog outputs the currentLogEntry of the schemaChangeWorker.
// It is a noop if l.verbose < 0.
func (l *logger) flushLog(message string) {
	if l.verbose < 1 {
		return
	}
	l.flushLogAndLock(message, true)
	defer l.currentLogEntry.mu.Unlock()
}

// flushLogAndLock prints the currentLogEntry of the schemaChangeWorker and does not release
// the lock for w.currentLogEntry upon returning. The lock will not be acquired if l.verbose < 1.
func (l *logger) flushLogAndLock(message string, stdout bool) {
	if l.verbose < 1 {
		return
	}

	l.currentLogEntry.mu.Lock()

	if l.currentLogEntry.mu.entry == nil || len(l.currentLogEntry.mu.entry.Ops) < 2 {
		return
	}

	if message != "" {
		l.currentLogEntry.mu.entry.Message = message
	}
	jsonBytes, err := json.MarshalIndent(l.currentLogEntry.mu.entry, "", " ")
	if err != nil {
		return
	}
	if stdout {
		l.stdoutLog.printLn(string(jsonBytes))
	}
	if l.artifactsLog != nil {
		var jsonBuf bytes.Buffer
		err = json.Compact(&jsonBuf, jsonBytes)
		if err != nil {
			return
		}
		l.artifactsLog.printLn(jsonBuf.String())
	}
	l.currentLogEntry.mu.entry = nil
}

// logWatchDog used by the watch dog to log entries on behalf of the current
// worker.
func (l *logger) logWatchDog(entry string) {
	logEntry := LogEntry{
		ClientTimestamp: timeutil.Now().Format("15:04:05.999999"),
		Ops:             nil,
		Message:         fmt.Sprintf("WATCH DOG: %s", entry),
	}
	jsonBytes, err := json.MarshalIndent(logEntry, "", " ")
	if err != nil {
		return
	}
	l.stdoutLog.printLn(string(jsonBytes))
	if l.artifactsLog != nil {
		var jsonBuf bytes.Buffer
		err = json.Compact(&jsonBuf, jsonBytes)
		if err != nil {
			return
		}
		l.artifactsLog.printLn(jsonBuf.String())
	}
}

type logger struct {
	verbose         int
	currentLogEntry *struct {
		mu struct {
			syncutil.Mutex
			entry *LogEntry
		}
	}
	stdoutLog    *atomicLog
	artifactsLog *atomicLog
}

// atomicLog is used to make synchronized writes to an io.Writer.
type atomicLog struct {
	mu struct {
		syncutil.Mutex
		log io.Writer
	}
}

func makeAtomicLog(w io.Writer) *atomicLog {
	return &atomicLog{
		mu: struct {
			syncutil.Mutex
			log io.Writer
		}{log: w},
	}
}

func (l *atomicLog) printLn(message string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	_, _ = l.mu.log.Write(append([]byte(message), '\n'))
}

func (s *schemaChange) initTracerProvider() (*sdktrace.TracerProvider, error) {
	path := s.traceFilePath
	if path == "" {
		path = fmt.Sprintf("schemachange-workload.%s.otlp.ndjson.gz", timeutil.Now().Format("20060102150405"))
	}

	// NB: otlptrace is usually used to connect to an HTTP or gRPC server, hence
	// the context. OTLPFileClient writes to a file, so there's no use in adding a timeout to this context.
	exporter, err := otlptrace.New(context.Background(), &OTLPFileClient{Path: path})
	if err != nil {
		return nil, err
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
	), nil
}

// initJsonLogFile opens the file denoted by filePath and sets s.logFile on success.
func (s *schemaChange) initJSONLogFile(filePath string) error {
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0660)
	if err != nil {
		return err
	}
	s.logFile = f
	return nil
}

// closeJsonLogFile closes s.logFile and is a noop if s.logFile is nil.
func (s *schemaChange) closeJSONLogFile() error {
	if s.logFile == nil {
		return nil
	}

	if err := s.logFile.Sync(); err != nil {
		return err
	}
	err := s.logFile.Close()
	s.logFile = nil
	return err
}
