// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemachange

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx"
	"github.com/spf13/pflag"
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
// - support `SET COLUMN DEFAULT`
//
// TODO(spaskob): introspect errors returned from the workload and determine
// whether they're expected or unexpected. Flag `tolerate-errors` should be
// added to tolerate unexpected errors and then unexpected errors should fail
// the workload.
//
//For example, an attempt to do something we don't support should be swallowed (though if we can detect that maybe we should just not do it, e.g). It will be hard to use this test for anything more than liveness detection until we go through the tedious process of classifying errors.:

const (
	defaultMaxOpsPerWorker = 5
	defaultErrorRate       = 10
	defaultEnumPct         = 10
	defaultMaxSourceTables = 3
)

type schemaChange struct {
	flags           workload.Flags
	dbOverride      string
	concurrency     int
	maxOpsPerWorker int
	errorRate       int
	enumPct         int
	verbose         int
	dryRun          bool
	maxSourceTables int
}

var schemaChangeMeta = workload.Meta{
	Name:        `schemachange`,
	Description: `schemachange randomly generates concurrent schema changes`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		s := &schemaChange{}
		s.flags.FlagSet = pflag.NewFlagSet(`schemachange`, pflag.ContinueOnError)
		s.flags.StringVar(&s.dbOverride, `db`, ``,
			`Override for the SQL database to use. If empty, defaults to the generator name`)
		s.flags.IntVar(&s.concurrency, `concurrency`, 2*runtime.NumCPU(), /* TODO(spaskob): sensible default? */
			`Number of concurrent workers`)
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
		return s
	},
}

func init() {
	workload.Register(schemaChangeMeta)
}

// Meta implements the workload.Generator interface.
func (s *schemaChange) Meta() workload.Meta {
	return schemaChangeMeta
}

// Flags implements the workload.Flagser interface.
func (s *schemaChange) Flags() workload.Flags {
	return s.flags
}

// Tables implements the workload.Generator interface.
func (s *schemaChange) Tables() []workload.Table {
	return nil
}

// Tables implements the workload.Opser interface.
func (s *schemaChange) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(s, s.dbOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	cfg := workload.MultiConnPoolCfg{
		MaxTotalConnections: s.concurrency * 2, //TODO(spaskob): pick a sensible default.
	}
	pool, err := workload.NewMultiConnPool(cfg, urls...)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	seqNum, err := s.initSeqNum(pool)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	ops := newDeck(rand.New(rand.NewSource(timeutil.Now().UnixNano())), opWeights...)
	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}

	for i := 0; i < s.concurrency; i++ {

		opGeneratorParams := operationGeneratorParams{
			seqNum:          seqNum,
			errorRate:       s.errorRate,
			enumPct:         s.enumPct,
			rng:             rand.New(rand.NewSource(timeutil.Now().UnixNano())),
			ops:             ops,
			maxSourceTables: s.maxSourceTables,
		}

		w := &schemaChangeWorker{
			verbose:         s.verbose,
			dryRun:          s.dryRun,
			maxOpsPerWorker: s.maxOpsPerWorker,
			pool:            pool,
			hists:           reg.GetHandle(),
			opGen:           makeOperationGenerator(&opGeneratorParams),
		}
		ql.WorkerFns = append(ql.WorkerFns, w.run)
	}
	return ql, nil
}

// initSeqName returns the smallest available sequence number to be
// used to generate new unique names. Note that this assumes that no
// other workload is being run at the same time.
// TODO(spaskob): Do we need to protect from workloads running concurrently.
// It's not obvious how the workloads will behave when accessing the same
// cluster.
func (s *schemaChange) initSeqNum(pool *workload.MultiConnPool) (*int64, error) {
	seqNum := new(int64)

	const q = `
SELECT max(regexp_extract(name, '[0-9]+$')::int)
  FROM ((SELECT table_name FROM [SHOW TABLES]) UNION (SELECT sequence_name FROM [SHOW SEQUENCES])) AS obj(name)
 WHERE name ~ '^(table|view|seq)[0-9]+$';
`
	var max gosql.NullInt64
	if err := pool.Get().QueryRow(q).Scan(&max); err != nil {
		return nil, err
	}
	if max.Valid {
		*seqNum = max.Int64 + 1
	}

	return seqNum, nil
}

type schemaChangeWorker struct {
	verbose         int
	dryRun          bool
	maxOpsPerWorker int
	pool            *workload.MultiConnPool
	hists           *histogram.Histograms
	opGen           *operationGenerator
}

// handleOpError returns an error if the op error is considered serious and
// we should terminate the workload.
func handleOpError(err error) error {
	if err == nil {
		return nil
	}
	if pgErr := (pgx.PgError{}); errors.As(err, &pgErr) {
		sqlstate := pgErr.SQLState()
		class := sqlstate[0:2]
		switch class {
		case "09":
			return errors.Wrap(err, "Class 09 - Triggered Action Exception")
		case "XX":
			return errors.Wrap(err, "Class XX - Internal Error")
		}
	} else {
		return errors.Wrapf(err, "unexpected error %v", err)
	}
	return nil
}

var (
	errRunInTxnFatalSentinel = errors.New("fatal error when running txn")
	errRunInTxnRbkSentinel   = errors.New("txn needs to rollback")
)

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

func (w *schemaChangeWorker) runInTxn(tx *pgx.Tx) (string, error) {
	var log strings.Builder
	opsNum := 1 + w.opGen.randIntn(w.maxOpsPerWorker)

	for i := 0; i < opsNum; i++ {
		w.opGen.resetOpState()
		op, noops, err := w.opGen.randOp(tx)
		if err != nil {
			return noops, errors.Mark(
				errors.Wrap(err, "could not generate a random operation"),
				errRunInTxnFatalSentinel,
			)
		}
		if w.verbose >= 2 {
			// Print the failed attempts to produce a random operation.
			log.WriteString(noops)
		}
		log.WriteString(fmt.Sprintf("  %s;\n", op))
		if !w.dryRun {
			start := timeutil.Now()

			if _, err = tx.Exec(op); err != nil {
				if w.opGen.screenForExecErrors {
					if w.opGen.expectedExecErrors.empty() {
						log.WriteString(fmt.Sprintf("***FAIL; Expected no errors, but got %v\n", err))
						return log.String(), errors.Mark(
							errors.Wrap(err, "***UNEXPECTED ERROR"),
							errRunInTxnFatalSentinel,
						)
					} else if pgErr := (pgx.PgError{}); !errors.As(err, &pgErr) || errors.As(err, &pgErr) && !w.opGen.expectedExecErrors.empty() && !w.opGen.expectedExecErrors.contains(pgcode.MakeCode(pgErr.Code)) {
						log.WriteString(fmt.Sprintf("***FAIL; Expected one of SQLSTATES %s, but got %v\n", w.opGen.expectedExecErrors.string(), err))
						return log.String(), errors.Mark(
							errors.Wrap(err, "***UNEXPECTED ERROR"),
							errRunInTxnFatalSentinel,
						)
					}

					log.WriteString(fmt.Sprintf("ROLLBACK; expected SQLSTATE(S) %s, and got %v\n", w.opGen.expectedExecErrors.string(), err))
					w.recordInHist(timeutil.Since(start), txnRollback)
					return log.String(), errors.Mark(
						err,
						errRunInTxnRbkSentinel,
					)
				}

				// TODO(jayshrivastava): Once all operations support error screening, delete this default and remove w.opGen.screenForExecErrors state
				w.recordInHist(timeutil.Since(start), txnRollback)
				log.WriteString(fmt.Sprintf("ROLLBACK; %v\n", err))
				return log.String(), errors.Mark(err, errRunInTxnRbkSentinel)

			}
			if w.opGen.screenForExecErrors {
				if !w.opGen.expectedExecErrors.empty() {
					log.WriteString(fmt.Sprintf("Expected SQLSTATE(S) %s, but got no errors\n", w.opGen.expectedExecErrors.string()))
					return log.String(), errors.Mark(
						errors.Errorf("***UNEXPECTED SUCCESS"),
						errRunInTxnFatalSentinel,
					)
				}
			}
			w.recordInHist(timeutil.Since(start), operationOk)
		}
	}
	return log.String(), nil
}

func (w *schemaChangeWorker) run(_ context.Context) error {
	tx, err := w.pool.Get().Begin()
	if err != nil {
		return errors.Wrap(err, "cannot get a connection and begin a txn")
	}

	// Run between 1 and maxOpsPerWorker schema change operations.
	start := timeutil.Now()
	w.opGen.resetTxnState()
	logs, err := w.runInTxn(tx)
	logs = "BEGIN\n" + logs
	defer func() {
		if w.verbose >= 1 {
			fmt.Print(logs)
		}
	}()

	if err != nil {
		// Rollback in all cases to release the txn object and its conn pool.
		if rbkErr := tx.Rollback(); rbkErr != nil {
			return errors.Wrapf(err, "***UNEXPECTED ERROR IN ROLLBACK %v", rbkErr)
		}
		switch {
		case errors.Is(err, errRunInTxnFatalSentinel):
			return err
		case errors.Is(err, errRunInTxnRbkSentinel):
			// TODO(jayshrivastava): Once all operations support error screening, return nil
			// All unexpected or non pg errors will be fatal, and all rollbacks will be expected
			if seriousErr := handleOpError(err); seriousErr != nil {
				return seriousErr
			}
			return nil
		default:
			return errors.Wrapf(err, "***UNEXPECTED ERROR")
		}
	}

	// If there were no errors commit the txn.
	histBin := txnOk
	cmtErrMsg := ""
	if err = tx.Commit(); err != nil {
		histBin = txnCommitError
		cmtErrMsg = fmt.Sprintf("***FAIL: %v", err)
	}
	w.recordInHist(timeutil.Since(start), histBin)
	logs = logs + fmt.Sprintf("COMMIT;  %s\n", cmtErrMsg)
	return nil
}
