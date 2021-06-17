// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package interleavedpartitioned

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

const (
	zoneLocationsStmt = `
UPSERT INTO system.locations VALUES
	('zone', $1, 33.0641249, -80.0433347),
	('zone', $2, 45.6319052, -121.2010282),
	('zone', $3, 41.238785 , -95.854239)
`

	nodeIDQuery = `
SELECT DISTINCT node_id
FROM crdb_internal.node_build_info
`

	// Table Schemas
	// TODO(bram): Deletes are very slow due to contention. We could create a
	// partitioned index on session, but to do so would require creating a
	// computed column (for east/west) and creating the index on that column and
	// created.
	sessionSchema = `
(
	session_id STRING(100) PRIMARY KEY,
	affiliate STRING(100) NOT NULL,
	channel STRING(50) NOT NULL,
	language STRING(20) NOT NULL,
	created TIMESTAMP NOT NULL,
	updated TIMESTAMP NOT NULL,
	status STRING(20) NOT NULL,
	platform STRING(50) NOT NULL,
	query_id STRING(100) NOT NULL
) PARTITION BY RANGE (session_id) (
	PARTITION east VALUES FROM ('E-') TO ('F-'),
	PARTITION west VALUES FROM ('W-') TO ('X-'),
	PARTITION central VALUES FROM ('C-') TO ('D-')
)`
	genericChildSchema = `
(
	session_id STRING(100) NOT NULL,
	id STRING(50) NOT NULL,
	value STRING(50) NOT NULL,
	created TIMESTAMP NOT NULL,
	updated TIMESTAMP NOT NULL,
	PRIMARY KEY (session_id, id),
	FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
) INTERLEAVE IN PARENT sessions(session_id)`
	deviceSchema = `
(
	session_id STRING(100) NOT NULL,
	id STRING(100) NOT NULL,
	device_id STRING(50),
	name STRING(50),
	make STRING(50),
	macaddress STRING(50),
	model STRING(50),
	serial_number STRING(50),
	created TIMESTAMP NOT NULL,
	updated TIMESTAMP NOT NULL,
	PRIMARY KEY (session_id, id),
	FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
) INTERLEAVE IN PARENT sessions(session_id)
`
	querySchema = `
(
	session_id STRING(100) NOT NULL,
	id STRING(50) NOT NULL,
	created TIMESTAMP NOT NULL,
	updated TIMESTAMP NOT NULL,
	PRIMARY KEY (session_id, id),
	FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
) INTERLEAVE IN PARENT sessions(session_id)
`

	// Insert Queries
	insertQuery = `INSERT INTO sessions(
	session_id,
	affiliate,
	channel,
	language,
	created,
	updated,
	status,
	platform,
	query_id
) VALUES ($1, $2, $3, $4, now(), now(), $5, $6, $7)
`
	insertQueryCustomers = `
INSERT INTO customers(session_id, id, value, created, updated)
VALUES ($1, $2, $3, now(), now())
`
	insertQueryVariants = `
INSERT INTO variants(session_id, id, value, created, updated)
VALUES ($1, $2, $3, now(), now())
`
	insertQueryParameters = `
INSERT INTO parameters(session_id, id, value, created, updated)
VALUES ($1, $2, $3, now(), now())
`
	insertQueryDevices = `
INSERT INTO devices(
	session_id,
	id,
	device_id,
	name,
	make,
	macaddress,
	model,
	serial_number,
	created,
	updated
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now(), now())
`
	insertQueryQuery = `
INSERT INTO queries(session_id, id, created, updated)
VALUES ($1, $2, now(), now())
`

	// Delete queries
	deleteWestQuery = `
DELETE FROM sessions
WHERE session_id LIKE 'W-%'
	AND created < now() - interval '1' minute
LIMIT $1
`
	deleteEastQuery = `
DELETE FROM sessions
WHERE session_id LIKE 'E-%'
	AND created < now() - interval '1' minute
LIMIT $1
`

	// Retrieve queries
	retrieveQuery1 = `
SELECT session_id, affiliate, channel, created, language, status, platform, query_id, updated
FROM sessions
WHERE session_id = $1
`
	retrieveQuery2 = `
SELECT
	device.id,
	device.session_id,
	device.created,
	device.name,
	device.model,
	device.macaddress,
	device.serial_number,
	device.make,
	device.updated,
	session.session_id,
	session.affiliate,
	session.created,
	session.channel,
	session.language,
	session.status,
	session.platform,
	session.query_id,
	session.updated
	FROM sessions as session
	LEFT OUTER JOIN devices AS device
	ON session.session_id = device.session_id
	WHERE session.session_id = $1
`
	retrieveQuery3 = `
UPDATE sessions
SET updated = now()
WHERE session_id = $1
`
	retrieveQuery4 = `
SELECT session_id, id, id, session_id, created, value, updated
FROM customers
WHERE session_id = $1
`
	retrieveQuery5 = `
SELECT session_id, id, id, session_id, created, value, updated
FROM parameters
WHERE session_id = $1
`
	retrieveQuery6 = `
SELECT session_id, id, id, session_id, created, value, updated
FROM variants
WHERE session_id = $1
`
	retrieveQuery7 = `
SELECT d.session_id AS device_session_id,
	d.created AS device_created,
	d.device_id AS device_id,
	d.make AS make,
	d.model AS model,
	d.name AS name,
	d.serial_number AS name,
	d.updated AS device_updated,
	s.session_id AS session_id,
	s.affiliate AS affiliate,
	s.channel AS channel,
	s.created AS session_created,
	s.language AS language,
	s.platform AS platform,
	s.query_id AS query_id,
	s.status AS status,
	s.updated AS session_updated
FROM devices AS d
LEFT JOIN sessions AS s
ON d.session_id = s.session_id
WHERE d.session_id = $1
`

	// Update Queries
	updateQuery1 = `
UPDATE sessions
SET query_id = $1, updated = now()
WHERE session_id = $2
`
	updateQuery2 = `
UPDATE sessions
SET status = $1, updated = now()
WHERE session_id = $2
`

	// Fetch random session ID queries.
	findSessionIDQuery1 = `
SELECT session_id
FROM sessions
WHERE session_id > $1
LIMIT 1
`
	findSessionIDQuery2 = `
SELECT session_id
FROM sessions
WHERE session_id > $1
LIMIT 1
`
)

var (
	retrieveQueries = []string{
		retrieveQuery1,
		retrieveQuery2,
		retrieveQuery3,
		retrieveQuery4,
		retrieveQuery5,
		retrieveQuery6,
		retrieveQuery7,
	}

	// All retrieve queries are run in an update operation before the update
	// queries.
	updateQueries = []string{
		updateQuery1,
		updateQuery2,
	}
)

func init() {
	workload.Register(interleavedPartitionedMeta)
}

type interleavedPartitioned struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	locality string

	// data distribution flags
	customersPerSession  int
	devicesPerSession    int
	variantsPerSession   int
	parametersPerSession int
	queriesPerSession    int

	// flags for initial db loading
	initEastPercent int
	initSessions    int

	// operation flags
	insertPercent        int
	insertLocalPercent   int
	retrievePercent      int
	retrieveLocalPercent int
	updatePercent        int
	updateLocalPercent   int

	// info for delete jobs
	deletes       bool // set based on zone, not a flag
	rowsPerDelete int

	// zones
	eastZoneName    string
	westZoneName    string
	centralZoneName string

	// prepared statements
	retrieveStatements       []*gosql.Stmt
	updateStatements         []*gosql.Stmt
	insertStatement          *gosql.Stmt
	insertCustomerStatement  *gosql.Stmt
	insertDeviceStatement    *gosql.Stmt
	insertVariantStatement   *gosql.Stmt
	insertParameterStatement *gosql.Stmt
	insertQueryStatement     *gosql.Stmt
	deleteEastStatement      *gosql.Stmt
	deleteWestStatement      *gosql.Stmt
	findSessionIDStatement1  *gosql.Stmt
	findSessionIDStatement2  *gosql.Stmt
}

var interleavedPartitionedMeta = workload.Meta{
	Name:        `interleavedpartitioned`,
	Description: `Tests the performance of tables that are both interleaved and partitioned`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		g := &interleavedPartitioned{}
		g.flags.FlagSet = pflag.NewFlagSet(`interleavedpartitioned`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`batch`: {RuntimeOnly: true},
		}
		g.flags.IntVar(&g.initSessions, `init-sessions`, 1000, `Number of sessions (rows in the parent table) to create during initialization`)
		g.flags.IntVar(&g.customersPerSession, `customers-per-session`, 2, `Number of customers associated with each session`)
		g.flags.IntVar(&g.devicesPerSession, `devices-per-session`, 2, `Number of devices associated with each session`)
		g.flags.IntVar(&g.variantsPerSession, `variants-per-session`, 5, `Number of variants associated with each session`)
		g.flags.IntVar(&g.parametersPerSession, `parameters-per-session`, 1, `Number of parameters associated with each session`)
		g.flags.IntVar(&g.queriesPerSession, `queries-per-session`, 1, `Number of queries associated with each session`)
		g.flags.IntVar(&g.initEastPercent, `init-east-percent`, 50, `Percentage (0-100) of sessions that are in us-east used when initializing rows only`)
		g.flags.IntVar(&g.insertPercent, `insert-percent`, 70, `Percentage (0-100) of operations that are inserts`)
		g.flags.IntVar(&g.insertLocalPercent, `insert-local-percent`, 100, `Percentage of insert operations that are local`)
		g.flags.IntVar(&g.retrievePercent, `retrieve-percent`, 20, `Percentage (0-100) of operations that are retrieval queries`)
		g.flags.IntVar(&g.retrieveLocalPercent, `retrieve-local-percent`, 100, `Percentage of retrieve operations that are local`)
		g.flags.IntVar(&g.updatePercent, `update-percent`, 10, `Percentage (0-100) of operations that are update queries`)
		g.flags.IntVar(&g.updateLocalPercent, `update-local-percent`, 100, `Percentage of update operations that are local`)
		g.flags.IntVar(&g.rowsPerDelete, `rows-per-delete`, 1, `Number of rows per delete operation`)
		g.flags.StringVar(&g.eastZoneName, `east-zone-name`, `us-east1-b`, `Name of the zone to be used as east`)
		g.flags.StringVar(&g.westZoneName, `west-zone-name`, `us-west1-b`, `Name of the zone to be used as west`)
		// NB: us-central1-a has been causing issues, see:
		// https://github.com/cockroachdb/cockroach/issues/66184
		g.flags.StringVar(&g.centralZoneName, `central-zone-name`, `us-central1-f`, `Name of the zone to be used as central`)
		g.flags.StringVar(&g.locality, `locality`, ``, `Which locality is the workload running in? (east,west,central)`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (w *interleavedPartitioned) Meta() workload.Meta { return interleavedPartitionedMeta }

// Flags implements the Flagser interface.
func (w *interleavedPartitioned) Flags() workload.Flags { return w.flags }

// Tables implements the Generator interface.
func (w *interleavedPartitioned) Tables() []workload.Table {
	sessionsTable := workload.Table{
		Name:   `sessions`,
		Schema: sessionSchema,
		InitialRows: workload.Tuples(
			w.initSessions,
			w.sessionsInitialRow,
		),
	}
	customerTable := workload.Table{
		Name:   `customers`,
		Schema: genericChildSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: w.initSessions,
			FillBatch:  w.childInitialRowBatchFunc(2, w.customersPerSession),
		},
	}
	devicesTable := workload.Table{
		Name:   `devices`,
		Schema: deviceSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: w.initSessions,
			FillBatch:  w.deviceInitialRowBatch,
		},
	}
	variantsTable := workload.Table{
		Name:   `variants`,
		Schema: genericChildSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: w.initSessions,
			FillBatch:  w.childInitialRowBatchFunc(3, w.variantsPerSession),
		},
	}
	parametersTable := workload.Table{
		Name:   `parameters`,
		Schema: genericChildSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: w.initSessions,
			FillBatch:  w.childInitialRowBatchFunc(4, w.parametersPerSession),
		},
	}
	queriesTable := workload.Table{
		Name:   `queries`,
		Schema: querySchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: w.initSessions,
			FillBatch:  w.queryInitialRowBatch,
		},
	}
	return []workload.Table{
		sessionsTable, customerTable, devicesTable, variantsTable, parametersTable, queriesTable,
	}
}

// Ops implements the Opser interface.
func (w *interleavedPartitioned) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(w, ``, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}

	db.SetMaxOpenConns(w.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(w.connFlags.Concurrency + 1)

	ql := workload.QueryLoad{
		SQLDatabase: sqlDatabase,
	}

	workerCount := w.connFlags.Concurrency
	if w.deletes {
		// Only run a single worker function if performing deletes.
		workerCount = 1
	}

	// Prepare the queries, stmts are safe for concurrent use.
	w.retrieveStatements = make([]*gosql.Stmt, len(retrieveQueries))
	for i, query := range retrieveQueries {
		var err error
		w.retrieveStatements[i], err = db.Prepare(query)
		if err != nil {
			return workload.QueryLoad{}, err
		}
	}
	w.updateStatements = make([]*gosql.Stmt, len(updateQueries))
	for i, query := range updateQueries {
		var err error
		w.updateStatements[i], err = db.Prepare(query)
		if err != nil {
			return workload.QueryLoad{}, err
		}
	}
	w.insertStatement, err = db.Prepare(insertQuery)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	w.insertCustomerStatement, err = db.Prepare(insertQueryCustomers)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	w.insertDeviceStatement, err = db.Prepare(insertQueryDevices)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	w.insertVariantStatement, err = db.Prepare(insertQueryVariants)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	w.insertParameterStatement, err = db.Prepare(insertQueryParameters)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	w.insertQueryStatement, err = db.Prepare(insertQueryQuery)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	w.deleteEastStatement, err = db.Prepare(deleteEastQuery)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	w.deleteWestStatement, err = db.Prepare(deleteWestQuery)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	w.findSessionIDStatement1, err = db.Prepare(findSessionIDQuery1)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	w.findSessionIDStatement2, err = db.Prepare(findSessionIDQuery2)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	for i := 0; i < workerCount; i++ {
		workerID := i
		ql.WorkerFns = append(ql.WorkerFns, func(ctx context.Context) error {
			rng := rand.New(rand.NewSource(timeutil.Now().Add(time.Hour * time.Duration(i)).UnixNano()))

			hists := reg.GetHandle()
			if w.deletes {
				return w.deleteFunc(ctx, hists, rng)
			}

			operation := rng.Intn(100)
			switch {
			case operation < w.insertPercent: // insert
				return w.insertFunc(ctx, db, hists, rng, workerID)
			case operation < w.insertPercent+w.retrievePercent: // retrieve
				return w.retrieveFunc(ctx, hists, rng)
			case operation < w.insertPercent+w.retrievePercent+w.updatePercent: // update
				return w.updateFunc(ctx, hists, rng)
			default: // No operation.
				return nil
			}
		})
	}

	return ql, nil
}

func (w *interleavedPartitioned) deleteFunc(
	ctx context.Context, hists *histogram.Histograms, rng *rand.Rand,
) error {
	start := timeutil.Now()
	var statement *gosql.Stmt
	// Prepare the statements.
	if rng.Intn(2) > 0 {
		statement = w.deleteEastStatement
	} else {
		statement = w.deleteWestStatement
	}
	// Execute the statements.
	if _, err := statement.ExecContext(ctx, w.rowsPerDelete); err != nil {
		return err
	}
	// Record Stats.
	elapsed := timeutil.Since(start)
	hists.Get(`delete`).Record(elapsed)
	return nil
}

func (w *interleavedPartitioned) insertFunc(
	ctx context.Context, db *gosql.DB, hists *histogram.Histograms, rng *rand.Rand, workerID int,
) error {
	start := timeutil.Now()
	// Execute the transaction.
	if err := crdb.ExecuteTx(
		context.Background(),
		db,
		nil, /* txopts */
		func(tx *gosql.Tx) error {
			// Get the node id.
			var nodeID int
			if err := tx.QueryRow(nodeIDQuery).Scan(&nodeID); err != nil {
				return err
			}

			sessionID := randomSessionIDForInsert(rng, w.locality, w.insertLocalPercent, nodeID, workerID)
			args := []interface{}{
				sessionID,            // session_id
				randString(rng, 100), // affiliate
				randString(rng, 50),  // channel
				randString(rng, 20),  // language
				randString(rng, 20),  // status
				randString(rng, 50),  // platform
				randString(rng, 100), // query_id
			}
			if _, err := tx.StmtContext(ctx, w.insertStatement).ExecContext(ctx, args...); err != nil {
				return err
			}
			for i := 0; i < w.customersPerSession; i++ {
				args := []interface{}{
					sessionID,           // session_id
					randString(rng, 50), // id
					randString(rng, 50), // value
				}
				if _, err := tx.StmtContext(ctx, w.insertCustomerStatement).ExecContext(ctx, args...); err != nil {
					return err
				}
			}
			for i := 0; i < w.devicesPerSession; i++ {
				args := []interface{}{
					sessionID,            // session_id
					randString(rng, 100), // id
					randString(rng, 50),  // device_id
					randString(rng, 50),  // name
					randString(rng, 50),  // make
					randString(rng, 50),  // macaddress
					randString(rng, 50),  // model
					randString(rng, 50),  // serial_number
				}
				if _, err := tx.StmtContext(ctx, w.insertDeviceStatement).ExecContext(ctx, args...); err != nil {
					return err
				}
			}
			for i := 0; i < w.variantsPerSession; i++ {
				args := []interface{}{
					sessionID,           // session_id
					randString(rng, 50), // id
					randString(rng, 50), // value
				}
				if _, err := tx.StmtContext(ctx, w.insertVariantStatement).ExecContext(ctx, args...); err != nil {
					return err
				}
			}
			for i := 0; i < w.parametersPerSession; i++ {
				args := []interface{}{
					sessionID,           // session_id
					randString(rng, 50), // id
					randString(rng, 50), // value
				}
				if _, err := tx.StmtContext(ctx, w.insertParameterStatement).ExecContext(ctx, args...); err != nil {
					return err
				}
			}
			for i := 0; i < w.queriesPerSession; i++ {
				args := []interface{}{
					sessionID,           // session_id
					randString(rng, 50), // id
				}
				if _, err := tx.StmtContext(ctx, w.insertQueryStatement).ExecContext(ctx, args...); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
		return err
	}
	// Record Stats.
	elapsed := timeutil.Since(start)
	hists.Get(`insert`).Record(elapsed)
	return nil
}

func (w *interleavedPartitioned) fetchSessionID(
	ctx context.Context,
	rng *rand.Rand,
	hists *histogram.Histograms,
	locality string,
	localPercent int,
) (string, error) {
	start := timeutil.Now()
	baseSessionID := randomSessionID(rng, locality, localPercent)
	var sessionID string
	if err := w.findSessionIDStatement1.QueryRowContext(ctx, baseSessionID).Scan(&sessionID); err != nil && !errors.Is(err, gosql.ErrNoRows) {
		return "", err
	}
	// Didn't find a next session ID, let's try the other way.
	if len(sessionID) == 0 {
		if err := w.findSessionIDStatement2.QueryRowContext(ctx, baseSessionID).Scan(&sessionID); err != nil && !errors.Is(err, gosql.ErrNoRows) {
			return "", err
		}
	}
	elapsed := timeutil.Since(start)
	if len(sessionID) == 0 {
		hists.Get(`findNoID`).Record(elapsed)
	} else {
		hists.Get(`findID`).Record(elapsed)
	}
	return sessionID, nil
}

func (w *interleavedPartitioned) retrieveFunc(
	ctx context.Context, hists *histogram.Histograms, rng *rand.Rand,
) error {
	sessionID, err := w.fetchSessionID(ctx, rng, hists, w.locality, w.retrieveLocalPercent)
	if err != nil {
		return err
	}
	//Could not find a session ID, skip this operation.
	if len(sessionID) == 0 {
		return nil
	}

	start := timeutil.Now()

	// Execute the queries.
	for i, statement := range w.retrieveStatements {
		if _, err := statement.ExecContext(ctx, sessionID); err != nil {
			return errors.Wrapf(err, "error with query: %s", retrieveQueries[i])
		}
	}
	// Record Stats.
	elapsed := timeutil.Since(start)
	hists.Get(`retrieve`).Record(elapsed)
	return nil
}

func (w *interleavedPartitioned) updateFunc(
	ctx context.Context, hists *histogram.Histograms, rng *rand.Rand,
) error {
	sessionID, err := w.fetchSessionID(ctx, rng, hists, w.locality, w.updateLocalPercent)
	if err != nil {
		return err
	}
	//Could not find a session ID, skip this operation.
	if len(sessionID) == 0 {
		return nil
	}

	start := timeutil.Now()
	// Execute the statements.
	for i, statement := range w.retrieveStatements {
		if _, err = statement.ExecContext(ctx, sessionID); err != nil {
			return errors.Wrapf(err, "error with query: %s", retrieveQueries[i])
		}
	}
	for i, statement := range w.updateStatements {
		if _, err = statement.ExecContext(ctx, randString(rng, 20), sessionID); err != nil {
			return errors.Wrapf(err, "error with query: %s", updateQueries[i])
		}
	}
	// Record Stats.
	elapsed := timeutil.Since(start)
	hists.Get(`updates`).Record(elapsed)
	return nil
}

// Hooks implements the Hookser interface.
func (w *interleavedPartitioned) Hooks() workload.Hooks {
	return workload.Hooks{
		PreCreate: func(db *gosql.DB) error {
			if _, err := db.Exec(`SET CLUSTER SETTING sql.defaults.interleaved_tables.enabled = true`); err != nil {
				return err
			}
			return nil
		},
		PreLoad: func(db *gosql.DB) error {
			if _, err := db.Exec(
				zoneLocationsStmt, w.eastZoneName, w.westZoneName, w.centralZoneName,
			); err != nil {
				return err
			}

			if _, err := db.Exec(
				fmt.Sprintf(
					"ALTER PARTITION west OF TABLE sessions CONFIGURE ZONE USING"+
						" lease_preferences = '[[+zone=%[1]s]]', "+
						"constraints = '{+zone=%[1]s : 1}', num_replicas = 3",
					w.westZoneName,
				),
			); err != nil {
				return errors.Wrapf(err, "could not set zone for partition west")
			}
			if _, err := db.Exec(
				fmt.Sprintf(
					"ALTER PARTITION east OF TABLE sessions CONFIGURE ZONE USING"+
						" lease_preferences = '[[+zone=%[1]s]]', "+
						"constraints = '{+zone=%[1]s : 1}', num_replicas = 3",
					w.eastZoneName,
				),
			); err != nil {
				return errors.Wrapf(err, "could not set zone for partition east")
			}
			return nil
		},
		Validate: func() error {
			switch w.locality {
			case `east`, `west`:
			case `central`:
				w.deletes = true
				w.insertPercent = 0
				w.retrievePercent = 0
				w.updatePercent = 0
				log.Info(context.Background(),
					"locality is set to central, turning deletes on and everything else off",
				)
				return nil

			default:
				return errors.New("invalid locality (needs to be east, west, or central)")
			}
			if w.insertPercent+w.retrievePercent+w.updatePercent != 100 {
				return errors.New(
					"operation percents ({insert,retrieve,delete}-percent flags) must add up to 100",
				)
			}
			return nil
		},
	}
}

func (w *interleavedPartitioned) sessionsInitialRow(rowIdx int) []interface{} {
	rng := rand.New(rand.NewSource(int64(rowIdx)))
	// Set the time for the now string to be minus 5 mins so delete operations can
	// start right away.
	nowString := timeutil.Now().Add(time.Minute * time.Duration(-5)).UTC().Format(time.RFC3339)
	sessionID := randomSessionID(rng, `east`, w.initEastPercent)
	return []interface{}{
		sessionID,            // session_id
		randString(rng, 100), // affiliate
		randString(rng, 50),  // channel
		randString(rng, 20),  // language
		nowString,            // created
		nowString,            // updated
		randString(rng, 20),  // status
		randString(rng, 50),  // platform
		randString(rng, 100), // query_id
	}
}

var childTypes = []*types.T{
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
}

func (w *interleavedPartitioned) childInitialRowBatchFunc(
	rngFactor int64, nPerBatch int,
) func(int, coldata.Batch, *bufalloc.ByteAllocator) {
	return func(sessionRowIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator) {
		sessionRNG := rand.New(rand.NewSource(int64(sessionRowIdx)))
		sessionID := randomSessionID(sessionRNG, `east`, w.initEastPercent)
		nowString := timeutil.Now().UTC().Format(time.RFC3339)
		rng := rand.New(rand.NewSource(int64(sessionRowIdx) + rngFactor))

		cb.Reset(childTypes, nPerBatch, coldata.StandardColumnFactory)
		sessionIDCol := cb.ColVec(0).Bytes()
		idCol := cb.ColVec(1).Bytes()
		valueCol := cb.ColVec(2).Bytes()
		createdCol := cb.ColVec(3).Bytes()
		updatedCol := cb.ColVec(4).Bytes()
		for rowIdx := 0; rowIdx < nPerBatch; rowIdx++ {
			sessionIDCol.Set(rowIdx, []byte(sessionID))
			idCol.Set(rowIdx, []byte(randString(rng, 50)))
			valueCol.Set(rowIdx, []byte(randString(rng, 50)))
			createdCol.Set(rowIdx, []byte(nowString))
			updatedCol.Set(rowIdx, []byte(nowString))
		}
	}
}

var deviceTypes = []*types.T{
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
}

func (w *interleavedPartitioned) deviceInitialRowBatch(
	sessionRowIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator,
) {
	rng := rand.New(rand.NewSource(int64(sessionRowIdx) * 64))
	sessionRNG := rand.New(rand.NewSource(int64(sessionRowIdx)))
	sessionID := randomSessionID(sessionRNG, `east`, w.initEastPercent)
	nowString := timeutil.Now().UTC().Format(time.RFC3339)

	cb.Reset(deviceTypes, w.devicesPerSession, coldata.StandardColumnFactory)
	sessionIDCol := cb.ColVec(0).Bytes()
	idCol := cb.ColVec(1).Bytes()
	deviceIDCol := cb.ColVec(2).Bytes()
	nameCol := cb.ColVec(3).Bytes()
	makeCol := cb.ColVec(4).Bytes()
	macaddressCol := cb.ColVec(5).Bytes()
	modelCol := cb.ColVec(6).Bytes()
	serialNumberCol := cb.ColVec(7).Bytes()
	createdCol := cb.ColVec(8).Bytes()
	updatedCol := cb.ColVec(9).Bytes()
	for rowIdx := 0; rowIdx < w.devicesPerSession; rowIdx++ {
		sessionIDCol.Set(rowIdx, []byte(sessionID))
		idCol.Set(rowIdx, []byte(randString(rng, 100)))
		deviceIDCol.Set(rowIdx, []byte(randString(rng, 50)))
		nameCol.Set(rowIdx, []byte(randString(rng, 50)))
		makeCol.Set(rowIdx, []byte(randString(rng, 50)))
		macaddressCol.Set(rowIdx, []byte(randString(rng, 50)))
		modelCol.Set(rowIdx, []byte(randString(rng, 50)))
		serialNumberCol.Set(rowIdx, []byte(randString(rng, 50)))
		createdCol.Set(rowIdx, []byte(nowString))
		updatedCol.Set(rowIdx, []byte(nowString))
	}
}

var queryTypes = []*types.T{
	types.Bytes,
	types.Bytes,
	types.Bytes,
	types.Bytes,
}

func (w *interleavedPartitioned) queryInitialRowBatch(
	sessionRowIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator,
) {
	rng := rand.New(rand.NewSource(int64(sessionRowIdx) * 64))
	sessionRNG := rand.New(rand.NewSource(int64(sessionRowIdx)))
	sessionID := randomSessionID(sessionRNG, `east`, w.initEastPercent)
	nowString := timeutil.Now().UTC().Format(time.RFC3339)

	cb.Reset(queryTypes, w.queriesPerSession, coldata.StandardColumnFactory)
	sessionIDCol := cb.ColVec(0).Bytes()
	idCol := cb.ColVec(1).Bytes()
	createdCol := cb.ColVec(2).Bytes()
	updatedCol := cb.ColVec(3).Bytes()
	for rowIdx := 0; rowIdx < w.queriesPerSession; rowIdx++ {
		sessionIDCol.Set(rowIdx, []byte(sessionID))
		idCol.Set(rowIdx, []byte(randString(rng, 50)))
		createdCol.Set(rowIdx, []byte(nowString))
		updatedCol.Set(rowIdx, []byte(nowString))
	}
}

func randomSessionID(rng *rand.Rand, locality string, localPercent int) string {
	return randomSessionIDForInsert(rng, locality, localPercent, 0 /* nodeID */, 0 /* workerID */)
}

func randomSessionIDForInsert(
	rng *rand.Rand, locality string, localPercent int, nodeID int, workerID int,
) string {
	// Is this a local operation? As in an east node accessing east data.
	local := rng.Intn(100) < localPercent
	// There have been some issues of session ID collisions so by adding the node
	// and worker IDs is an attempt to minimize that. If they still occur, it must
	// point to a serious issue and having the IDs should help identify it.
	if (local && locality == `east`) || (!local && locality == `west`) {
		return fmt.Sprintf("E-%s-n%dw%d", randString(rng, 90), nodeID, workerID)
	}
	return fmt.Sprintf("W-%s-n%dw%d", randString(rng, 90), nodeID, workerID)
}

func randString(rng *rand.Rand, length int) string {
	return string(randutil.RandBytes(rng, length))
}
