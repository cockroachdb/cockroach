// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package interleavedpartitioned

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
)

const (
	zoneLocationsStmt = `
UPSERT INTO system.locations VALUES
	('zone', 'us-east1-b', 33.0641249, -80.0433347),
	('zone', 'us-west1-b', 45.6319052, -121.2010282)
`
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
	query_id STRING(100) NOT NULL,
	INDEX con_session_created_idx(created),
	FAMILY "primary" (session_id, affiliate, channel, language, created, updated, status, platform, query_id)
) PARTITION BY RANGE (session_id) (
	PARTITION east VALUES FROM ('E-') TO ('F-'),
	PARTITION west VALUES FROM ('W-') TO ('X-'),
	PARTITION central VALUES FROM ('C-') TO ('D-')
)`
	genericChildSchema = `
(
	session_id STRING(100) NOT NULL,
	key STRING(50) NOT NULL,
	value STRING(50) NOT NULL,
	created TIMESTAMP NOT NULL,
	updated TIMESTAMP NOT NULL,
	PRIMARY KEY (session_id, key),
	FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
	FAMILY "primary" (session_id, key, value, created, updated)
) INTERLEAVE IN PARENT sessions(session_id)`
	deviceSchema = `
(
	id STRING(100) NOT NULL,
	session_id STRING(100) NOT NULL REFERENCES sessions ON DELETE CASCADE,
	device_id STRING(50),
	name STRING(50),
	make STRING(50),
	macaddress STRING(50),
	model STRING(50),
	serialno STRING(50),
	created TIMESTAMP NOT NULL,
	updated TIMESTAMP NOT NULL,
	PRIMARY KEY (session_id, id),
	FAMILY "primary" (id, session_id, device_id, name, make, macaddress, model, serialno, created, updated)
) INTERLEAVE IN PARENT sessions(session_id)
`
	querySchema = `
(
	session_id STRING(100) NOT NULL REFERENCES sessions ON DELETE CASCADE,
	id STRING(50) NOT NULL,
	created TIMESTAMP NOT NULL,
	updated TIMESTAMP NOT NULL,
	PRIMARY KEY (session_id, id),
	FAMILY "primary" (session_id, id, created, updated)
) INTERLEAVE IN PARENT sessions(session_id)
`
	insertQuery           = `INSERT INTO sessions(session_id, affiliate, channel, language, created, updated, status, platform, query_id) VALUES ($1, $2, $3, $4, now(), now(), $5, $6, $7)`
	insertQueryCustomers  = `INSERT INTO customers(session_id, key, value, created, updated) VALUES ($1, $2, $3, now(), now())`
	insertQueryVariants   = `INSERT INTO variants(session_id, key, value, created, updated) VALUES ($1, $2, $3, now(), now())`
	insertQueryParameters = `INSERT INTO parameters(session_id, key, value, created, updated) VALUES ($1, $2, $3, now(), now())`
	insertQueryDevices    = `INSERT INTO devices(id, session_id, device_id, name, make, macaddress, model, serialno, created, updated) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now(), now())`
	insertQueryQuery      = `INSERT INTO queries(session_id, id, created, updated) VALUES ($1, $2, now(), now())`
	deleteQuery           = `DELETE FROM sessions WHERE session_id IN (SELECT session_id FROM sessions LIMIT $1)`
	retrieveQuery0        = `SELECT session_id FROM sessions WHERE session_id > $1 LIMIT 1`
	retrieveQuery1        = `
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
	device.serialno,
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
SELECT session_id, key, key, session_id, created, value, updated
FROM customers
WHERE session_id = $1
`
	retrieveQuery5 = `
SELECT session_id, key, key, session_id, created, value, updated
FROM parameters
WHERE session_id = $1
`
	retrieveQuery6 = `
SELECT session_id, key, key, session_id, created, value, updated
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
	d.serialno AS name,
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
FROM devices AS d LEFT JOIN sessions AS s
ON d.session_id = s.session_id
WHERE d.session_id = $1
`
	updateQuery = `
UPDATE sessions
SET query_id = $1, updated = now()
WHERE session_id = $2
`
	updateQuery2 = `
UPDATE sessions
SET status = $1, updated = now()
WHERE session_id = $2
`
)

var (
	retrieveQueries = []string{retrieveQuery0, retrieveQuery1, retrieveQuery2, retrieveQuery3, retrieveQuery4, retrieveQuery5, retrieveQuery6, retrieveQuery7}
)

func init() {
	workload.Register(interleavedPartitionedMeta)
}

type interleavedPartitioned struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	sessions             int
	customersPerSession  int
	devicesPerSession    int
	variantsPerSession   int
	parametersPerSession int
	queriesPerSession    int

	// flags for setting operations
	insertPercent   int
	retrievePercent int
	updatePercent   int
	deletePercent   int

	eastPercent          int
	east                 bool
	insertLocalPercent   int
	retrieveLocalPercent int
	updateLocalPercent   int

	deleteBatchSize int

	sessionIDs []string
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
		g.flags.IntVar(&g.sessions, `sessions`, 1000, `Number of sessions (rows in the parent table)`)
		g.flags.IntVar(&g.customersPerSession, `customers-per-session`, 2, `Number of customers associated with each session`)
		g.flags.IntVar(&g.devicesPerSession, `devices-per-session`, 2, `Number of devices associated with each session`)
		g.flags.IntVar(&g.variantsPerSession, `variants-per-session`, 5, `Number of variants associated with each session`)
		g.flags.IntVar(&g.parametersPerSession, `parameters-per-session`, 1, `Number of parameters associated with each session`)
		g.flags.IntVar(&g.queriesPerSession, `queries-per-session`, 1, `Number of queries associated with each session`)
		g.flags.IntVar(&g.eastPercent, `east-percent`, 50, `Percentage (0-100) of sessions that are in us-east`)
		g.flags.IntVar(&g.insertPercent, `insert-percent`, 50, `Percentage (0-100) of operations that are inserts`)
		g.flags.IntVar(&g.insertLocalPercent, `insert-local-percent`, 100, `Percentage of insert operations that are local`)
		g.flags.IntVar(&g.retrievePercent, `retrieve-percent`, 0, `Percentage (0-100) of operations that are retrieval queries`)
		g.flags.IntVar(&g.retrieveLocalPercent, `retrieve-local-percent`, 100, `Percentage of retrieve operations that are local`)
		g.flags.IntVar(&g.updatePercent, `update-percent`, 0, `Percentage (0-100) of operations that are update queries`)
		g.flags.IntVar(&g.updateLocalPercent, `update-local-percent`, 100, `Percentage of update operations that are local`)
		g.flags.IntVar(&g.deletePercent, `delete-percent`, 50, `Percentage (0-100) of operations that are delete queries`)
		g.flags.IntVar(&g.deleteBatchSize, `delete-batch-size`, 100, `Number of rows per delete operation`)
		g.flags.BoolVar(&g.east, `east`, true, `Is this location in the east (true) or the west (false)`)
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
			w.sessions,
			w.sessionsInitialRow,
		),
	}
	customerTable := workload.Table{
		Name:   `customers`,
		Schema: genericChildSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: w.sessions,
			Batch:      w.childInitialRowBatchFunc(2, w.customersPerSession),
		},
	}
	devicesTable := workload.Table{
		Name:   `devices`,
		Schema: deviceSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: w.sessions,
			Batch:      w.deviceInitialRowBatch,
		},
	}
	variantsTable := workload.Table{
		Name:   `variants`,
		Schema: genericChildSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: w.sessions,
			Batch:      w.childInitialRowBatchFunc(3, w.variantsPerSession),
		},
	}
	parametersTable := workload.Table{
		Name:   `parameters`,
		Schema: genericChildSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: w.sessions,
			Batch:      w.childInitialRowBatchFunc(4, w.parametersPerSession),
		},
	}
	queriesTable := workload.Table{
		Name:   `queries`,
		Schema: querySchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: w.sessions,
			Batch:      w.queryInitialRowBatch,
		},
	}
	return []workload.Table{sessionsTable, customerTable, devicesTable, variantsTable, parametersTable, queriesTable}
}

// Ops implements the Opser interface.
func (w *interleavedPartitioned) Ops(
	urls []string, reg *workload.HistogramRegistry,
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

	if err != nil {
		return workload.QueryLoad{}, err
	}

	ql := workload.QueryLoad{
		SQLDatabase: sqlDatabase,
	}
	workerFn := func(ctx context.Context) error {
		hists := reg.GetHandle()
		rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
		opRand := rng.Intn(100)
		if opRand < w.insertPercent {
			log.Info(context.TODO(), "inserting")
			start := timeutil.Now()
			insertStatement, err := db.Prepare(insertQuery)
			if err != nil {
				return err
			}
			sessionID := w.randomSessionID(rng, w.pickLocality(rng, w.insertLocalPercent))
			args := []interface{}{
				sessionID,            // session_id
				randString(rng, 100), // affiliate
				randString(rng, 50),  // channel
				randString(rng, 20),  // language
				randString(rng, 20),  // status
				randString(rng, 50),  // platform
				randString(rng, 100), // query_id
			}
			_, err = insertStatement.ExecContext(ctx, args...)
			if err != nil {
				return err
			}
			for i := 0; i < w.customersPerSession; i++ {
				insertCustomerStatement, err := db.Prepare(insertQueryCustomers)
				if err != nil {
					return err
				}
				args := []interface{}{
					sessionID,
					randString(rng, 50),
					randString(rng, 50),
				}
				_, err = insertCustomerStatement.ExecContext(ctx, args...)
				if err != nil {
					return err
				}
			}
			for i := 0; i < w.devicesPerSession; i++ {
				insertDeviceStatement, err := db.Prepare(insertQueryDevices)
				if err != nil {
					return err
				}
				args := []interface{}{
					randString(rng, 100),
					sessionID,
					randString(rng, 50), // device_id
					randString(rng, 50), // name
					randString(rng, 50), // make
					randString(rng, 50), // macaddress
					randString(rng, 50), // model
					randString(rng, 50), // serialno
				}
				_, err = insertDeviceStatement.ExecContext(ctx, args...)
				if err != nil {
					return err
				}
			}
			for i := 0; i < w.variantsPerSession; i++ {
				insertVariantStatement, err := db.Prepare(insertQueryVariants)
				if err != nil {
					return err
				}
				args := []interface{}{
					sessionID,
					randString(rng, 50),
					randString(rng, 50),
				}
				_, err = insertVariantStatement.ExecContext(ctx, args...)
				if err != nil {
					return err
				}
			}
			for i := 0; i < w.customersPerSession; i++ {
				insertParameterStatement, err := db.Prepare(insertQueryParameters)
				if err != nil {
					return err
				}
				args := []interface{}{
					sessionID,
					randString(rng, 50),
					randString(rng, 50),
				}
				_, err = insertParameterStatement.ExecContext(ctx, args...)
				if err != nil {
					return err
				}
			}
			for i := 0; i < w.customersPerSession; i++ {
				insertQueryStatement, err := db.Prepare(insertQueryQuery)
				if err != nil {
					return err
				}
				args := []interface{}{
					sessionID,
					randString(rng, 50),
				}
				_, err = insertQueryStatement.ExecContext(ctx, args...)
				if err != nil {
					return err
				}
			}
			hists.Get(`insert`).Record(timeutil.Since(start))
			return nil
		} else if opRand < w.insertPercent+w.retrievePercent { // retrieve
			log.Info(context.TODO(), "querying")
			sessionID := w.randomSessionID(rng, w.pickLocality(rng, w.retrieveLocalPercent))
			args := []interface{}{
				sessionID,
			}
			start := timeutil.Now()
			for _, query := range retrieveQueries {
				retrieveStatement, err := db.Prepare(query)
				if err != nil {
					return err
				}
				_, err = retrieveStatement.ExecContext(ctx, args...)
				if err != nil {
					return err
				}
			}
			hists.Get(`retrieve`).Record(timeutil.Since(start))
			return nil
		} else if opRand < w.insertPercent+w.retrievePercent+w.updatePercent { // update
			log.Info(context.TODO(), "updating")
			sessionID := w.randomSessionID(rng, w.pickLocality(rng, w.updateLocalPercent))
			retrieveArgs := []interface{}{
				sessionID,
			}
			start := timeutil.Now()
			for _, query := range retrieveQueries {
				retrieveStatement, err := db.Prepare(query)
				if err != nil {
					return err
				}
				_, err = retrieveStatement.ExecContext(ctx, retrieveArgs...)
				if err != nil {
					return err
				}
			}
			updateStatement1, err := db.Prepare(updateQuery)
			if err != nil {
				return err
			}
			if _, err = updateStatement1.ExecContext(ctx, randString(rng, 100), sessionID); err != nil {
				return err
			}
			updateStatement2, err := db.Prepare(updateQuery2)
			if err != nil {
				return err
			}
			_, err = updateStatement2.ExecContext(ctx, randString(rng, 20), sessionID)
			hists.Get(`updates`).Record(timeutil.Since(start))
			return err
		} else if opRand < w.insertPercent+w.retrievePercent+w.updatePercent+w.deletePercent { // delete
			log.Info(context.TODO(), "deleting")
			start := timeutil.Now()
			deleteStatement, err := db.Prepare(deleteQuery)
			if err != nil {
				return err
			}
			_, err = deleteStatement.ExecContext(ctx, w.deleteBatchSize)
			if err != nil {
				return err
			}
			hists.Get(`delete`).Record(timeutil.Since(start))
		}

		return nil
	}

	for i := 0; i < w.connFlags.Concurrency; i++ {
		ql.WorkerFns = append(ql.WorkerFns, workerFn)
	}

	return ql, nil
}

// Hooks implements the Hookser interface.
func (w *interleavedPartitioned) Hooks() workload.Hooks {
	return workload.Hooks{
		PreLoad: func(db *gosql.DB) error {
			log.Info(context.TODO(), "Preload")
			if _, err := db.Exec(zoneLocationsStmt); err != nil {
				return err
			}
			if _, err := db.Exec(
				"ALTER PARTITION west OF TABLE sessions EXPERIMENTAL CONFIGURE ZONE 'experimental_lease_preferences: [[+zone=us-west1-b]]'",
			); err != nil {
				return errors.Wrapf(err, "could not set zone for partition east")
			}
			if _, err := db.Exec(
				"ALTER PARTITION east OF TABLE sessions EXPERIMENTAL CONFIGURE ZONE 'experimental_lease_preferences: [[+zone=us-east4-b]]'",
			); err != nil {
				return errors.Wrapf(err, "could not set zone for partition west")
			}
			return nil
		},
	}
}

func (w *interleavedPartitioned) sessionsInitialRow(rowIdx int) []interface{} {
	rng := rand.New(rand.NewSource(int64(rowIdx)))
	nowString := timeutil.Now().UTC().Format(time.RFC3339)
	sessionID := w.generateSessionID(rng, rowIdx)
	w.sessionIDs = append(w.sessionIDs, sessionID)
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

func (w *interleavedPartitioned) childInitialRowBatchFunc(
	rngFactor int64, nPerBatch int,
) func(int) [][]interface{} {
	return func(sessionRowIdx int) [][]interface{} {
		sessionRNG := rand.New(rand.NewSource(int64(sessionRowIdx)))
		sessionID := w.generateSessionID(sessionRNG, sessionRowIdx)
		nowString := timeutil.Now().UTC().Format(time.RFC3339)
		rng := rand.New(rand.NewSource(int64(sessionRowIdx) + rngFactor))
		var rows [][]interface{}
		for i := 0; i < nPerBatch; i++ {
			rows = append(rows, []interface{}{
				sessionID,
				randString(rng, 50), // key
				randString(rng, 50), // value
				nowString,           // created
				nowString,           // updated
			})
		}
		return rows
	}
}

func (w *interleavedPartitioned) deviceInitialRowBatch(sessionRowIdx int) [][]interface{} {
	rng := rand.New(rand.NewSource(int64(sessionRowIdx) * 64))
	sessionRNG := rand.New(rand.NewSource(int64(sessionRowIdx)))
	sessionID := w.generateSessionID(sessionRNG, sessionRowIdx)
	nowString := timeutil.Now().UTC().Format(time.RFC3339)
	var rows [][]interface{}
	for i := 0; i < w.devicesPerSession; i++ {
		rows = append(rows, []interface{}{
			randString(rng, 100), // id
			sessionID,
			randString(rng, 50), // device_id
			randString(rng, 50), // name
			randString(rng, 50), // make
			randString(rng, 50), // macaddress
			randString(rng, 50), // model
			randString(rng, 50), // serialno
			nowString,           // created
			nowString,           // updated
		})
	}
	return rows
}

func (w *interleavedPartitioned) queryInitialRowBatch(sessionRowIdx int) [][]interface{} {
	var rows [][]interface{}
	rng := rand.New(rand.NewSource(int64(sessionRowIdx) * 64))
	sessionRNG := rand.New(rand.NewSource(int64(sessionRowIdx)))
	sessionID := w.generateSessionID(sessionRNG, sessionRowIdx)
	nowString := timeutil.Now().UTC().Format(time.RFC3339)
	for i := 0; i < w.queriesPerSession; i++ {
		rows = append(rows, []interface{}{
			sessionID,
			randString(rng, 50), // id
			nowString,           // created
			nowString,           // updated
		})
	}
	return rows
}

func (w *interleavedPartitioned) pickLocality(rng *rand.Rand, percent int) bool {
	localRand := rng.Intn(100)
	if localRand < percent {
		return w.east
	}
	return !w.east
}

func (w *interleavedPartitioned) randomSessionID(rng *rand.Rand, east bool) string {
	id := randString(rng, 98)
	if east {
		return fmt.Sprintf("E-%s", id)
	}
	return fmt.Sprintf("W-%s", id)
}

func (w *interleavedPartitioned) generateSessionID(rng *rand.Rand, rowIdx int) string {
	sessionIDBase := randString(rng, 98)
	if (rowIdx % 100) >= w.eastPercent {
		return fmt.Sprintf("E-%s", sessionIDBase)
	}
	return fmt.Sprintf("W-%s", sessionIDBase)
}

func randString(rng *rand.Rand, length int) string {
	return string(randutil.RandBytes(rng, length))
}
