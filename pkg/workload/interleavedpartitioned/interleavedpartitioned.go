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

	"github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
)

const (
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
	insertQuery    = `INSERT INTO sessions(session_id, affiliate, channel, language, created, updated, status, platform, query_id) VALUES ($1, $2, $3, $4, now(), now(), $5, $6, $7)`
	deleteQuery    = `DELETE FROM sessions WHERE session_id IN (SELECT session_id FROM sessions LIMIT $1)`
	retrieveQuery0 = `SELECT session_id FROM sessions WHERE session_id > $1 LIMIT 1`
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
	updateQuery = `
UPDATE sessions
SET affiliate = $1, updated = now()
WHERE session_id = $2
`
)

var (
	retrieveQueries = []string{retrieveQuery0, retrieveQuery1, retrieveQuery2, retrieveQuery3, retrieveQuery4, retrieveQuery5, retrieveQuery6}
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
	eastPercent          int

	// flags for setting operations
	insertPercent int
	queryPercent  int
	updatePercent int
	deletePercent int

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
		g.flags.IntVar(&g.eastPercent, `east-percent`, 50, `Percentage of sessions that are in us-east`)
		g.flags.IntVar(&g.insertPercent, `insert-percent`, 50, `Percentage of operations that are inserts`)
		g.flags.IntVar(&g.queryPercent, `query-percent`, 0, `Percentage of operations that are retrieval queries`)
		g.flags.IntVar(&g.updatePercent, `update-percent`, 0, `Percentage of operations that are update queries`)
		g.flags.IntVar(&g.deletePercent, `delete-percent`, 50, `Percentage of operations that are delete queries`)
		g.flags.IntVar(&g.deleteBatchSize, `delete-batch-size`, 100, `Number of rows per delete operation`)
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

	for i := 0; i < w.connFlags.Concurrency; i++ {
		ql.WorkerFns = append(ql.WorkerFns, func(ctx context.Context) error {
			hists := reg.GetHandle()
			rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
			opRand := rng.Intn(100)
			log.Warningf(context.TODO(), "opRand = %d", opRand)
			if opRand < w.insertPercent {
				log.Warning(context.TODO(), "inserting")
				start := timeutil.Now()
				insertStatement, err := db.Prepare(insertQuery)
				if err != nil {
					return err
				}
				sessionID := w.generateSessionID(rng, randInt(rng, 0, 100))
				args := []interface{}{
					sessionID,            // session_id
					randString(rng, 100), // affiliate
					randString(rng, 50),  // channel
					randString(rng, 20),  // language
					randString(rng, 20),  // status
					randString(rng, 50),  // platform
					randString(rng, 100), // query_id
				}
				rows, err := insertStatement.Query(args...)
				if err != nil {
					return err
				}
				hists.Get(`insert`).Record(timeutil.Since(start))
				return rows.Err()
			} else if opRand < w.insertPercent+w.queryPercent { // query
				log.Warning(context.TODO(), "querying")
				sessionID := w.generateSessionID(rng, randInt(rng, 0, 100))
				args := []interface{}{
					sessionID,
				}
				start := timeutil.Now()
				for _, query := range retrieveQueries {
					retrieveStatement, err := db.Prepare(query)
					if err != nil {
						return err
					}
					rows, err := retrieveStatement.Query(args...)
					if err != nil {
						return err
					}
					if rows.Err() != nil {
						return rows.Err()
					}
				}
				hists.Get(`retrieve`).Record(timeutil.Since(start))
			} else if opRand < w.insertPercent+w.queryPercent+w.updatePercent { // update
				log.Warning(context.TODO(), "updating")
				sessionID := w.generateSessionID(rng, randInt(rng, 0, 100))
				args := []interface{}{
					sessionID,
				}
				start := timeutil.Now()
				for _, query := range retrieveQueries {
					retrieveStatement, err := db.Prepare(query)
					if err != nil {
						return err
					}
					rows, err := retrieveStatement.Query(args...)
					if err != nil {
						return err
					}
					if rows.Err() != nil {
						return rows.Err()
					}
				}

				updateStatement, err := db.Prepare(updateQuery)
				if err != nil {
					return err
				}
				rows, err := updateStatement.Query(randString(rng, 20), sessionID)
				if err != nil {
					return err
				}
				hists.Get(`updates`).Record(timeutil.Since(start))
				return rows.Err()
			}
			// else case, delete
			log.Warning(context.TODO(), "deleting")
			start := timeutil.Now()
			deleteStatement, err := db.Prepare(deleteQuery)
			if err != nil {
				return err
			}
			rows, err := deleteStatement.Query(w.deleteBatchSize)
			if err != nil {
				return err
			}
			hists.Get(`delete`).Record(timeutil.Since(start))
			return rows.Err()
		})
	}

	return ql, nil
}

func (w *interleavedPartitioned) sessionsInitialRow(rowIdx int) []interface{} {
	rng := rand.New(rand.NewSource(int64(rowIdx)))
	nowString := timeutil.Now().UTC().Format(time.RFC3339)
	sessionID := w.generateSessionID(rng, rowIdx)
	w.sessionIDs = append(w.sessionIDs, sessionID)
	log.Warningf(context.TODO(), "inserting into parent row %d, len of sessionIDs is now %d", rowIdx, len(w.sessionIDs))
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
		log.Warningf(context.TODO(), "inserting into child %d, sessionRowIdx %d. len(w.sessionIDs) = %d", rngFactor, sessionRowIdx, len(w.sessionIDs))
		rng := rand.New(rand.NewSource(int64(sessionRowIdx) + rngFactor))
		if sessionRowIdx >= len(w.sessionIDs) {
			sessionRowIdx = randInt(rng, 0, len(w.sessionIDs)-1)
			// log.Warningf(context.TODO(), "len(w.sessionIDs): %d, w.sessionIDs[len] = %s", len(w.sessionIDs), w.sessionIDs[sessionRowIdx])
		}
		sessionID := w.sessionIDs[sessionRowIdx]
		nowString := timeutil.Now().UTC().Format(time.RFC3339)
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
	log.Warningf(context.TODO(), "inserting into devices, sessionRowIdx %d", sessionRowIdx)
	rng := rand.New(rand.NewSource(int64(sessionRowIdx) * 64))
	if sessionRowIdx >= len(w.sessionIDs) {
		sessionRowIdx = randInt(rng, 0, len(w.sessionIDs)-1)
	}
	sessionID := w.sessionIDs[sessionRowIdx]
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
	log.Warningf(context.TODO(), "inserting into queries, sessionRowIdx %d", sessionRowIdx)
	var rows [][]interface{}
	rng := rand.New(rand.NewSource(int64(sessionRowIdx) * 64))
	if sessionRowIdx >= len(w.sessionIDs) {
		sessionRowIdx = randInt(rng, 0, len(w.sessionIDs)-1)
	}
	sessionID := w.sessionIDs[sessionRowIdx]
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
func randInt(rng *rand.Rand, min, max int) int {
	return rng.Intn(max-min+1) + min
}
