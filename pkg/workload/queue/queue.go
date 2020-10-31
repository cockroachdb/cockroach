// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package queue

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/spf13/pflag"
)

const (
	queueSchema = `(ts BIGINT NOT NULL, id BIGINT NOT NULL, PRIMARY KEY(ts, id))`
)

type queue struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags
	batchSize int
}

func init() {
	workload.Register(queueMeta)
}

var queueMeta = workload.Meta{
	Name: `queue`,
	Description: `A simple queue-like application load: inserts into a table in sequence ` +
		`(ordered by primary key), followed by the deletion of inserted rows starting from the ` +
		`beginning of the sequence.`,
	Version: `1.0.0`,
	New: func() workload.Generator {
		g := &queue{}
		g.flags.FlagSet = pflag.NewFlagSet(`queue`, pflag.ContinueOnError)
		g.connFlags = workload.NewConnFlags(&g.flags)
		g.flags.IntVar(&g.batchSize, `batch`, 1, `Number of blocks to insert in a single SQL statement`)
		return g
	},
}

// Meta implements the Generator interface.
func (*queue) Meta() workload.Meta { return queueMeta }

// Flags implements the Flagser interface.
func (w *queue) Flags() workload.Flags { return w.flags }

// Tables implements the Generator interface.
func (w *queue) Tables() []workload.Table {
	table := workload.Table{
		Name:   `queue`,
		Schema: queueSchema,
	}
	return []workload.Table{table}
}

// Ops implements the Opser interface.
func (w *queue) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(w, w.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	db.SetMaxOpenConns(w.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(w.connFlags.Concurrency + 1)

	// Generate queue insert statement.
	var buf bytes.Buffer
	buf.WriteString(`INSERT INTO queue (ts, id) VALUES`)
	for i := 0; i < w.batchSize; i++ {
		j := i * 2
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, ` ($%d, $%d)`, j+1, j+2)
	}
	insertStmt, err := db.Prepare(buf.String())
	if err != nil {
		return workload.QueryLoad{}, err
	}

	// Generate queue deletion statement. This is intentionally in a naive form
	// for testing purposes.
	deleteStmt, err := db.Prepare(`DELETE FROM queue WHERE ts < $1`)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	seqFunc := makeSequenceFunc()

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for i := 0; i < w.connFlags.Concurrency; i++ {
		op := queueOp{
			workerID:   i + 1,
			config:     w,
			hists:      reg.GetHandle(),
			db:         db,
			insertStmt: insertStmt,
			deleteStmt: deleteStmt,
			getSeq:     seqFunc,
		}
		ql.WorkerFns = append(ql.WorkerFns, op.run)
	}
	return ql, nil
}

// queueOp represents a single concurrent "worker" generating the workload. Each
// queueOp worker both inserts into the queue table *and* consumes (deletes)
// entries from the beginning of the queue.
type queueOp struct {
	workerID   int
	config     *queue
	hists      *histogram.Histograms
	db         *gosql.DB
	insertStmt *gosql.Stmt
	deleteStmt *gosql.Stmt
	getSeq     func() int
}

func (o *queueOp) run(ctx context.Context) error {
	count := o.getSeq()
	start := count * o.config.batchSize
	end := start + o.config.batchSize

	// Write batch.
	params := make([]interface{}, 2*o.config.batchSize)
	for i := 0; i < o.config.batchSize; i++ {
		paramOffset := i * 2
		params[paramOffset+0] = start + i
		params[paramOffset+1] = o.workerID
	}
	startTime := timeutil.Now()
	_, err := o.insertStmt.Exec(params...)
	if err != nil {
		return err
	}
	elapsed := timeutil.Since(startTime)
	o.hists.Get("write").Record(elapsed)

	// Delete batch which was just written.
	startTime = timeutil.Now()
	_, err = o.deleteStmt.Exec(end)
	elapsed = timeutil.Since(startTime)
	o.hists.Get(`delete`).Record(elapsed)
	return err
}

func makeSequenceFunc() func() int {
	i := int64(0)
	return func() int {
		return int(atomic.AddInt64(&i, 1))
	}
}
