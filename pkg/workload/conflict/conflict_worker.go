// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package conflict

import (
	"context"
	gosql "database/sql"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	workloadrand "github.com/cockroachdb/cockroach/pkg/workload/rand"
	"golang.org/x/sync/errgroup"
)

type conflictWorker struct {
	table   workloadrand.Table
	rng     *rand.Rand
	writers []*writer
	nullPct int
}

func newConflictWorker(
	ctx context.Context, connections []*gosql.DB, tableName string,
) (*conflictWorker, error) {
	table, err := workloadrand.LoadTable(connections[0], tableName)
	if err != nil {
		return nil, err
	}

	writers := make([]*writer, len(connections))
	for i := range writers {
		writers[i], err = newWriter(ctx, connections[i], table)
		if err != nil {
			return nil, err
		}
	}

	return &conflictWorker{
		table:   table,
		writers: writers,
		rng:     rand.New(rand.NewSource(timeutil.Now().UnixNano())),
		nullPct: 10,
	}, nil
}

func (w *conflictWorker) RunOp(ctx context.Context) (err error) {
	log.Dev.Infof(ctx, "running conflict worker")
	defer func() {
		log.Dev.Infof(ctx, "conflict worker done: %v", err)
	}()

	row, err := w.table.RandomRow(w.rng, w.nullPct)
	if err != nil {
		return err
	}

	group, ctx := errgroup.WithContext(ctx)
	for _, writer := range w.writers {
		// Create a new RNG source because RNG is not thread safe.
		rng := rand.New(rand.NewSource(w.rng.Int63()))
		group.Go(func() error {
			return w.runOpOnCluster(ctx, writer, row, rng)
		})
	}

	return group.Wait()
}

func (w *conflictWorker) runOpOnCluster(
	ctx context.Context, writer *writer, row []any, rng *rand.Rand,
) error {
	var err error
	// run between 0, 1, or 2 mutations on each cluster. This way we get coverage of:
	// 1. One direction replication.
	// 2. Same row inserted into both clusters.
	// 3. Conflicting inserts or updates.
	for range w.rng.Int31n(3) {
		if rand.Float64() < 0.8 {
			if rand.Float64() < 0.5 {
				row, err = w.table.MutateRow(rng, w.nullPct, row)
				if err != nil {
					return err
				}
			}
			if err := writer.upsertRow(ctx, row); err != nil {
				return err
			}
		} else {
			if err := writer.deleteRow(ctx, row); err != nil {
				return err
			}
		}
	}
	return nil
}
