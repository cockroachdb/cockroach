package conflict

import (
	"context"
	"database/sql"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	ctx context.Context, connections []*sql.DB, tableName string,
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
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
		nullPct: 10,
	}, nil
}

func (w *conflictWorker) RunOp(ctx context.Context) (err error) {
	log.Infof(ctx, "running conflict worker")
	defer func() {
		log.Infof(ctx, "conflict worker done: %v", err)
	}()

	row, err := w.table.RandomRow(w.rng, w.nullPct)
	if err != nil {
		return err
	}

	group, ctx := errgroup.WithContext(ctx)
	for _, writer := range w.writers {
		group.Go(func() error {
			if rand.Float64() < 0.8 {
				mutatedRow, err := w.table.MutateRow(w.rng, w.nullPct, row)
				if err != nil {
					return err
				}
				if err := writer.upsertRow(ctx, mutatedRow); err != nil {
					return err
				}
			} else {
				if err := writer.deleteRow(ctx, row); err != nil {
					return err
				}
			}
			return nil
		})
	}

	return group.Wait()
}
