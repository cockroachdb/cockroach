// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestRejectedFilename(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tests := []struct {
		name     string
		fname    string
		rejected string
	}{
		{
			name:     "http",
			fname:    "http://127.0.0.1",
			rejected: "http://127.0.0.1/.rejected",
		},
		{
			name:     "nodelocal",
			fname:    "nodelocal://0/file.csv",
			rejected: "nodelocal://0/file.csv.rejected",
		},
	}
	for _, tc := range tests {
		rej, err := rejectedFilename(tc.fname)
		if err != nil {
			t.Fatal(err)
		}
		if tc.rejected != rej {
			t.Errorf("expected:\n%v\ngot:\n%v\n", tc.rejected, rej)
		}
	}
}

// nilDataProducer produces infinite stream of nulls.
// It implements importRowProducer.
type nilDataProducer struct{}

func (p *nilDataProducer) Scan() bool {
	return true
}

func (p *nilDataProducer) Err() error {
	return nil
}

func (p *nilDataProducer) Skip() error {
	return nil
}

func (p *nilDataProducer) Row() (interface{}, error) {
	return nil, nil
}

func (p *nilDataProducer) Progress() float32 {
	return 0.0
}

var _ importRowProducer = &nilDataProducer{}

// errorReturningConsumer always returns an error.
// It implements importRowConsumer.
type errorReturningConsumer struct {
	err error
}

func (d *errorReturningConsumer) FillDatums(
	_ interface{}, _ int64, c *row.DatumRowConverter,
) error {
	return d.err
}

var _ importRowConsumer = &errorReturningConsumer{}

// nilDataConsumer consumes and emits infinite stream of null.
// it implements importRowConsumer.
type nilDataConsumer struct{}

func (n *nilDataConsumer) FillDatums(_ interface{}, _ int64, c *row.DatumRowConverter) error {
	c.Datums[0] = tree.DNull
	return nil
}

var _ importRowConsumer = &nilDataConsumer{}

func TestParallelImportProducerHandlesConsumerErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Dummy descriptor for import
	descr := descpb.TableDescriptor{
		Name: "test",
		Columns: []descpb.ColumnDescriptor{
			{Name: "column", ID: 1, Type: types.Int, Nullable: true},
		},
	}

	// Flush datum converter frequently
	defer row.TestingSetDatumRowConverterBatchSize(1)()

	// Create KV channel and arrange for it to be drained
	kvCh := make(chan row.KVBatch)
	defer close(kvCh)
	go func() {
		for range kvCh {
		}
	}()

	// Prepare import context, which flushes to kvCh frequently.
	importCtx := &parallelImportContext{
		numWorkers: 1,
		batchSize:  2,
		evalCtx:    testEvalCtx,
		tableDesc:  tabledesc.NewBuilder(&descr).BuildImmutableTable(),
		kvCh:       kvCh,
	}

	consumer := &errorReturningConsumer{errors.New("consumer aborted")}

	require.Equal(t, consumer.err,
		runParallelImport(context.Background(), importCtx,
			&importFileContext{}, &nilDataProducer{}, consumer))
}

func TestParallelImportProducerHandlesCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Dummy descriptor for import
	descr := descpb.TableDescriptor{
		Name: "test",
		Columns: []descpb.ColumnDescriptor{
			{Name: "column", ID: 1, Type: types.Int, Nullable: true},
		},
	}

	// Flush datum converter frequently
	defer row.TestingSetDatumRowConverterBatchSize(1)()

	// Create KV channel and arrange for it to be drained
	kvCh := make(chan row.KVBatch)
	defer close(kvCh)
	go func() {
		for range kvCh {
		}
	}()

	// Prepare import context, which flushes to kvCh frequently.
	importCtx := &parallelImportContext{
		numWorkers: 1,
		batchSize:  2,
		evalCtx:    testEvalCtx,
		tableDesc:  tabledesc.NewBuilder(&descr).BuildImmutableTable(),
		kvCh:       kvCh,
	}

	// Run a hundred imports, which will timeout shortly after they start.
	require.NoError(t, ctxgroup.GroupWorkers(context.Background(), 100,
		func(_ context.Context, _ int) error {
			timeout := time.Millisecond * time.Duration(250+rand.Intn(250))
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer func(f func()) {
				f()
			}(cancel)
			require.Equal(t, context.DeadlineExceeded,
				runParallelImport(ctx, importCtx,
					&importFileContext{}, &nilDataProducer{}, &nilDataConsumer{}))
			return nil
		}))
}
