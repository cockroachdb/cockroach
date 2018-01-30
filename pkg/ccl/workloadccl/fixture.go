// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package workloadccl

import (
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"net/url"
	"path"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

const (
	fixtureGCSURIScheme = `gs`
)

// FixtureStore describes a storage place for fixtures.
type FixtureStore struct {
	// GCSBucket is a Google Cloud Storage bucket.
	GCSBucket string

	// GCSPrefix is a prefix to prepend to each Google Cloud Storage object
	// path.
	GCSPrefix string
}

func (s FixtureStore) objectPathToURI(folder string) string {
	return (&url.URL{
		Scheme: fixtureGCSURIScheme,
		Host:   s.GCSBucket,
		Path:   folder,
	}).String()
}

// Fixture describes pre-computed data for a Generator, allowing quick
// initialization of large clusters.
type Fixture struct {
	Store     FixtureStore
	Generator workload.Generator
	Tables    []FixtureTable
}

// FixtureTable describes pre-computed data for a single table in a Generator,
// allowing quick initializaiton of large clusters.
type FixtureTable struct {
	TableName string
	BackupURI string
}

// serializeOptions deterministically represents the configuration of a
// Generator as a string.
func serializeOptions(gen workload.Generator) string {
	f, ok := gen.(workload.Flagser)
	if !ok {
		return ``
	}
	// NB: VisitAll visits in a deterministic (alphabetical) order.
	var buf bytes.Buffer
	f.Flags().VisitAll(func(f *pflag.Flag) {
		if buf.Len() > 0 {
			buf.WriteString(`,`)
		}
		fmt.Fprintf(&buf, `%s=%s`, url.PathEscape(f.Name), url.PathEscape(f.Value.String()))
	})
	return buf.String()
}

func generatorToGCSFolder(store FixtureStore, gen workload.Generator) string {
	meta := gen.Meta()
	return filepath.Join(
		store.GCSPrefix,
		meta.Name,
		fmt.Sprintf(`version=%s,%s`, meta.Version, serializeOptions(gen)),
	)
}

// GetFixture returns a handle for pre-computed Generator data stored on GCS. It
// is expected that the generator will have had Configure called on it.
func GetFixture(
	ctx context.Context, gcs *storage.Client, store FixtureStore, gen workload.Generator,
) (Fixture, error) {
	b := gcs.Bucket(store.GCSBucket)

	fixtureFolder := generatorToGCSFolder(store, gen)
	_, err := b.Objects(ctx, &storage.Query{Prefix: fixtureFolder, Delimiter: `/`}).Next()
	if err == iterator.Done {
		return Fixture{}, errors.Errorf(`fixture not found: %s`, fixtureFolder)
	} else if err != nil {
		return Fixture{}, err
	}

	fixture := Fixture{Store: store, Generator: gen}
	for _, table := range gen.Tables() {
		tableFolder := filepath.Join(fixtureFolder, table.Name)
		_, err := b.Objects(ctx, &storage.Query{Prefix: tableFolder, Delimiter: `/`}).Next()
		if err == iterator.Done {
			return Fixture{}, errors.Errorf(`fixture table not found: %s`, tableFolder)
		} else if err != nil {
			return Fixture{}, err
		}
		fixture.Tables = append(fixture.Tables, FixtureTable{
			TableName: table.Name,
			BackupURI: store.objectPathToURI(tableFolder),
		})
	}
	return fixture, nil
}

type groupCSVWriter struct {
	sem            chan struct{}
	gcs            *storage.Client
	store          FixtureStore
	folder         string
	chunkSizeBytes int64

	start           time.Time
	csvBytesWritten int64 // Only access via atomic
}

// groupWriteCSVs creates files on GCS in the specified folder that contain the
// data for the given table and rows.
//
// Files are chunked into ~c.chunkSizeBytes or smaller. Concurrency is limited
// by c.sem. The GCS object paths to the written files are returned on
// c.pathsCh.
func (c *groupCSVWriter) groupWriteCSVs(
	ctx context.Context, pathsCh chan<- string, table workload.Table, rowStart, rowEnd int,
) error {
	if rowStart == rowEnd {
		return nil
	}

	// For each table, first write out a chunk of ~c.chunkSizeBytes. If the
	// table fits in one chunk, we're done, otherwise this gives an estimate for
	// how many rows are needed.
	var rowIdx int
	if err := func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case c.sem <- struct{}{}:
		}
		defer func() { <-c.sem }()

		path := path.Join(c.folder, table.Name, fmt.Sprintf(`%09d.csv`, rowStart))
		const maxAttempts = 3
		err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
			w := c.gcs.Bucket(c.store.GCSBucket).Object(path).NewWriter(ctx)
			bytesWrittenW := &bytesWrittenWriter{w: w}
			csvW := csv.NewWriter(bytesWrittenW)
			for rowIdx = rowStart; rowIdx < rowEnd; rowIdx++ {
				if c.chunkSizeBytes > 0 && bytesWrittenW.written > c.chunkSizeBytes {
					break
				}
				row := table.InitialRowFn(rowIdx)
				rowStrings := make([]string, len(row))
				for i, datum := range row {
					if datum == nil {
						rowStrings[i] = `NULL`
					} else {
						rowStrings[i] = fmt.Sprintf(`%v`, datum)
					}
				}
				if err := csvW.Write(rowStrings); err != nil {
					return err
				}
			}
			csvW.Flush()
			if err := csvW.Error(); err != nil {
				return err
			}
			if err := w.Close(); err != nil {
				return err
			}

			pathsCh <- path
			newBytesWritten := atomic.AddInt64(&c.csvBytesWritten, bytesWrittenW.written)
			d := timeutil.Since(c.start)
			throughput := float64(newBytesWritten) / (d.Seconds() * float64(1<<20) /* 1MiB */)
			log.Infof(ctx, `wrote csv %s [%d,%d] of %d rows (total %s in %s: %.1f MB/s)`,
				table.Name, rowStart, rowIdx, table.InitialRowCount,
				humanizeutil.IBytes(newBytesWritten), d, throughput)

			return nil
		})
		return err
	}(); err != nil {
		return err
	}
	if rowIdx >= rowEnd {
		return nil
	}

	// If rowIdx < rowEnd, then the rows didn't all fit in one chunk. Use the
	// number of rows that did fit to estimate how many chunks are needed to
	// finish the table. Then break up the remaining rows into that many chunks,
	// running this whole process recursively in case the distribution of row
	// size is not uniform. Something like `(rowIdx - rowStart) * fudge` would
	// be simpler, but this will make the chunks a more even size.
	var rowStep int
	{
		const fudge = 0.9
		additionalChunks := int(float64(rowEnd-rowIdx) / (float64(rowIdx-rowStart) * fudge))
		if additionalChunks <= 0 {
			additionalChunks = 1
		}
		rowStep = (rowEnd - rowIdx) / additionalChunks
		if rowStep <= 0 {
			rowStep = 1
		}
	}

	g, gCtx := errgroup.WithContext(ctx)
	for rowIdx < rowEnd {
		chunkRowStart, chunkRowEnd := rowIdx, rowIdx+rowStep
		if chunkRowEnd > rowEnd {
			chunkRowEnd = rowEnd
		}
		g.Go(func() error {
			return c.groupWriteCSVs(gCtx, pathsCh, table, chunkRowStart, chunkRowEnd)
		})
		rowIdx = chunkRowEnd
	}
	return g.Wait()
}

// MakeFixture regenerates a fixture, storing it to GCS. It is expected that the
// generator will have had Configure called on it.
//
// There's some ideal world in which we can generate backups (and thus
// fixtures) directly from a Generator, but for now, we use `IMPORT ... CSV
// DATA`. First a CSV file with the table data is written to GCS. `IMPORT
// ... CSV DATA` works by turning a set of CSV files for a single table into a
// backup file, then restoring that file into a cluster. The `transform` option
// gives us only the first half (which is all we want for fixture generation).
func MakeFixture(
	ctx context.Context,
	sqlDB *gosql.DB,
	gcs *storage.Client,
	store FixtureStore,
	gen workload.Generator,
) (Fixture, error) {
	const writeCSVChunkSize = 64 * 1 << 20 // 64 MB

	fixtureFolder := generatorToGCSFolder(store, gen)
	if _, err := sqlDB.Exec(
		`SET CLUSTER SETTING experimental.importcsv.enabled = true`,
	); err != nil {
		return Fixture{}, err
	}

	writeCSVConcurrency := runtime.NumCPU()
	c := &groupCSVWriter{
		sem:            make(chan struct{}, writeCSVConcurrency),
		gcs:            gcs,
		store:          store,
		folder:         fixtureFolder,
		chunkSizeBytes: writeCSVChunkSize,
		start:          timeutil.Now(),
	}

	g, gCtx := errgroup.WithContext(ctx)
	for _, t := range gen.Tables() {
		table := t
		tableCSVPathsCh := make(chan string)

		g.Go(func() error {
			defer close(tableCSVPathsCh)
			startRow, endRow := 0, table.InitialRowCount
			return c.groupWriteCSVs(gCtx, tableCSVPathsCh, table, startRow, endRow)
		})
		g.Go(func() error {
			params := []interface{}{
				store.objectPathToURI(filepath.Join(fixtureFolder, table.Name)),
			}
			// NB: it's fine to loop over this channel without selecting
			// ctx.Done because a context cancel will cause the above goroutine
			// to finish and close tableCSVPathsCh.
			for tableCSVPath := range tableCSVPathsCh {
				params = append(params, store.objectPathToURI(tableCSVPath))
			}
			select {
			case <-gCtx.Done():
				return gCtx.Err()
			default:
			}

			var buf bytes.Buffer
			fmt.Fprintf(&buf, `IMPORT TABLE "%s" %s CSV DATA (`, table.Name, table.Schema)
			// $1 is used for the backup path. Generate $2,...,$N, where N is
			// the number of params (including the backup path) for use with the
			// csv paths.
			for i := 2; i <= len(params); i++ {
				if i != 2 {
					buf.WriteString(`,`)
				}
				fmt.Fprintf(&buf, `$%d`, i)
			}
			buf.WriteString(`) WITH transform=$1, nullif='NULL'`)
			if _, err := sqlDB.ExecContext(gCtx, buf.String(), params...); err != nil {
				return errors.Wrapf(err, `creating backup for table %s`, table.Name)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return Fixture{}, err
	}

	// TODO(dan): Clean up the CSVs.
	return GetFixture(ctx, gcs, store, gen)
}

// RestoreFixture loads a fixture into a CockroachDB cluster. An enterprise
// license is required to have been set in the cluster.
func RestoreFixture(ctx context.Context, sqlDB *gosql.DB, fixture Fixture, database string) error {
	for _, table := range fixture.Tables {
		// The IMPORT ... CSV DATA command generates a backup with the table in
		// database `csv`.
		importStmt := fmt.Sprintf(`RESTORE csv.%s FROM $1 WITH into_db=$2`, table.TableName)
		if _, err := sqlDB.ExecContext(ctx, importStmt, table.BackupURI, database); err != nil {
			return err
		}
	}
	const splitConcurrency = 384 // TODO(dan): Don't hardcode this.
	for _, table := range fixture.Generator.Tables() {
		if err := workload.Split(ctx, sqlDB, table, splitConcurrency); err != nil {
			return err
		}
	}
	return nil
}

// ListFixtures returns the object paths to all fixtures stored in a FixtureStore.
func ListFixtures(ctx context.Context, gcs *storage.Client, store FixtureStore) ([]string, error) {
	b := gcs.Bucket(store.GCSBucket)

	var fixtures []string
	gensPrefix := store.GCSPrefix + `/`
	for genIter := b.Objects(ctx, &storage.Query{Prefix: gensPrefix, Delimiter: `/`}); ; {
		gen, err := genIter.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}
		for genConfigIter := b.Objects(ctx, &storage.Query{Prefix: gen.Prefix, Delimiter: `/`}); ; {
			genConfig, err := genConfigIter.Next()
			if err == iterator.Done {
				break
			} else if err != nil {
				return nil, err
			}
			fixtures = append(fixtures, genConfig.Prefix)
		}
	}
	return fixtures, nil
}

type bytesWrittenWriter struct {
	w       io.Writer
	written int64
}

func (w *bytesWrittenWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.written += int64(n)
	return n, err
}
