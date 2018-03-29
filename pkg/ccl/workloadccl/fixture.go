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
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
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

// FixtureConfig describes a storage place for fixtures.
type FixtureConfig struct {
	// GCSBucket is a Google Cloud Storage bucket.
	GCSBucket string

	// GCSPrefix is a prefix to prepend to each Google Cloud Storage object
	// path.
	GCSPrefix string

	// CSVServerURL is a url to a `./workload csv-server` to use as a source of
	// CSV data. The url is anything accepted by our backup/restore. Notably, if
	// you run a csv-server next to each CockroachDB node,
	// `http://localhost:<port>` will work.
	CSVServerURL string
}

func (s FixtureConfig) objectPathToURI(folder string) string {
	return (&url.URL{
		Scheme: fixtureGCSURIScheme,
		Host:   s.GCSBucket,
		Path:   folder,
	}).String()
}

// Fixture describes pre-computed data for a Generator, allowing quick
// initialization of large clusters.
type Fixture struct {
	Config    FixtureConfig
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
	flags := f.Flags()
	flags.VisitAll(func(f *pflag.Flag) {
		if flags.Meta != nil && flags.Meta[f.Name].RuntimeOnly {
			return
		}
		if buf.Len() > 0 {
			buf.WriteString(`,`)
		}
		fmt.Fprintf(&buf, `%s=%s`, url.PathEscape(f.Name), url.PathEscape(f.Value.String()))
	})
	return buf.String()
}

func generatorToGCSFolder(config FixtureConfig, gen workload.Generator) string {
	meta := gen.Meta()
	return filepath.Join(
		config.GCSPrefix,
		meta.Name,
		fmt.Sprintf(`version=%s,%s`, meta.Version, serializeOptions(gen)),
	)
}

// GetFixture returns a handle for pre-computed Generator data stored on GCS. It
// is expected that the generator will have had Configure called on it.
func GetFixture(
	ctx context.Context, gcs *storage.Client, config FixtureConfig, gen workload.Generator,
) (Fixture, error) {
	b := gcs.Bucket(config.GCSBucket)

	fixtureFolder := generatorToGCSFolder(config, gen)
	_, err := b.Objects(ctx, &storage.Query{Prefix: fixtureFolder, Delimiter: `/`}).Next()
	if err == iterator.Done {
		return Fixture{}, errors.Errorf(`fixture not found: %s`, fixtureFolder)
	} else if err != nil {
		return Fixture{}, err
	}

	fixture := Fixture{Config: config, Generator: gen}
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
			BackupURI: config.objectPathToURI(tableFolder),
		})
	}
	return fixture, nil
}

type groupCSVWriter struct {
	sem            chan struct{}
	gcs            *storage.Client
	config         FixtureConfig
	folder         string
	chunkSizeBytes int64

	start           time.Time
	csvBytesWritten int64 // Only access via atomic
}

// defaultRetryOptions was copied from base because base was bringing in a lot
// of other deps and this shaves ~0.5s off the ~2s pkg/cmd/workload build time.
func defaultRetryOptions() retry.Options {
	return retry.Options{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		Multiplier:     2,
	}
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
		err := retry.WithMaxAttempts(ctx, defaultRetryOptions(), maxAttempts, func() error {
			w := c.gcs.Bucket(c.config.GCSBucket).Object(path).NewWriter(ctx)
			var err error
			rowIdx, err = workload.WriteCSVRows(ctx, w, table, rowStart, rowEnd, c.chunkSizeBytes)
			closeErr := w.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}

			pathsCh <- c.config.objectPathToURI(path)
			newBytesWritten := atomic.AddInt64(&c.csvBytesWritten, w.Attrs().Size)
			d := timeutil.Since(c.start)
			throughput := float64(newBytesWritten) / (d.Seconds() * float64(1<<20) /* 1MiB */)
			log.Infof(ctx, `wrote csv %s [%d,%d] of %d row batches (%.2f%% (%s) in %s: %.1f MB/s)`,
				table.Name, rowStart, rowIdx, table.InitialRows.NumBatches,
				float64(100*table.InitialRows.NumBatches)/float64(rowIdx),
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

func csvServerPaths(
	csvServerURL string, gen workload.Generator, table workload.Table, numNodes int,
) []string {
	if table.InitialRows.Batch == nil {
		// Some workloads don't support initial table data.
		return nil
	}

	// More files means more granularity in the progress tracking, but more
	// files also means larger jobs table entries, so this is a balance. The
	// IMPORT code round-robins the files in an import per node, so it's best to
	// have some integer multiple of the number of nodes in the cluster, which
	// will guarantee that the work is balanced across the cluster. In practice,
	// even as few as 100 files caused jobs badness when creating tpcc fixtures,
	// so our "integer multiple" is picked to be 1 to minimize this effect. Too
	// bad about the progress tracking granularity.
	numFiles := numNodes
	rowStep := table.InitialRows.NumBatches / numFiles
	if rowStep == 0 {
		rowStep = 1
	}

	var paths []string
	for rowIdx := 0; rowIdx < table.InitialRows.NumBatches; {
		chunkRowStart, chunkRowEnd := rowIdx, rowIdx+rowStep
		if chunkRowEnd > table.InitialRows.NumBatches {
			chunkRowEnd = table.InitialRows.NumBatches
		}

		params := url.Values{
			`row-start`: []string{strconv.Itoa(chunkRowStart)},
			`row-end`:   []string{strconv.Itoa(chunkRowEnd)},
		}
		if f, ok := gen.(workload.Flagser); ok {
			f.Flags().VisitAll(func(f *pflag.Flag) {
				params[f.Name] = append(params[f.Name], f.Value.String())
			})
		}
		path := fmt.Sprintf(`%s/csv/%s/%s?%s`,
			csvServerURL, gen.Meta().Name, table.Name, params.Encode())
		paths = append(paths, path)

		rowIdx = chunkRowEnd
	}
	return paths
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
	config FixtureConfig,
	gen workload.Generator,
) (Fixture, error) {
	const writeCSVChunkSize = 64 * 1 << 20 // 64 MB

	fixtureFolder := generatorToGCSFolder(config, gen)
	if _, err := GetFixture(ctx, gcs, config, gen); err == nil {
		return Fixture{}, errors.Errorf(
			`fixture %s already exists`, config.objectPathToURI(fixtureFolder))
	}

	writeCSVConcurrency := runtime.NumCPU()
	c := &groupCSVWriter{
		sem:            make(chan struct{}, writeCSVConcurrency),
		gcs:            gcs,
		config:         config,
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
			if len(config.CSVServerURL) == 0 {
				startRow, endRow := 0, table.InitialRows.NumBatches
				return c.groupWriteCSVs(gCtx, tableCSVPathsCh, table, startRow, endRow)
			}
			// Specify an explicit empty prefix for crdb_internal to avoid an error if
			// the database we're connected to does not exist.
			const numNodesQuery = `SELECT COUNT(node_id) FROM "".crdb_internal.gossip_liveness`
			var numNodes int
			if err := sqlDB.QueryRow(numNodesQuery).Scan(&numNodes); err != nil {
				return err
			}
			paths := csvServerPaths(config.CSVServerURL, gen, table, numNodes)
			for _, path := range paths {
				tableCSVPathsCh <- path
			}
			return nil
		})
		g.Go(func() error {
			params := []interface{}{
				config.objectPathToURI(filepath.Join(fixtureFolder, table.Name)),
			}
			// NB: it's fine to loop over this channel without selecting
			// ctx.Done because a context cancel will cause the above goroutine
			// to finish and close tableCSVPathsCh.
			for tableCSVPath := range tableCSVPathsCh {
				params = append(params, tableCSVPath)
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
			if _, err := sqlDB.Exec(buf.String(), params...); err != nil {
				return errors.Wrapf(err, `creating backup for table %s`, table.Name)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return Fixture{}, err
	}

	// TODO(dan): Clean up the CSVs.
	return GetFixture(ctx, gcs, config, gen)
}

// RestoreFixture loads a fixture into a CockroachDB cluster. An enterprise
// license is required to have been set in the cluster.
func RestoreFixture(ctx context.Context, sqlDB *gosql.DB, fixture Fixture, database string) error {
	g, gCtx := errgroup.WithContext(ctx)
	for _, table := range fixture.Tables {
		table := table
		g.Go(func() error {
			// The IMPORT ... CSV DATA command generates a backup with the table in
			// database `csv`.
			start := timeutil.Now()
			importStmt := fmt.Sprintf(`RESTORE csv.%s FROM $1 WITH into_db=$2`, table.TableName)
			var rows, index, bytes int64
			var discard interface{}
			if err := sqlDB.QueryRow(importStmt, table.BackupURI, database).Scan(
				&discard, &discard, &discard, &rows, &index, &discard, &bytes,
			); err != nil {
				return err
			}
			log.Infof(gCtx, `loaded %s (%s, %d rows, %d index entries, %v)`,
				table.TableName, timeutil.Since(start).Round(time.Second), rows, index, humanizeutil.IBytes(bytes),
			)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	const splitConcurrency = 384 // TODO(dan): Don't hardcode this.
	for _, table := range fixture.Generator.Tables() {
		if err := workload.Split(ctx, sqlDB, table, splitConcurrency); err != nil {
			return errors.Wrapf(err, `splitting %s`, table.Name)
		}
	}
	if h, ok := fixture.Generator.(workload.Hookser); ok {
		if hooks := h.Hooks(); hooks.PostLoad != nil {
			if err := hooks.PostLoad(sqlDB); err != nil {
				return errors.Wrap(err, `PostLoad hook`)
			}
		}
	}
	return nil
}

// ListFixtures returns the object paths to all fixtures stored in a FixtureConfig.
func ListFixtures(
	ctx context.Context, gcs *storage.Client, config FixtureConfig,
) ([]string, error) {
	b := gcs.Bucket(config.GCSBucket)

	var fixtures []string
	gensPrefix := config.GCSPrefix + `/`
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
