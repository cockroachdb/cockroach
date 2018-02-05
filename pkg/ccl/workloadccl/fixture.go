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
	"net/url"
	"path/filepath"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
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
	// NB: VisitAll visits in a deterministic (alphabetical) order.
	var buf bytes.Buffer
	gen.Flags().VisitAll(func(f *pflag.Flag) {
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

// writeCSVs creates a file on GCS in the specified folder that contains the
// data for the given table. The GCS object path to the written file is
// returned.
func writeCSVs(
	ctx context.Context, gcs *storage.Client, table workload.Table, store FixtureStore, folder string,
) (string, error) {
	// TODO(dan): For large tables, break this up into multiple CSVs.
	csvPath := filepath.Join(folder, table.Name+`.csv`)
	const maxAttempts = 3
	err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
		w := gcs.Bucket(store.GCSBucket).Object(csvPath).NewWriter(ctx)
		csvW := csv.NewWriter(w)
		for rowIdx := 0; rowIdx < table.InitialRowCount; rowIdx++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
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
		return w.Close()
	})
	return csvPath, err
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
	fixtureFolder := generatorToGCSFolder(store, gen)

	// TODO(dan): Break up large tables into many CSV files and use the
	// `distributed` option of `IMPORT` to do the backup generation work in
	// parallel. Afterward, experiment with parallelizing the per-table work
	// (IMPORT is pretty good at using cluster resources, so this is not as
	// obvious of a win as it might seem).
	for _, table := range gen.Tables() {
		tableFolder := filepath.Join(fixtureFolder, table.Name)
		csvPath, err := writeCSVs(ctx, gcs, table, store, tableFolder)
		if err != nil {
			return Fixture{}, err
		}
		backupURI := store.objectPathToURI(tableFolder)
		csvURI := store.objectPathToURI(csvPath)

		if _, err := sqlDB.ExecContext(
			ctx, `SET CLUSTER SETTING experimental.importcsv.enabled = true`,
		); err != nil {
			return Fixture{}, err
		}
		importStmt := fmt.Sprintf(
			`IMPORT TABLE "%s" %s CSV DATA ($1) WITH transform=$2, nullif='NULL'`,
			table.Name, table.Schema,
		)
		if _, err := sqlDB.ExecContext(ctx, importStmt, csvURI, backupURI); err != nil {
			return Fixture{}, errors.Wrapf(err, `creating backup for table %s`, table.Name)
		}
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
