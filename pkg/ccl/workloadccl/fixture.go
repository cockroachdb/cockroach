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
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
	"google.golang.org/api/iterator"
)

const (
	fixtureGCSURIScheme = `gs`
)

func init() {
	workload.ImportDataLoader = ImportDataLoader{}
}

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

	// BillingProject if non-empty, is the Google Cloud project to bill for all
	// storage requests. This is required to be set if using a "requestor pays"
	// bucket.
	BillingProject string

	// If TableStats is true, CREATE STATISTICS is called on all tables before
	// creating the fixture.
	TableStats bool
}

func (s FixtureConfig) objectPathToURI(folder string) string {
	u := &url.URL{
		Scheme: fixtureGCSURIScheme,
		Host:   s.GCSBucket,
		Path:   folder,
	}
	q := url.Values{}
	if s.BillingProject != `` {
		q.Add("GOOGLE_BILLING_PROJECT", s.BillingProject)
	}
	q.Add("AUTH", "implicit")
	u.RawQuery = q.Encode()
	return u.String()
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

// FixtureURL returns the URL for pre-computed Generator data stored on GCS.
func FixtureURL(config FixtureConfig, gen workload.Generator) string {
	return config.objectPathToURI(generatorToGCSFolder(config, gen))
}

// GetFixture returns a handle for pre-computed Generator data stored on GCS. It
// is expected that the generator will have had Configure called on it.
func GetFixture(
	ctx context.Context, gcs *storage.Client, config FixtureConfig, gen workload.Generator,
) (Fixture, error) {
	var fixture Fixture
	var err error
	var notFound bool
	for r := retry.StartWithCtx(ctx, retry.Options{MaxRetries: 10}); r.Next(); {
		err = func() error {
			b := gcs.Bucket(config.GCSBucket)
			if config.BillingProject != `` {
				b = b.UserProject(config.BillingProject)
			}

			fixtureFolder := generatorToGCSFolder(config, gen)
			_, err := b.Objects(ctx, &storage.Query{Prefix: fixtureFolder, Delimiter: `/`}).Next()
			if errors.Is(err, iterator.Done) {
				notFound = true
				return errors.Errorf(`fixture not found: %s`, fixtureFolder)
			} else if err != nil {
				return err
			}

			fixture = Fixture{Config: config, Generator: gen}
			for _, table := range gen.Tables() {
				tableFolder := filepath.Join(fixtureFolder, table.Name)
				_, err := b.Objects(ctx, &storage.Query{Prefix: tableFolder, Delimiter: `/`}).Next()
				if errors.Is(err, iterator.Done) {
					return errors.Errorf(`fixture table not found: %s`, tableFolder)
				} else if err != nil {
					return err
				}
				fixture.Tables = append(fixture.Tables, FixtureTable{
					TableName: table.Name,
					BackupURI: config.objectPathToURI(tableFolder),
				})
			}
			return nil
		}()
		if err == nil || notFound {
			break
		}
	}
	return fixture, err
}

func csvServerPaths(
	csvServerURL string, gen workload.Generator, table workload.Table, numNodes int,
) []string {
	if table.InitialRows.FillBatch == nil {
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
	for rowIdx := 0; ; {
		chunkRowStart, chunkRowEnd := rowIdx, rowIdx+rowStep
		if chunkRowEnd > table.InitialRows.NumBatches {
			chunkRowEnd = table.InitialRows.NumBatches
		}

		params := url.Values{
			`row-start`: []string{strconv.Itoa(chunkRowStart)},
			`row-end`:   []string{strconv.Itoa(chunkRowEnd)},
			`version`:   []string{gen.Meta().Version},
		}
		if f, ok := gen.(workload.Flagser); ok {
			flags := f.Flags()
			flags.VisitAll(func(f *pflag.Flag) {
				if flags.Meta[f.Name].RuntimeOnly {
					return
				}
				params[f.Name] = append(params[f.Name], f.Value.String())
			})
		}
		path := fmt.Sprintf(`%s/csv/%s/%s?%s`,
			csvServerURL, gen.Meta().Name, table.Name, params.Encode())
		paths = append(paths, path)

		rowIdx = chunkRowEnd
		if rowIdx >= table.InitialRows.NumBatches {
			break
		}
	}
	return paths
}

// Specify an explicit empty prefix for crdb_internal to avoid an error if
// the database we're connected to does not exist.
const numNodesQuery = `SELECT count(node_id) FROM "".crdb_internal.gossip_liveness`

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
	filesPerNode int,
) (Fixture, error) {
	for _, t := range gen.Tables() {
		if t.InitialRows.FillBatch == nil {
			return Fixture{}, errors.Errorf(
				`make fixture is not supported for workload %s`, gen.Meta().Name,
			)
		}
	}

	fixtureFolder := generatorToGCSFolder(config, gen)
	if _, err := GetFixture(ctx, gcs, config, gen); err == nil {
		return Fixture{}, errors.Errorf(
			`fixture %s already exists`, config.objectPathToURI(fixtureFolder))
	}

	dbName := gen.Meta().Name
	if _, err := sqlDB.Exec(`CREATE DATABASE IF NOT EXISTS ` + dbName); err != nil {
		return Fixture{}, err
	}
	l := ImportDataLoader{
		FilesPerNode: filesPerNode,
	}
	// NB: Intentionally don't use workloadsql.Setup because it runs the PostLoad
	// hooks (adding foreign keys, etc), but historically the BACKUPs created by
	// `fixtures make` didn't have them. Instead they're added by `fixtures load`.
	// Ideally, the PostLoad hooks would be idempotent and we could include them
	// here (but still run them on load for old fixtures without them), but that
	// yak will remain unshaved.
	if _, err := l.InitialDataLoad(ctx, sqlDB, gen); err != nil {
		return Fixture{}, err
	}

	if config.TableStats {
		// Clean up any existing statistics.
		_, err := sqlDB.Exec("DELETE FROM system.table_statistics WHERE true")
		if err != nil {
			return Fixture{}, errors.Wrapf(err, "while deleting table statistics")
		}
		g := ctxgroup.WithContext(ctx)
		for _, t := range gen.Tables() {
			t := t
			g.Go(func() error {
				log.Infof(ctx, "Creating table stats for %s", t.Name)
				_, err := sqlDB.Exec(fmt.Sprintf(
					`CREATE STATISTICS pre_backup FROM "%s"."%s"`, dbName, t.Name,
				))
				return err
			})
		}
		if err := g.Wait(); err != nil {
			return Fixture{}, err
		}
	}

	g := ctxgroup.WithContext(ctx)
	for _, t := range gen.Tables() {
		t := t
		g.Go(func() error {
			q := fmt.Sprintf(`BACKUP "%s"."%s" TO $1`, dbName, t.Name)
			output := config.objectPathToURI(filepath.Join(fixtureFolder, t.Name))
			log.Infof(ctx, "Backing %s up to %q...", t.Name, output)
			_, err := sqlDB.Exec(q, output)
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return Fixture{}, err
	}

	return GetFixture(ctx, gcs, config, gen)
}

// ImportDataLoader is an InitialDataLoader implementation that loads data with
// IMPORT. The zero-value gets some sane defaults for the tunable settings.
type ImportDataLoader struct {
	FilesPerNode int
	InjectStats  bool
	CSVServer    string
}

// InitialDataLoad implements the InitialDataLoader interface.
func (l ImportDataLoader) InitialDataLoad(
	ctx context.Context, db *gosql.DB, gen workload.Generator,
) (int64, error) {
	if l.FilesPerNode == 0 {
		l.FilesPerNode = 1
	}

	log.Infof(ctx, "starting import of %d tables", len(gen.Tables()))
	start := timeutil.Now()
	const useConnectionDB = ``
	bytes, err := ImportFixture(
		ctx, db, gen, useConnectionDB, l.FilesPerNode, l.InjectStats, l.CSVServer)
	if err != nil {
		return 0, errors.Wrap(err, `importing fixture`)
	}
	elapsed := timeutil.Since(start)
	log.Infof(ctx, "imported %s bytes in %d tables (took %s, %s)",
		humanizeutil.IBytes(bytes), len(gen.Tables()), elapsed, humanizeutil.DataRate(bytes, elapsed))

	return bytes, nil
}

// ImportFixture works like MakeFixture, but instead of stopping halfway or
// writing a backup to cloud storage, it finishes ingesting the data.
// It also includes the option to inject pre-calculated table statistics if
// injectStats is true.
func ImportFixture(
	ctx context.Context,
	sqlDB *gosql.DB,
	gen workload.Generator,
	dbName string,
	filesPerNode int,
	injectStats bool,
	csvServer string,
) (int64, error) {
	for _, t := range gen.Tables() {
		if t.InitialRows.FillBatch == nil {
			return 0, errors.Errorf(
				`import fixture is not supported for workload %s`, gen.Meta().Name,
			)
		}
	}

	var numNodes int
	if err := sqlDB.QueryRow(numNodesQuery).Scan(&numNodes); err != nil {
		return 0, err
	}

	var bytesAtomic int64
	g := ctxgroup.WithContext(ctx)
	tables := gen.Tables()
	if injectStats && tablesHaveStats(tables) {
		// Turn off automatic stats temporarily so we don't trigger stats creation
		// after the IMPORT. We will inject stats inside importFixtureTable.
		// TODO(rytaft): It would be better if the automatic statistics code would
		// just trigger a no-op if there are new stats available so we wouldn't
		// have to disable and re-enable automatic stats here.
		enableFn := disableAutoStats(ctx, sqlDB)
		defer enableFn()
	}

	pathPrefix := csvServer
	if pathPrefix == `` {
		pathPrefix = `workload://`
	}

	for _, t := range tables {
		table := t
		paths := csvServerPaths(pathPrefix, gen, table, numNodes*filesPerNode)
		g.GoCtx(func(ctx context.Context) error {
			tableBytes, err := importFixtureTable(
				ctx, sqlDB, dbName, table, paths, `` /* output */, injectStats)
			atomic.AddInt64(&bytesAtomic, tableBytes)
			return errors.Wrapf(err, `importing table %s`, table.Name)
		})
	}
	if err := g.Wait(); err != nil {
		return 0, err
	}
	return atomic.LoadInt64(&bytesAtomic), nil
}

func importFixtureTable(
	ctx context.Context,
	sqlDB *gosql.DB,
	dbName string,
	table workload.Table,
	paths []string,
	output string,
	injectStats bool,
) (int64, error) {
	start := timeutil.Now()
	var buf bytes.Buffer
	var params []interface{}
	qualifiedTableName := makeQualifiedTableName(dbName, &table)
	fmt.Fprintf(&buf, `IMPORT TABLE %s %s CSV DATA (`, qualifiedTableName, table.Schema)
	// Generate $1,...,$N-1, where N is the number of csv paths.
	for _, path := range paths {
		params = append(params, path)
		if len(params) != 1 {
			buf.WriteString(`,`)
		}
		fmt.Fprintf(&buf, `$%d`, len(params))
	}
	buf.WriteString(`) WITH nullif='NULL'`)
	if len(output) > 0 {
		params = append(params, output)
		fmt.Fprintf(&buf, `, transform=$%d`, len(params))
	}
	var rows, index, tableBytes int64
	var discard driver.Value
	res, err := sqlDB.Query(buf.String(), params...)
	if err != nil {
		return 0, err
	}
	defer res.Close()
	if !res.Next() {
		if err := res.Err(); err != nil {
			return 0, errors.Wrap(err, "unexpected error during import")
		}
		return 0, gosql.ErrNoRows
	}
	resCols, err := res.Columns()
	if err != nil {
		return 0, err
	}
	if len(resCols) == 7 {
		if err := res.Scan(
			&discard, &discard, &discard, &rows, &index, &discard, &tableBytes,
		); err != nil {
			return 0, err
		}
	} else {
		if err := res.Scan(
			&discard, &discard, &discard, &rows, &index, &tableBytes,
		); err != nil {
			return 0, err
		}
	}
	elapsed := timeutil.Since(start)
	log.Infof(ctx, `imported %s in %s table (%d rows, %d index entries, took %s, %s)`,
		humanizeutil.IBytes(tableBytes), table.Name, rows, index, elapsed,
		humanizeutil.DataRate(tableBytes, elapsed))

	// Inject pre-calculated stats.
	if injectStats && len(table.Stats) > 0 {
		if err := injectStatistics(qualifiedTableName, &table, sqlDB); err != nil {
			return 0, err
		}
	}

	return tableBytes, nil
}

// tablesHaveStats returns whether any of the provided tables have associated
// table statistics to inject.
func tablesHaveStats(tables []workload.Table) bool {
	for _, t := range tables {
		if len(t.Stats) > 0 {
			return true
		}
	}
	return false
}

// disableAutoStats disables automatic stats if they are enabled and returns
// a function to re-enable them later. If automatic stats are already disabled,
// disableAutoStats does nothing and returns an empty function.
func disableAutoStats(ctx context.Context, sqlDB *gosql.DB) func() {
	var autoStatsEnabled bool
	err := sqlDB.QueryRow(
		`SHOW CLUSTER SETTING sql.stats.automatic_collection.enabled`,
	).Scan(&autoStatsEnabled)
	if err != nil {
		log.Warningf(ctx, "error retrieving automatic stats cluster setting: %v", err)
		return func() {}
	}

	if autoStatsEnabled {
		_, err = sqlDB.Exec(
			`SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false`,
		)
		if err != nil {
			log.Warningf(ctx, "error disabling automatic stats: %v", err)
			return func() {}
		}
		return func() {
			_, err := sqlDB.Exec(
				`SET CLUSTER SETTING sql.stats.automatic_collection.enabled=true`,
			)
			if err != nil {
				log.Warningf(ctx, "error enabling automatic stats: %v", err)
			}
		}
	}

	return func() {}
}

// injectStatistics injects pre-calculated statistics for the given table.
func injectStatistics(qualifiedTableName string, table *workload.Table, sqlDB *gosql.DB) error {
	var encoded []byte
	encoded, err := json.Marshal(table.Stats)
	if err != nil {
		return err
	}
	if _, err := sqlDB.Exec(
		fmt.Sprintf(`ALTER TABLE %s INJECT STATISTICS '%s'`, qualifiedTableName, encoded),
	); err != nil {
		if strings.Contains(err.Error(), "syntax error") {
			// This syntax was added in v2.1, so ignore the syntax error
			// if run against versions earlier than this.
			return nil
		}
		return err
	}
	return nil
}

// makeQualifiedTableName constructs a qualified table name from the specified
// database name and table.
func makeQualifiedTableName(dbName string, table *workload.Table) string {
	if dbName == "" {
		return fmt.Sprintf(`"%s"`, table.Name)
	}
	return fmt.Sprintf(`"%s"."%s"`, dbName, table.Name)
}

// RestoreFixture loads a fixture into a CockroachDB cluster. An enterprise
// license is required to have been set in the cluster.
func RestoreFixture(
	ctx context.Context, sqlDB *gosql.DB, fixture Fixture, database string, injectStats bool,
) (int64, error) {
	var bytesAtomic int64
	g := ctxgroup.WithContext(ctx)
	genName := fixture.Generator.Meta().Name
	tables := fixture.Generator.Tables()
	if injectStats && tablesHaveStats(tables) {
		// Turn off automatic stats temporarily so we don't trigger stats creation
		// after the RESTORE.
		// TODO(rytaft): It would be better if the automatic statistics code would
		// just trigger a no-op if there are new stats available so we wouldn't
		// have to disable and re-enable automatic stats here.
		enableFn := disableAutoStats(ctx, sqlDB)
		defer enableFn()
	}
	for _, table := range fixture.Tables {
		table := table
		g.GoCtx(func(ctx context.Context) error {
			start := timeutil.Now()
			restoreStmt := fmt.Sprintf(`RESTORE %s.%s FROM $1 WITH into_db=$2`, genName, table.TableName)
			log.Infof(ctx, "Restoring from %s", table.BackupURI)
			var rows, index, tableBytes int64
			var discard interface{}
			res, err := sqlDB.Query(restoreStmt, table.BackupURI, database)
			if err != nil {
				return errors.Wrapf(err, "restore: %s", table.BackupURI)
			}
			defer res.Close()
			if !res.Next() {
				if err := res.Err(); err != nil {
					return errors.Wrap(err, "unexpected error during restore")
				}
				return gosql.ErrNoRows
			}
			resCols, err := res.Columns()
			if err != nil {
				return err
			}
			if len(resCols) == 7 {
				if err := res.Scan(
					&discard, &discard, &discard, &rows, &index, &discard, &tableBytes,
				); err != nil {
					return err
				}
			} else {
				if err := res.Scan(
					&discard, &discard, &discard, &rows, &index, &tableBytes,
				); err != nil {
					return err
				}
			}
			atomic.AddInt64(&bytesAtomic, tableBytes)
			elapsed := timeutil.Since(start)
			log.Infof(ctx, `loaded %s table %s in %s (%d rows, %d index entries, %s)`,
				humanizeutil.IBytes(tableBytes), table.TableName, elapsed, rows, index,
				humanizeutil.IBytes(int64(float64(tableBytes)/elapsed.Seconds())))
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return 0, err
	}
	if injectStats {
		for i := range tables {
			t := &tables[i]
			if len(t.Stats) > 0 {
				qualifiedTableName := makeQualifiedTableName(genName, t)
				if err := injectStatistics(qualifiedTableName, t, sqlDB); err != nil {
					return 0, err
				}
			}
		}
	}
	return atomic.LoadInt64(&bytesAtomic), nil
}

// ListFixtures returns the object paths to all fixtures stored in a FixtureConfig.
func ListFixtures(
	ctx context.Context, gcs *storage.Client, config FixtureConfig,
) ([]string, error) {
	b := gcs.Bucket(config.GCSBucket)
	if config.BillingProject != `` {
		b = b.UserProject(config.BillingProject)
	}

	var fixtures []string
	gensPrefix := config.GCSPrefix + `/`
	for genIter := b.Objects(ctx, &storage.Query{Prefix: gensPrefix, Delimiter: `/`}); ; {
		gen, err := genIter.Next()
		if errors.Is(err, iterator.Done) {
			break
		} else if err != nil {
			return nil, err
		}
		for genConfigIter := b.Objects(ctx, &storage.Query{Prefix: gen.Prefix, Delimiter: `/`}); ; {
			genConfig, err := genConfigIter.Next()
			if errors.Is(err, iterator.Done) {
				break
			} else if err != nil {
				return nil, err
			}
			fixtures = append(fixtures, genConfig.Prefix)
		}
	}
	return fixtures, nil
}
