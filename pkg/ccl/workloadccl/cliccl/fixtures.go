// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/ccl/workloadccl"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	workloadcli "github.com/cockroachdb/cockroach/pkg/workload/cli"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/api/option"
)

var useast1bFixtures = workloadccl.FixtureConfig{
	// TODO(dan): Keep fixtures in more than one region to better support
	// geo-distributed clusters.
	GCSBucket: `cockroach-fixtures`,
	GCSPrefix: `workload`,
}

func config() workloadccl.FixtureConfig {
	config := useast1bFixtures
	if len(*gcsBucketOverride) > 0 {
		config.GCSBucket = *gcsBucketOverride
	}
	if len(*gcsPrefixOverride) > 0 {
		config.GCSPrefix = *gcsPrefixOverride
	}
	if len(*gcsBillingProjectOverride) > 0 {
		config.BillingProject = *gcsBillingProjectOverride
	}
	config.CSVServerURL = *fixturesMakeImportCSVServerURL
	config.TableStats = *fixturesMakeTableStats
	return config
}

var fixturesCmd = workloadcli.SetCmdDefaults(&cobra.Command{
	Use:   `fixtures`,
	Short: `tools for quickly synthesizing and loading large datasets`,
})
var fixturesListCmd = workloadcli.SetCmdDefaults(&cobra.Command{
	Use:   `list`,
	Short: `list all fixtures stored on GCS`,
	Run:   workloadcli.HandleErrs(fixturesList),
})
var fixturesMakeCmd = workloadcli.SetCmdDefaults(&cobra.Command{
	Use:   `make`,
	Short: `IMPORT a fixture and then store a BACKUP of it on GCS`,
})
var fixturesLoadCmd = workloadcli.SetCmdDefaults(&cobra.Command{
	Use:   `load`,
	Short: `load a fixture into a running cluster. An enterprise license is required.`,
})
var fixturesImportCmd = workloadcli.SetCmdDefaults(&cobra.Command{
	Use:   `import`,
	Short: `import a fixture into a running cluster. An enterprise license is NOT required.`,
})
var fixturesURLCmd = workloadcli.SetCmdDefaults(&cobra.Command{
	Use:   `url`,
	Short: `generate the GCS URL for a fixture`,
})

var fixturesLoadImportShared = pflag.NewFlagSet(`load/import`, pflag.ContinueOnError)
var fixturesMakeImportShared = pflag.NewFlagSet(`load/import`, pflag.ContinueOnError)

var fixturesMakeImportCSVServerURL = fixturesMakeImportShared.String(
	`csv-server`, ``,
	`Skip saving CSVs to cloud storage, instead get them from a 'csv-server' running at this url`)

var fixturesMakeOnlyTable = fixturesMakeCmd.PersistentFlags().String(
	`only-tables`, ``,
	`Only load the tables with the given comma-separated names`)

var fixturesMakeFilesPerNode = fixturesMakeCmd.PersistentFlags().Int(
	`files-per-node`, 1,
	`number of file URLs to generate per node when using csv-server`)

var fixturesMakeTableStats = fixturesMakeCmd.PersistentFlags().Bool(
	`table-stats`, true,
	`generate full table statistics for all tables`)

var fixturesImportFilesPerNode = fixturesImportCmd.PersistentFlags().Int(
	`files-per-node`, 1,
	`number of file URLs to generate per node`)

var fixturesRunChecks = fixturesLoadImportShared.Bool(
	`checks`, true, `Run validity checks on the loaded fixture`)

var fixturesImportInjectStats = fixturesImportCmd.PersistentFlags().Bool(
	`inject-stats`, true, `Inject pre-calculated statistics if they are available`)

var gcsBucketOverride, gcsPrefixOverride, gcsBillingProjectOverride *string

func init() {
	gcsBucketOverride = fixturesCmd.PersistentFlags().String(`gcs-bucket-override`, ``, ``)
	gcsPrefixOverride = fixturesCmd.PersistentFlags().String(`gcs-prefix-override`, ``, ``)
	_ = fixturesCmd.PersistentFlags().MarkHidden(`gcs-bucket-override`)
	_ = fixturesCmd.PersistentFlags().MarkHidden(`gcs-prefix-override`)

	gcsBillingProjectOverride = fixturesCmd.PersistentFlags().String(
		`gcs-billing-project`, ``,
		`Google Cloud project to use for storage billing; `+
			`required to be non-empty if the bucket is requestor pays`)
}

const storageError = `failed to create google cloud client ` +
	`(You may need to setup the GCS application default credentials: ` +
	`'gcloud auth application-default login --project=cockroach-shared')`

// getStorage returns a GCS client using "application default" credentials. The
// caller is responsible for closing it.
func getStorage(ctx context.Context) (*storage.Client, error) {
	// TODO(dan): Right now, we don't need all the complexity of
	// cloud.ExternalStorage, but if we start supporting more than just GCS,
	// this should probably be switched to it.
	g, err := storage.NewClient(ctx, option.WithScopes(storage.ScopeReadWrite))
	return g, errors.Wrap(err, storageError)
}

func init() {
	workloadcli.AddSubCmd(func(userFacing bool) *cobra.Command {
		for _, meta := range workload.Registered() {
			gen := meta.New()
			var genFlags *pflag.FlagSet
			if f, ok := gen.(workload.Flagser); ok {
				genFlags = f.Flags().FlagSet
				// Hide runtime-only flags so they don't clutter up the help text,
				// but don't remove them entirely so if someone switches from
				// `./workload run` to `./workload fixtures` they don't have to
				// remove them from the invocation.
				for flagName, meta := range f.Flags().Meta {
					if meta.RuntimeOnly || meta.CheckConsistencyOnly {
						_ = genFlags.MarkHidden(flagName)
					}
				}
			}

			genMakeCmd := workloadcli.SetCmdDefaults(&cobra.Command{
				Use:  meta.Name + ` [CRDB URI]`,
				Args: cobra.RangeArgs(0, 1),
			})
			genMakeCmd.Flags().AddFlagSet(genFlags)
			genMakeCmd.Flags().AddFlagSet(fixturesMakeImportShared)
			genMakeCmd.Run = workloadcli.CmdHelper(gen, fixturesMake)
			fixturesMakeCmd.AddCommand(genMakeCmd)

			genLoadCmd := workloadcli.SetCmdDefaults(&cobra.Command{
				Use:  meta.Name + ` [CRDB URI]`,
				Args: cobra.RangeArgs(0, 1),
			})
			genLoadCmd.Flags().AddFlagSet(genFlags)
			genLoadCmd.Flags().AddFlagSet(fixturesLoadImportShared)
			genLoadCmd.Run = workloadcli.CmdHelper(gen, fixturesLoad)
			fixturesLoadCmd.AddCommand(genLoadCmd)

			genImportCmd := workloadcli.SetCmdDefaults(&cobra.Command{
				Use:  meta.Name + ` [CRDB URI]`,
				Args: cobra.RangeArgs(0, 1),
			})
			genImportCmd.Flags().AddFlagSet(genFlags)
			genImportCmd.Flags().AddFlagSet(fixturesLoadImportShared)
			genImportCmd.Flags().AddFlagSet(fixturesMakeImportShared)
			genImportCmd.Run = workloadcli.CmdHelper(gen, fixturesImport)
			fixturesImportCmd.AddCommand(genImportCmd)

			genURLCmd := workloadcli.SetCmdDefaults(&cobra.Command{
				Use:  meta.Name,
				Args: cobra.NoArgs,
			})
			genURLCmd.Flags().AddFlagSet(genFlags)
			genURLCmd.Run = fixturesURL(gen)
			fixturesURLCmd.AddCommand(genURLCmd)
		}
		fixturesCmd.AddCommand(fixturesListCmd)
		fixturesCmd.AddCommand(fixturesMakeCmd)
		fixturesCmd.AddCommand(fixturesLoadCmd)
		fixturesCmd.AddCommand(fixturesImportCmd)
		fixturesCmd.AddCommand(fixturesURLCmd)
		return fixturesCmd
	})
}

func fixturesList(_ *cobra.Command, _ []string) error {
	ctx := context.Background()
	gcs, err := getStorage(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = gcs.Close() }()
	fixtures, err := workloadccl.ListFixtures(ctx, gcs, config())
	if err != nil {
		return err
	}
	for _, fixture := range fixtures {
		fmt.Println(fixture)
	}
	return nil
}

type filteringGenerator struct {
	gen    workload.Generator
	filter map[string]struct{}
}

func (f filteringGenerator) Meta() workload.Meta {
	return f.gen.Meta()
}

func (f filteringGenerator) Tables() []workload.Table {
	ret := make([]workload.Table, 0)
	for _, t := range f.gen.Tables() {
		if _, ok := f.filter[t.Name]; ok {
			ret = append(ret, t)
		}
	}
	return ret
}

func fixturesMake(gen workload.Generator, urls []string, _ string) error {
	ctx := context.Background()
	gcs, err := getStorage(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = gcs.Close() }()

	sqlDB, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return err
	}
	if *fixturesMakeOnlyTable != "" {
		tableNames := strings.Split(*fixturesMakeOnlyTable, ",")
		if len(tableNames) == 0 {
			return errors.New("no table names specified")
		}
		filter := make(map[string]struct{}, len(tableNames))
		for _, tableName := range tableNames {
			filter[tableName] = struct{}{}
		}
		gen = filteringGenerator{
			gen:    gen,
			filter: filter,
		}
	}
	filesPerNode := *fixturesMakeFilesPerNode
	fixture, err := workloadccl.MakeFixture(ctx, sqlDB, gcs, config(), gen, filesPerNode)
	if err != nil {
		return err
	}
	for _, table := range fixture.Tables {
		log.Infof(ctx, `stored backup %s`, table.BackupURI)
	}
	return nil
}

// restoreDataLoader is an InitialDataLoader implementation that loads data with
// RESTORE.
type restoreDataLoader struct {
	fixture  workloadccl.Fixture
	database string
}

// InitialDataLoad implements the InitialDataLoader interface.
func (l restoreDataLoader) InitialDataLoad(
	ctx context.Context, db *gosql.DB, gen workload.Generator,
) (int64, error) {
	log.Infof(ctx, "starting restore of %d tables", len(gen.Tables()))
	start := timeutil.Now()
	bytes, err := workloadccl.RestoreFixture(ctx, db, l.fixture, l.database, true /* injectStats */)
	if err != nil {
		return 0, errors.Wrap(err, `restoring fixture`)
	}
	elapsed := timeutil.Since(start)
	log.Infof(ctx, "restored %s bytes in %d tables (took %s, %s)",
		humanizeutil.IBytes(bytes), len(gen.Tables()), elapsed, humanizeutil.DataRate(bytes, elapsed))
	return bytes, nil
}

func fixturesLoad(gen workload.Generator, urls []string, dbName string) error {
	ctx := context.Background()
	gcs, err := getStorage(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = gcs.Close() }()

	sqlDB, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return err
	}
	if _, err := sqlDB.Exec(`CREATE DATABASE IF NOT EXISTS ` + dbName); err != nil {
		return err
	}

	fixture, err := workloadccl.GetFixture(ctx, gcs, config(), gen)
	if err != nil {
		return errors.Wrap(err, `finding fixture`)
	}

	l := restoreDataLoader{fixture: fixture, database: dbName}
	if _, err := workloadsql.Setup(ctx, sqlDB, gen, l); err != nil {
		return err
	}

	if hooks, ok := gen.(workload.Hookser); *fixturesRunChecks && ok {
		if consistencyCheckFn := hooks.Hooks().CheckConsistency; consistencyCheckFn != nil {
			log.Info(ctx, "fixture is imported; now running consistency checks (ctrl-c to abort)")
			if err := consistencyCheckFn(ctx, sqlDB); err != nil {
				return err
			}
		}
	}

	return nil
}

func fixturesImport(gen workload.Generator, urls []string, dbName string) error {
	ctx := context.Background()
	sqlDB, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return err
	}
	if _, err := sqlDB.Exec(`CREATE DATABASE IF NOT EXISTS ` + dbName); err != nil {
		return err
	}

	l := workloadccl.ImportDataLoader{
		FilesPerNode: *fixturesImportFilesPerNode,
		InjectStats:  *fixturesImportInjectStats,
		CSVServer:    *fixturesMakeImportCSVServerURL,
	}
	if _, err := workloadsql.Setup(ctx, sqlDB, gen, l); err != nil {
		return err
	}

	if hooks, ok := gen.(workload.Hookser); *fixturesRunChecks && ok {
		if consistencyCheckFn := hooks.Hooks().CheckConsistency; consistencyCheckFn != nil {
			log.Info(ctx, "fixture is restored; now running consistency checks (ctrl-c to abort)")
			if err := consistencyCheckFn(ctx, sqlDB); err != nil {
				return err
			}
		}
	}

	return nil
}

func fixturesURL(gen workload.Generator) func(*cobra.Command, []string) {
	return workloadcli.HandleErrs(func(*cobra.Command, []string) error {
		if h, ok := gen.(workload.Hookser); ok {
			if err := h.Hooks().Validate(); err != nil {
				return err
			}
		}

		fmt.Println(workloadccl.FixtureURL(config(), gen))
		return nil
	})
}
