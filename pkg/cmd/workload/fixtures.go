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

package main

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/api/option"

	"github.com/cockroachdb/cockroach/pkg/ccl/workloadccl"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload"
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
	config.CSVServerURL = *fixturesMakeCSVServerURL
	return config
}

var fixturesCmd = &cobra.Command{Use: `fixtures`}
var fixturesListCmd = &cobra.Command{
	Use:   `list`,
	Short: `List all fixtures stored on GCS`,
	Run:   handleErrs(fixturesList),
}
var fixturesMakeCmd = &cobra.Command{
	Use:   `make`,
	Short: `Regenerate and store a fixture on GCS`,
}
var fixturesLoadCmd = &cobra.Command{
	Use:   `load`,
	Short: `Load a fixture into a running cluster. An enterprise license is required.`,
}

var fixturesMakeCSVServerURL = fixturesMakeCmd.PersistentFlags().String(
	`csv-server`, ``,
	`Skip saving CSVs to cloud storage, instead get them from a 'csv-server' running at this url`)

var fixturesMakeOnlyTable = fixturesMakeCmd.PersistentFlags().String(
	`only-tables`, ``,
	`Only load the tables with the given comma-separated names`)

var fixturesLoadRunChecks = fixturesLoadCmd.PersistentFlags().Bool(
	`checks`, true, `Run validity checks on the loaded fixture`)

// gcs-bucket-override and gcs-prefix-override are exposed for testing.
var gcsBucketOverride, gcsPrefixOverride *string

func init() {
	gcsBucketOverride = fixturesCmd.PersistentFlags().String(`gcs-bucket-override`, ``, ``)
	gcsPrefixOverride = fixturesCmd.PersistentFlags().String(`gcs-prefix-override`, ``, ``)
	_ = fixturesCmd.PersistentFlags().MarkHidden(`gcs-bucket-override`)
	_ = fixturesCmd.PersistentFlags().MarkHidden(`gcs-prefix-override`)
}

const storageError = `failed to create google cloud client ` +
	`(You may need to setup the GCS application default credentials: ` +
	`'gcloud auth application-default login --project=cockroach-shared')`

// getStorage returns a GCS client using "application default" credentials. The
// caller is responsible for closing it.
func getStorage(ctx context.Context) (*storage.Client, error) {
	// TODO(dan): Right now, we don't need all the complexity of
	// storageccl.ExportStorage, but if we start supporting more than just GCS,
	// this should probably be switched to it.
	g, err := storage.NewClient(ctx, option.WithScopes(storage.ScopeReadWrite))
	return g, errors.Wrap(err, storageError)
}

func init() {
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

		genMakeCmd := &cobra.Command{
			Use:  meta.Name + ` [CRDB URI]`,
			Args: cobra.RangeArgs(0, 1),
		}
		genMakeCmd.Flags().AddFlagSet(genFlags)
		genMakeCmd.Run = cmdHelper(gen, fixturesMake)
		fixturesMakeCmd.AddCommand(genMakeCmd)

		genLoadCmd := &cobra.Command{
			Use:  meta.Name + ` [CRDB URI]`,
			Args: cobra.RangeArgs(0, 1),
		}
		genLoadCmd.Flags().AddFlagSet(genFlags)
		genLoadCmd.Run = cmdHelper(gen, fixturesLoad)
		fixturesLoadCmd.AddCommand(genLoadCmd)
	}
	fixturesCmd.AddCommand(fixturesListCmd)
	fixturesCmd.AddCommand(fixturesMakeCmd)
	fixturesCmd.AddCommand(fixturesLoadCmd)
	rootCmd.AddCommand(fixturesCmd)
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

func fixturesMake(gen workload.Generator, urls []string, dbName string) error {
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
	fixture, err := workloadccl.MakeFixture(ctx, sqlDB, gcs, config(), gen)
	if err != nil {
		return err
	}
	for _, table := range fixture.Tables {
		log.Infof(ctx, `stored backup %s`, table.BackupURI)
	}
	return nil
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
	if err := workloadccl.RestoreFixture(ctx, sqlDB, fixture, dbName); err != nil {
		return errors.Wrap(err, `restoring fixture`)
	}

	if hooks, ok := gen.(workload.Hookser); *fixturesLoadRunChecks && ok {
		if consistencyCheckFn := hooks.Hooks().CheckConsistency; consistencyCheckFn != nil {
			log.Info(ctx, "fixture is restored; now running consistency checks (ctrl-c to abort)")
			if err := consistencyCheckFn(ctx, sqlDB); err != nil {
				return err
			}
		}
	}

	return nil
}
