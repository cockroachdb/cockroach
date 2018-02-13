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

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/api/option"

	"github.com/cockroachdb/cockroach/pkg/ccl/workloadccl"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload"
)

var useast1bFixtures = workloadccl.FixtureStore{
	// TODO(dan): Keep fixtures in more than one region to better support
	// geo-distributed clusters.
	GCSBucket: `cockroach-fixtures`,
	GCSPrefix: `workload`,
}

var fixturesCmd = &cobra.Command{Use: `fixtures`}
var fixturesListCmd = &cobra.Command{
	Use:   `list`,
	Short: `List all fixtures stored on GCS`,
	RunE:  fixturesList,
}
var fixturesStoreCmd = &cobra.Command{
	Use:   `store`,
	Short: `Regenerate and store a fixture on GCS`,
}
var fixturesLoadCmd = &cobra.Command{
	Use: `load`,
	Short: `Load a fixture into a running cluster. ` +
		`An enterprise license is required.`,
}

var fixturesStoreCSVServerURL = fixturesStoreCmd.PersistentFlags().String(
	`csv-server`, ``,
	`Skip saving CSVs to cloud storage, instead get them from a 'csv-server' running at this url`)

// gcs-bucket-override and gcs-prefix-override are exposed for testing.
var gcsBucketOverride, gcsPrefixOverride *string

func init() {
	gcsBucketOverride = fixturesStoreCmd.PersistentFlags().String(`gcs-bucket-override`, ``, ``)
	gcsPrefixOverride = fixturesStoreCmd.PersistentFlags().String(`gcs-prefix-override`, ``, ``)
	_ = fixturesStoreCmd.PersistentFlags().MarkHidden(`gcs-bucket-override`)
	_ = fixturesStoreCmd.PersistentFlags().MarkHidden(`gcs-prefix-override`)
}

var fixturesLoadDB = fixturesLoadCmd.PersistentFlags().String(
	`into-db`, `workload`, `SQL database to load fixture into`)

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
		}

		genStoreCmd := &cobra.Command{
			Use:  meta.Name + ` [CRDB URI]`,
			Args: cobra.RangeArgs(0, 1),
		}
		genStoreCmd.Flags().AddFlagSet(genFlags)
		genStoreCmd.RunE = func(cmd *cobra.Command, args []string) error {
			crdb := crdbDefaultURI
			if len(args) > 0 {
				crdb = args[0]
			}
			return fixturesStore(cmd, gen, crdb)
		}
		fixturesStoreCmd.AddCommand(genStoreCmd)

		genLoadCmd := &cobra.Command{
			Use:  meta.Name + ` [CRDB URI]`,
			Args: cobra.RangeArgs(0, 1),
		}
		genLoadCmd.Flags().AddFlagSet(genFlags)
		genLoadCmd.RunE = func(cmd *cobra.Command, args []string) error {
			crdb := crdbDefaultURI
			if len(args) > 0 {
				crdb = args[0]
			}
			return fixturesLoad(cmd, gen, crdb)
		}
		fixturesLoadCmd.AddCommand(genLoadCmd)
	}
	fixturesCmd.AddCommand(fixturesListCmd)
	fixturesCmd.AddCommand(fixturesStoreCmd)
	fixturesCmd.AddCommand(fixturesLoadCmd)
	rootCmd.AddCommand(fixturesCmd)
}

func fixturesList(cmd *cobra.Command, _ []string) error {
	ctx := context.Background()
	gcs, err := getStorage(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = gcs.Close() }()
	fixtures, err := workloadccl.ListFixtures(ctx, gcs, useast1bFixtures)
	if err != nil {
		return err
	}
	for _, fixture := range fixtures {
		fmt.Println(fixture)
	}
	return nil
}

func fixturesStore(cmd *cobra.Command, gen workload.Generator, crdbURI string) error {
	ctx := context.Background()
	gcs, err := getStorage(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = gcs.Close() }()

	sqlDB, err := gosql.Open(`postgres`, crdbURI)
	if err != nil {
		return err
	}
	store := useast1bFixtures
	if len(*gcsBucketOverride) > 0 {
		store.GCSBucket = *gcsBucketOverride
	}
	if len(*gcsPrefixOverride) > 0 {
		store.GCSPrefix = *gcsPrefixOverride
	}
	store.CSVServerURL = *fixturesStoreCSVServerURL
	fixture, err := workloadccl.MakeFixture(ctx, sqlDB, gcs, store, gen)
	if err != nil {
		return err
	}
	for _, table := range fixture.Tables {
		log.Infof(ctx, `stored backup %s`, table.BackupURI)
	}
	return nil
}

func fixturesLoad(cmd *cobra.Command, gen workload.Generator, crdbURI string) error {
	ctx := context.Background()
	gcs, err := getStorage(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = gcs.Close() }()

	sqlDB, err := gosql.Open(`postgres`, crdbURI)
	if err != nil {
		return err
	}
	if _, err := sqlDB.Exec(`CREATE DATABASE IF NOT EXISTS ` + *fixturesLoadDB); err != nil {
		return err
	}

	fixture, err := workloadccl.GetFixture(ctx, gcs, useast1bFixtures, gen)
	if err != nil {
		return errors.Wrap(err, `finding fixture`)
	}
	if err := workloadccl.RestoreFixture(ctx, sqlDB, fixture, *fixturesLoadDB); err != nil {
		return errors.Wrap(err, `restoring fixture`)
	}
	for _, table := range fixture.Tables {
		log.Infof(ctx, `loaded %s`, table.BackupURI)
	}
	return nil
}
