// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliccl

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/cliccl/cliflagsccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func init() {
	loadCSVCmd := &cobra.Command{
		Use:   "csv --table=name --dest=directory [--data=file [...]] [flags]",
		Short: "convert CSV files into enterprise backup format",
		Long: `
Convert CSV files into enterprise backup format.

The file at table must contain a single CREATE TABLE statement. The
files at the various data options (or table.dat if unspecified) must be
CSV files with data matching the table. A comma is used as a delimiter,
but can be changed with the delimiter option. Lines beginning with
comment are ignored. Fields are considered null if equal to nullif
(may be the empty string).

The backup's tables are created in the "csv" database.

It requires approximately 2x the size of the data files of free disk
space. An intermediate copy will be stored in the location specified
by the --tempdir argument, and the final copy in the dest directory.

For example, if there were a file at /data/names containing:

	CREATE TABLE names (first string, last string)

And a file at /data/names.dat containing:

	James,Kirk
	Leonard,McCoy
	Spock,

Then the file could be converted and saved to /data/backup with:

	cockroach load csv --table '/data/names' --nullif '' --dest '/data/backup'
`,
		RunE: cli.MaybeDecorateGRPCError(runLoadCSV),
	}
	f := loadCSVCmd.PersistentFlags()
	cli.StringFlag(f, &csvTableName, cliflagsccl.CSVTable, "")
	cli.StringSliceFlag(f, &csvDataNames, cliflagsccl.CSVDataNames, nil)
	cli.StringFlag(f, &csvDest, cliflagsccl.CSVDest, "")
	cli.StringFlag(f, &csvNullIf, cliflagsccl.CSVNullIf, "")
	cli.StringFlag(f, &csvComma, cliflagsccl.CSVComma, "")
	cli.StringFlag(f, &csvComment, cliflagsccl.CSVComment, "")
	cli.StringFlag(f, &csvTempDir, cliflagsccl.CSVTempDir, os.TempDir())

	loadShowCmd := &cobra.Command{
		Use:   "show <basepath>",
		Short: "show backups",
		Long:  "Shows information about a SQL backup.",
		RunE:  cli.MaybeDecorateGRPCError(runLoadShow),
	}

	loadCmds := &cobra.Command{
		Use:   "load [command]",
		Short: "loading commands",
		Long:  `Commands for bulk loading external files.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}
	cli.AddCmd(loadCmds)
	loadCmds.AddCommand(loadCSVCmd)
	loadCmds.AddCommand(loadShowCmd)
}

var (
	csvComma     string
	csvComment   string
	csvDataNames []string
	csvDest      string
	csvNullIf    string
	csvTableName string
	csvTempDir   string
)

func runLoadCSV(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	// The Go CSV package by default uses a comma and doesn't allow comments. We
	// use GetFirstRune to check if there is a valid and single Unicode rune
	// specified. If not, GetFirstRune returns 0. If the 0 rune is passed to
	// LoadCSV, it leaves the Go defaults for those options. Otherwise, it uses
	// that rune as the delimiter or comment char.
	comma, err := util.GetSingleRune(csvComma)
	if err != nil {
		return errors.Wrap(err, "delimiter flag")
	}
	comment, err := util.GetSingleRune(csvComment)
	if err != nil {
		return errors.Wrap(err, "comment flag")
	}
	var nullIf *string
	// pflags doesn't have an option to have a flag without a default value
	// (which would leave it as nil). Instead, we must iterate through all set
	// flags and detect its presence ourselves.
	cmd.Flags().Visit(func(f *pflag.Flag) {
		if f.Name != "nullif" {
			return
		}
		s := f.Value.String()
		nullIf = &s
	})

	const sstMaxSize = 1024 * 1024 * 50

	csv, kv, sst, err := sqlccl.LoadCSV(
		ctx,
		csvTableName,
		csvDataNames,
		csvDest,
		comma,
		comment,
		nullIf,
		sstMaxSize,
		csvTempDir,
	)
	if err != nil {
		return err
	}
	log.Infof(ctx, "CSV rows read: %d", csv)
	log.Infof(ctx, "KVs pairs created: %d", kv)
	log.Infof(ctx, "SST files written: %d", sst)

	return nil
}

func runLoadShow(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return errors.New("basepath argument is required")
	}

	ctx := context.Background()
	basepath := args[0]
	if !strings.Contains(basepath, "://") {
		var err error
		basepath, err = storageccl.MakeLocalStorageURI(basepath)
		if err != nil {
			return err
		}
	}
	desc, err := sqlccl.ReadBackupDescriptorFromURI(ctx, basepath, cluster.NoSettings)
	if err != nil {
		return err
	}
	start := timeutil.Unix(0, desc.StartTime.WallTime).Format(time.RFC3339Nano)
	end := timeutil.Unix(0, desc.EndTime.WallTime).Format(time.RFC3339Nano)
	fmt.Printf("StartTime: %s (%s)\n", start, desc.StartTime)
	fmt.Printf("EndTime: %s (%s)\n", end, desc.EndTime)
	fmt.Printf("DataSize: %d (%s)\n", desc.EntryCounts.DataSize, humanizeutil.IBytes(desc.EntryCounts.DataSize))
	fmt.Printf("Rows: %d\n", desc.EntryCounts.Rows)
	fmt.Printf("IndexEntries: %d\n", desc.EntryCounts.IndexEntries)
	fmt.Printf("SystemRecords: %d\n", desc.EntryCounts.SystemRecords)
	fmt.Printf("FormatVersion: %d\n", desc.FormatVersion)
	fmt.Printf("ClusterID: %s\n", desc.ClusterID)
	fmt.Printf("NodeID: %s\n", desc.NodeID)
	fmt.Printf("BuildInfo: %s\n", desc.BuildInfo.Short())
	fmt.Printf("Spans:\n")
	for _, s := range desc.Spans {
		fmt.Printf("	%s\n", s)
	}
	fmt.Printf("Files:\n")
	for _, f := range desc.Files {
		fmt.Printf("	%s:\n", f.Path)
		fmt.Printf("		Span: %s\n", f.Span)
		fmt.Printf("		Sha512: %0128x\n", f.Sha512)
		fmt.Printf("		DataSize: %d (%s)\n", f.EntryCounts.DataSize, humanizeutil.IBytes(f.EntryCounts.DataSize))
		fmt.Printf("		Rows: %d\n", f.EntryCounts.Rows)
		fmt.Printf("		IndexEntries: %d\n", f.EntryCounts.IndexEntries)
		fmt.Printf("		SystemRecords: %d\n", f.EntryCounts.SystemRecords)
	}
	fmt.Printf("Descriptors:\n")
	for _, d := range desc.Descriptors {
		if desc := d.GetTable(); desc != nil {
			fmt.Printf("	%d: %s (table)\n", d.GetID(), d.GetName())
		}
		if desc := d.GetDatabase(); desc != nil {
			fmt.Printf("	%d: %s (database)\n", d.GetID(), d.GetName())
		}
	}
	return nil
}
