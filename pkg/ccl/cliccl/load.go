// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package cliccl

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"

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
space. An intermediate copy will be stored in the OS temp directory,
and the final copy in the dest directory.

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
	flags := loadCSVCmd.PersistentFlags()
	flags.StringVar(&csvTableName, "table", "", "location of a file containing a single CREATE TABLE statement")
	flags.StringSliceVar(&csvDataNames, "data", nil, "filenames of CSV data; uses <table>.dat if empty")
	flags.StringVar(&csvDest, "dest", "", "destination directory for backup files")
	flags.StringVar(&csvNullIf, "nullif", "", "if specified, the value of NULL; can specify the empty string")
	flags.StringVar(&csvComma, "delimiter", "", "if specified, the CSV delimiter instead of a comma")
	flags.StringVar(&csvComment, "comment", "", "if specified, allows comment lines starting with this character")

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
}

var (
	csvComma     string
	csvComment   string
	csvDataNames []string
	csvDest      string
	csvNullIf    string
	csvTableName string
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
	)
	if err != nil {
		return err
	}
	log.Infof(ctx, "CSV rows read: %d", csv)
	log.Infof(ctx, "KVs pairs created: %d", kv)
	log.Infof(ctx, "SST files written: %d", sst)

	return nil
}
