// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliccl

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var externalIODir string
var readTime string
var destination string
var format string
var nullas string

func init() {

	loadShowSummaryCmd := &cobra.Command{
		Use:   "summary <backup_path>",
		Short: "show backups summary",
		Long:  "Shows summary of meta information about a SQL backup.",
		Args:  cobra.ExactArgs(1),
		RunE:  cli.MaybeDecorateGRPCError(runLoadShowSummary),
	}

	loadShowBackupsCmd := &cobra.Command{
		Use:   "backups <backup_path>",
		Short: "show backups in collections",
		Long:  "Shows full backups in a backup collections.",
		Args:  cobra.ExactArgs(1),
		RunE:  cli.MaybeDecorateGRPCError(runLoadShowBackups),
	}

	loadShowIncrementalCmd := &cobra.Command{
		Use:   "incremental <backup_path>",
		Short: "show incremental backups",
		Long:  "Shows incremental chain of a SQL backup.",
		Args:  cobra.ExactArgs(1),
		RunE:  cli.MaybeDecorateGRPCError(runLoadShowIncremental),
	}

	loadShowDataCmd := &cobra.Command{
		Use:   "data <table> <backup_path>",
		Short: "show data",
		Long:  "Shows data of a SQL backup.",
		Args:  cobra.MinimumNArgs(2),
		RunE:  cli.MaybeDecorateGRPCError(runLoadShowData),
	}

	loadShowCmds := &cobra.Command{
		Use:   "show [command]",
		Short: "show backups",
		Long:  "Shows information about a SQL backup.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}

	loadCmds := &cobra.Command{
		Use:   "load [command]",
		Short: "load backup commands",
		Long:  `Commands for bulk loading external files.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}

	loadFlags := loadCmds.Flags()
	loadFlags.StringVarP(
		&externalIODir,
		cliflags.ExternalIODir.Name,
		cliflags.ExternalIODir.Shorthand,
		"", /*value*/
		cliflags.ExternalIODir.Usage())

	loadShowDataCmd.Flags().StringVarP(
		&readTime,
		cliflags.ReadTime.Name,
		cliflags.ReadTime.Shorthand,
		"", /*value*/
		cliflags.ReadTime.Usage())

	loadShowDataCmd.Flags().StringVarP(
		&destination,
		cliflags.ExportDestination.Name,
		cliflags.ExportDestination.Shorthand,
		"", /*value*/
		cliflags.ExportDestination.Usage())

	loadShowDataCmd.Flags().StringVarP(
		&format,
		cliflags.ExportTableFormat.Name,
		cliflags.ExportTableFormat.Shorthand,
		"csv", /*value*/
		cliflags.ExportTableFormat.Usage())

	loadShowDataCmd.Flags().StringVarP(
		&nullas,
		cliflags.ExportCSVNullas.Name,
		cliflags.ExportCSVNullas.Shorthand,
		"null", /*value*/
		cliflags.ExportCSVNullas.Usage())

	cli.AddCmd(loadCmds)
	loadCmds.AddCommand(loadShowCmds)

	loadShowSubCmds := []*cobra.Command{
		loadShowSummaryCmd,
		loadShowBackupsCmd,
		loadShowIncrementalCmd,
		loadShowDataCmd,
	}

	for _, cmd := range loadShowSubCmds {
		loadShowCmds.AddCommand(cmd)
		cmd.Flags().AddFlagSet(loadFlags)
	}
}

func newBlobFactory(ctx context.Context, dialing roachpb.NodeID) (blobs.BlobClient, error) {
	if dialing != 0 {
		return nil, errors.Errorf("accessing node %d during nodelocal access is unsupported for CLI inspection; only local access is supported with nodelocal://self", dialing)
	}
	if externalIODir == "" {
		externalIODir = filepath.Join(server.DefaultStorePath, "extern")
	}
	return blobs.NewLocalClient(externalIODir)
}

func externalStorageFromURIFactory(
	ctx context.Context, uri string, user security.SQLUsername,
) (cloud.ExternalStorage, error) {
	return cloudimpl.ExternalStorageFromURI(ctx, uri, base.ExternalIODirConfig{},
		cluster.NoSettings, newBlobFactory, user, nil /*Internal Executor*/, nil /*kvDB*/)
}

func getManifestFromURI(ctx context.Context, path string) (backupccl.BackupManifest, error) {

	if !strings.Contains(path, "://") {
		path = cloudimpl.MakeLocalStorageURI(path)
	}
	// This reads the raw backup descriptor (with table descriptors possibly not
	// upgraded from the old FK representation, or even older formats). If more
	// fields are added to the output, the table descriptors may need to be
	// upgraded.
	backupManifest, err := backupccl.ReadBackupManifestFromURI(ctx, path, security.RootUserName(),
		externalStorageFromURIFactory, nil)
	if err != nil {
		return backupccl.BackupManifest{}, err
	}
	return backupManifest, nil
}

func runLoadShowSummary(cmd *cobra.Command, args []string) error {

	path := args[0]
	ctx := context.Background()
	desc, err := getManifestFromURI(ctx, path)
	if err != nil {
		return errors.Wrapf(err, "fetching backup manifest")
	}
	showMeta(desc)
	showSpans(desc)
	showFiles(desc)
	showDescriptors(desc)
	return nil
}

func runLoadShowBackups(cmd *cobra.Command, args []string) error {

	path := args[0]
	if !strings.Contains(path, "://") {
		path = cloudimpl.MakeLocalStorageURI(path)
	}
	ctx := context.Background()
	store, err := externalStorageFromURIFactory(ctx, path, security.RootUserName())
	if err != nil {
		return errors.Wrapf(err, "connect to external storage")
	}
	defer store.Close()

	backupPaths, err := backupccl.ListFullBackupsInCollection(ctx, store)
	if err != nil {
		return errors.Wrapf(err, "list full backups in collection")
	}

	if len(backupPaths) == 0 {
		fmt.Println("no backups found.")
	}

	for _, backupPath := range backupPaths {
		fmt.Println("./" + backupPath)
	}

	return nil
}

func runLoadShowIncremental(cmd *cobra.Command, args []string) error {

	path := args[0]
	if !strings.Contains(path, "://") {
		path = cloudimpl.MakeLocalStorageURI(path)
	}

	uri, err := url.Parse(path)
	if err != nil {
		return err
	}

	ctx := context.Background()
	store, err := externalStorageFromURIFactory(ctx, uri.String(), security.RootUserName())
	if err != nil {
		return errors.Wrapf(err, "connect to external storage")
	}
	defer store.Close()

	incPaths, err := backupccl.FindPriorBackupLocations(ctx, store)
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 28 /*minwidth*/, 1 /*tabwidth*/, 2 /*padding*/, ' ' /*padchar*/, 0 /*flags*/)
	basepath := uri.Path
	manifestPaths := append([]string{""}, incPaths...)
	stores := make([]cloud.ExternalStorage, len(manifestPaths))
	stores[0] = store

	for i := range manifestPaths {

		if i > 0 {
			uri.Path = filepath.Join(basepath, manifestPaths[i])
			stores[i], err = externalStorageFromURIFactory(ctx, uri.String(), security.RootUserName())
			if err != nil {
				return errors.Wrapf(err, "connect to external storage")
			}
			defer stores[i].Close()
		}

		manifest, err := backupccl.ReadBackupManifestFromStore(ctx, stores[i], nil)
		if err != nil {
			return err
		}
		startTime := manifest.StartTime.GoTime().Format(time.RFC3339)
		endTime := manifest.EndTime.GoTime().Format(time.RFC3339)
		if i == 0 {
			startTime = "-"
		}
		fmt.Fprintf(w, "%s	%s	%s\n", uri.Path, startTime, endTime)
	}

	if err := w.Flush(); err != nil {
		return err
	}
	return nil
}

func showMeta(desc backupccl.BackupManifest) {
	start := timeutil.Unix(0, desc.StartTime.WallTime).Format(time.RFC3339Nano)
	end := timeutil.Unix(0, desc.EndTime.WallTime).Format(time.RFC3339Nano)
	fmt.Printf("StartTime: %s (%s)\n", start, desc.StartTime)
	fmt.Printf("EndTime: %s (%s)\n", end, desc.EndTime)
	fmt.Printf("DataSize: %d (%s)\n", desc.EntryCounts.DataSize, humanizeutil.IBytes(desc.EntryCounts.DataSize))
	fmt.Printf("Rows: %d\n", desc.EntryCounts.Rows)
	fmt.Printf("IndexEntries: %d\n", desc.EntryCounts.IndexEntries)
	fmt.Printf("FormatVersion: %d\n", desc.FormatVersion)
	fmt.Printf("ClusterID: %s\n", desc.ClusterID)
	fmt.Printf("NodeID: %s\n", desc.NodeID)
	fmt.Printf("BuildInfo: %s\n", desc.BuildInfo.Short())
}

func showSpans(desc backupccl.BackupManifest) {
	fmt.Printf("Spans:\n")
	if len(desc.Spans) == 0 {
		fmt.Printf("	(No spans included in the specified backup path.)\n")
	}
	for _, s := range desc.Spans {
		fmt.Printf("	%s\n", s)
	}
}

func showFiles(desc backupccl.BackupManifest) {
	fmt.Printf("Files:\n")
	if len(desc.Files) == 0 {
		fmt.Printf("	(No sst files included in the specified backup path.)\n")
	}
	for _, f := range desc.Files {
		fmt.Printf("	%s:\n", f.Path)
		fmt.Printf("		Span: %s\n", f.Span)
		fmt.Printf("		DataSize: %d (%s)\n", f.EntryCounts.DataSize, humanizeutil.IBytes(f.EntryCounts.DataSize))
		fmt.Printf("		Rows: %d\n", f.EntryCounts.Rows)
		fmt.Printf("		IndexEntries: %d\n", f.EntryCounts.IndexEntries)
	}
}

func showDescriptors(desc backupccl.BackupManifest) {
	// Note that these descriptors could be from any past version of the cluster,
	// in case more fields need to be added to the output.
	dbIDs := make([]descpb.ID, 0, len(desc.Descriptors))
	dbIDToName := make(map[descpb.ID]string)
	schemaIDs := make([]descpb.ID, 0, len(desc.Descriptors))
	schemaIDs = append(schemaIDs, keys.PublicSchemaID)
	schemaIDToFullyQualifiedName := make(map[descpb.ID]string)
	schemaIDToFullyQualifiedName[keys.PublicSchemaID] = sessiondata.PublicSchemaName
	typeIDs := make([]descpb.ID, 0, len(desc.Descriptors))
	typeIDToFullyQualifiedName := make(map[descpb.ID]string)
	tableIDs := make([]descpb.ID, 0, len(desc.Descriptors))
	tableIDToFullyQualifiedName := make(map[descpb.ID]string)
	for i := range desc.Descriptors {
		d := &desc.Descriptors[i]
		id := descpb.GetDescriptorID(d)
		tableDesc, databaseDesc, typeDesc, schemaDesc := descpb.FromDescriptor(d)
		if databaseDesc != nil {
			dbIDToName[id] = descpb.GetDescriptorName(d)
			dbIDs = append(dbIDs, id)
		} else if schemaDesc != nil {
			dbName := dbIDToName[schemaDesc.GetParentID()]
			schemaName := descpb.GetDescriptorName(d)
			schemaIDToFullyQualifiedName[id] = dbName + "." + schemaName
			schemaIDs = append(schemaIDs, id)
		} else if typeDesc != nil {
			parentSchema := schemaIDToFullyQualifiedName[typeDesc.GetParentSchemaID()]
			if parentSchema == sessiondata.PublicSchemaName {
				parentSchema = dbIDToName[typeDesc.GetParentID()] + "." + parentSchema
			}
			typeName := descpb.GetDescriptorName(d)
			typeIDToFullyQualifiedName[id] = parentSchema + "." + typeName
			typeIDs = append(typeIDs, id)
		} else if tableDesc != nil {
			tbDesc := tabledesc.NewBuilder(tableDesc).BuildImmutable()
			parentSchema := schemaIDToFullyQualifiedName[tbDesc.GetParentSchemaID()]
			if parentSchema == sessiondata.PublicSchemaName {
				parentSchema = dbIDToName[tableDesc.GetParentID()] + "." + parentSchema
			}
			tableName := descpb.GetDescriptorName(d)
			tableIDToFullyQualifiedName[id] = parentSchema + "." + tableName
			tableIDs = append(tableIDs, id)
		}
	}

	fmt.Printf("Databases:\n")
	for _, id := range dbIDs {
		fmt.Printf("	%d: %s\n",
			id, dbIDToName[id])
	}

	fmt.Printf("Schemas:\n")
	for _, id := range schemaIDs {
		fmt.Printf("	%d: %s\n",
			id, schemaIDToFullyQualifiedName[id])
	}

	fmt.Printf("Types:\n")
	if len(typeIDs) == 0 {
		fmt.Printf("	(No user-defined types included in the specified backup path.)\n")
	}
	for _, id := range typeIDs {
		fmt.Printf("	%d: %s\n",
			id, typeIDToFullyQualifiedName[id])
	}

	fmt.Printf("Tables:\n")
	if len(tableIDs) == 0 {
		fmt.Printf("	(No tables included in the specified backup path.)\n")
	}
	for _, id := range tableIDs {
		fmt.Printf("	%d: %s\n",
			id, tableIDToFullyQualifiedName[id])
	}
}

func runLoadShowData(cmd *cobra.Command, args []string) error {

	fullyQualifiedTableName := strings.ToLower(args[0])
	manifestPaths := args[1:]

	ctx := context.Background()
	manifests := make([]backupccl.BackupManifest, 0, len(manifestPaths))
	for _, path := range manifestPaths {
		manifest, err := getManifestFromURI(ctx, path)
		if err != nil {
			return errors.Wrapf(err, "fetching backup manifests from %s", path)
		}
		manifests = append(manifests, manifest)
	}

	endTime, err := evalAsOfTimestamp(readTime)
	if err != nil {
		return errors.Wrapf(err, "eval as of timestamp %s", readTime)
	}

	codec := keys.TODOSQLCodec
	entry, err := backupccl.MakeBackupTableEntry(
		ctx,
		fullyQualifiedTableName,
		manifests,
		endTime,
		security.RootUserName(),
		codec,
	)
	if err != nil {
		return errors.Wrapf(err, "fetching entry")
	}

	if err = showData(ctx, entry, endTime, codec); err != nil {
		return errors.Wrapf(err, "show data")
	}
	return nil
}

func evalAsOfTimestamp(readTime string) (hlc.Timestamp, error) {
	if readTime == "" {
		return hlc.Timestamp{}, nil
	}
	var err error
	// Attempt to parse as timestamp.
	if ts, _, err := pgdate.ParseTimestampWithoutTimezone(timeutil.Now(), pgdate.ParseModeYMD, readTime); err == nil {
		readTS := hlc.Timestamp{WallTime: ts.UnixNano()}
		return readTS, nil
	}
	// Attempt to parse as a decimal.
	if dec, _, err := apd.NewFromString(readTime); err == nil {
		if readTS, err := tree.DecimalToHLC(dec); err == nil {
			return readTS, nil
		}
	}
	err = errors.Newf("value %s is neither timestamp nor decimal", readTime)
	return hlc.Timestamp{}, err
}

func showData(
	ctx context.Context, entry backupccl.BackupTableEntry, endTime hlc.Timestamp, codec keys.SQLCodec,
) (err error) {

	iters, cleanup, err := makeIters(ctx, entry)
	if err != nil {
		return errors.Wrapf(err, "make iters")
	}
	defer func() {
		cleanupErr := cleanup()
		if err == nil {
			err = cleanupErr
		}
	}()

	iter := storage.MakeMultiIterator(iters)
	defer iter.Close()

	rf, err := makeRowFetcher(ctx, entry, codec)
	if err != nil {
		return errors.Wrapf(err, "make row fetcher")
	}
	defer rf.Close(ctx)

	startKeyMVCC, endKeyMVCC := storage.MVCCKey{Key: entry.Span.Key}, storage.MVCCKey{Key: entry.Span.EndKey}
	kvFetcher := row.MakeBackupSSTKVFetcher(startKeyMVCC, endKeyMVCC, iter, endTime)

	if err := rf.StartScanFrom(ctx, &kvFetcher); err != nil {
		return errors.Wrapf(err, "row fetcher starts scan")
	}

	var writer *csv.Writer
	if format != "csv" {
		return errors.Newf("only exporting to csv format is supported")
	}

	buf := bytes.NewBuffer([]byte{})
	if destination == "" {
		writer = csv.NewWriter(os.Stdout)
	} else {
		writer = csv.NewWriter(buf)
	}

	for {
		datums, _, _, err := rf.NextRowDecoded(ctx)
		if err != nil {
			return errors.Wrapf(err, "decode row")
		}
		if datums == nil {
			break
		}
		row := make([]string, datums.Len())
		for i, datum := range datums {
			if datum == tree.DNull {
				row[i] = nullas
			} else {
				row[i] = datum.String()
			}
		}
		if err := writer.Write(row); err != nil {
			return err
		}
		writer.Flush()
	}

	if destination != "" {
		dir, file := filepath.Split(destination)
		store, err := externalStorageFromURIFactory(ctx, dir, security.RootUserName())
		if err != nil {
			return errors.Wrapf(err, "unable to open store to write files: %s", destination)
		}
		if err = store.WriteFile(ctx, file, bytes.NewReader(buf.Bytes())); err != nil {
			_ = store.Close()
			return err
		}
		return store.Close()
	}
	return err
}

func makeIters(
	ctx context.Context, entry backupccl.BackupTableEntry,
) ([]storage.SimpleMVCCIterator, func() error, error) {
	iters := make([]storage.SimpleMVCCIterator, len(entry.Files))
	dirStorage := make([]cloud.ExternalStorage, len(entry.Files))
	for i, file := range entry.Files {
		var err error
		clusterSettings := cluster.MakeClusterSettings()
		dirStorage[i], err = cloudimpl.MakeExternalStorage(ctx, file.Dir, base.ExternalIODirConfig{},
			clusterSettings, newBlobFactory, nil /*internal executor*/, nil /*kvDB*/)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "making external storage")
		}

		iters[i], err = storageccl.ExternalSSTReader(ctx, dirStorage[i], file.Path, nil)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "fetching sst reader")
		}
	}

	cleanup := func() error {
		for _, iter := range iters {
			iter.Close()
		}
		for _, dir := range dirStorage {
			if err := dir.Close(); err != nil {
				return err
			}
		}
		return nil
	}
	return iters, cleanup, nil
}

func makeRowFetcher(
	ctx context.Context, entry backupccl.BackupTableEntry, codec keys.SQLCodec,
) (row.Fetcher, error) {
	var colIdxMap catalog.TableColMap
	var valNeededForCol util.FastIntSet
	colDescs := make([]descpb.ColumnDescriptor, len(entry.Desc.PublicColumns()))
	for i, col := range entry.Desc.PublicColumns() {
		colIdxMap.Set(col.GetID(), i)
		valNeededForCol.Add(i)
		colDescs[i] = *col.ColumnDesc()
	}
	table := row.FetcherTableArgs{
		Spans:            []roachpb.Span{entry.Span},
		Desc:             entry.Desc,
		Index:            entry.Desc.GetPrimaryIndex().IndexDesc(),
		ColIdxMap:        colIdxMap,
		IsSecondaryIndex: false,
		Cols:             colDescs,
		ValNeededForCol:  valNeededForCol,
	}

	var rf row.Fetcher
	if err := rf.Init(
		ctx,
		codec,
		false, /*reverse*/
		descpb.ScanLockingStrength_FOR_NONE,
		descpb.ScanLockingWaitPolicy_BLOCK,
		false, /*isCheck*/
		&rowenc.DatumAlloc{},
		nil, /*mon.BytesMonitor*/
		table,
	); err != nil {
		return rf, err
	}
	return rf, nil
}
