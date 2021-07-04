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
	gohex "encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

const (
	backupOptRevisionHistory = "revision_history"
)

type key struct {
	rawByte []byte
	typ     string
}

func (k *key) String() string {
	return string(k.rawByte)
}

func (k *key) Type() string {
	return k.typ
}

func (k *key) setType(v string) (string, error) {
	i := strings.IndexByte(v, ':')
	if i == -1 {
		return "", errors.Newf("no format specified in start key %s", v)
	}
	k.typ = v[:i]
	return v[i+1:], nil
}

func (k *key) Set(v string) error {
	v, err := k.setType(v)
	if err != nil {
		return err
	}
	switch k.typ {
	case "hex":
		b, err := gohex.DecodeString(v)
		if err != nil {
			return err
		}
		k.rawByte = b
	case "raw":
		s, err := strconv.Unquote(`"` + v + `"`)
		if err != nil {
			return errors.Wrapf(err, "invalid argument %q", v)
		}
		k.rawByte = []byte(s)
	case "bytekey":
		s, err := strconv.Unquote(`"` + v + `"`)
		if err != nil {
			return errors.Wrapf(err, "invalid argument %q", v)
		}
		k.rawByte = []byte(s)
	}
	return nil
}

// debugBackupArgs captures the parameters of the `debug backup` command.
var debugBackupArgs struct {
	externalIODir string

	exportTableName string
	readTime        string
	destination     string
	format          string
	nullas          string
	maxRows         int
	startKey        key
	withRevisions   bool

	rowCount int
}

// setDebugBackupArgsDefault set the default values in debugBackupArgs.
// This function is called in every test that exercises debug backup
// command-line parsing.
func setDebugContextDefault() {
	debugBackupArgs.externalIODir = ""
	debugBackupArgs.exportTableName = ""
	debugBackupArgs.readTime = ""
	debugBackupArgs.destination = ""
	debugBackupArgs.format = "csv"
	debugBackupArgs.nullas = "null"
	debugBackupArgs.maxRows = 0
	debugBackupArgs.startKey = key{}
	debugBackupArgs.rowCount = 0
	debugBackupArgs.withRevisions = false
}

func init() {

	showCmd := &cobra.Command{
		Use:   "show <backup_path>",
		Short: "show backup summary",
		Long:  "Shows summary of meta information about a SQL backup.",
		Args:  cobra.ExactArgs(1),
		RunE:  cli.MaybeDecorateGRPCError(runShowCmd),
	}

	listBackupsCmd := &cobra.Command{
		Use:   "list-backups <collection_path>",
		Short: "show backups in collection",
		Long:  "Shows full backup paths in a backup collection.",
		Args:  cobra.ExactArgs(1),
		RunE:  cli.MaybeDecorateGRPCError(runListBackupsCmd),
	}

	listIncrementalCmd := &cobra.Command{
		Use:   "list-incremental <backup_path>",
		Short: "show incremental backups",
		Long:  "Shows incremental chain of a SQL backup.",
		Args:  cobra.ExactArgs(1),
		RunE:  cli.MaybeDecorateGRPCError(runListIncrementalCmd),
	}

	exportDataCmd := &cobra.Command{
		Use:   "export <backup_path>",
		Short: "export table data from a backup",
		Long:  "export table data from a backup, requires specifying --table to export data from",
		Args:  cobra.MinimumNArgs(1),
		RunE:  cli.MaybeDecorateGRPCError(runExportDataCmd),
	}

	backupCmds := &cobra.Command{
		Use:   "backup [command]",
		Short: "debug backups",
		Long:  "Shows information about a SQL backup.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}

	backupFlags := backupCmds.Flags()
	backupFlags.StringVarP(
		&debugBackupArgs.externalIODir,
		cliflags.ExternalIODir.Name,
		cliflags.ExternalIODir.Shorthand,
		"", /*value*/
		cliflags.ExternalIODir.Usage())

	exportDataCmd.Flags().StringVarP(
		&debugBackupArgs.exportTableName,
		cliflags.ExportTableTarget.Name,
		cliflags.ExportTableTarget.Shorthand,
		"", /*value*/
		cliflags.ExportTableTarget.Usage())

	exportDataCmd.Flags().StringVarP(
		&debugBackupArgs.readTime,
		cliflags.ReadTime.Name,
		cliflags.ReadTime.Shorthand,
		"", /*value*/
		cliflags.ReadTime.Usage())

	exportDataCmd.Flags().StringVarP(
		&debugBackupArgs.destination,
		cliflags.ExportDestination.Name,
		cliflags.ExportDestination.Shorthand,
		"", /*value*/
		cliflags.ExportDestination.Usage())

	exportDataCmd.Flags().StringVarP(
		&debugBackupArgs.format,
		cliflags.ExportTableFormat.Name,
		cliflags.ExportTableFormat.Shorthand,
		"csv", /*value*/
		cliflags.ExportTableFormat.Usage())

	exportDataCmd.Flags().StringVarP(
		&debugBackupArgs.nullas,
		cliflags.ExportCSVNullas.Name,
		cliflags.ExportCSVNullas.Shorthand,
		"null", /*value*/
		cliflags.ExportCSVNullas.Usage())

	exportDataCmd.Flags().IntVar(
		&debugBackupArgs.maxRows,
		cliflags.MaxRows.Name,
		0,
		cliflags.MaxRows.Usage())

	exportDataCmd.Flags().Var(
		&debugBackupArgs.startKey,
		cliflags.StartKey.Name,
		cliflags.StartKey.Usage())

	exportDataCmd.Flags().BoolVar(
		&debugBackupArgs.withRevisions,
		cliflags.ExportRevisions.Name,
		false, /*value*/
		cliflags.ExportRevisions.Usage())

	exportDataCmd.Flags().StringVarP(
		&debugBackupArgs.readTime,
		cliflags.ExportRevisionsUpTo.Name,
		cliflags.ExportRevisionsUpTo.Shorthand,
		"", /*value*/
		cliflags.ExportRevisionsUpTo.Usage())

	backupSubCmds := []*cobra.Command{
		showCmd,
		listBackupsCmd,
		listIncrementalCmd,
		exportDataCmd,
	}

	for _, cmd := range backupSubCmds {
		backupCmds.AddCommand(cmd)
		cmd.Flags().AddFlagSet(backupFlags)
	}
	cli.DebugCmd.AddCommand(backupCmds)
}

func newBlobFactory(ctx context.Context, dialing roachpb.NodeID) (blobs.BlobClient, error) {
	if dialing != 0 {
		return nil, errors.Errorf("accessing node %d during nodelocal access is unsupported for CLI inspection; only local access is supported with nodelocal://self", dialing)
	}
	if debugBackupArgs.externalIODir == "" {
		debugBackupArgs.externalIODir = filepath.Join(server.DefaultStorePath, "extern")
	}
	return blobs.NewLocalClient(debugBackupArgs.externalIODir)
}

func externalStorageFromURIFactory(
	ctx context.Context, uri string, user security.SQLUsername,
) (cloud.ExternalStorage, error) {
	return cloud.ExternalStorageFromURI(ctx, uri, base.ExternalIODirConfig{},
		cluster.NoSettings, newBlobFactory, user, nil /*Internal Executor*/, nil /*kvDB*/)
}

func getManifestFromURI(ctx context.Context, path string) (backupccl.BackupManifest, error) {

	if !strings.Contains(path, "://") {
		path = nodelocal.MakeLocalStorageURI(path)
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

func runShowCmd(cmd *cobra.Command, args []string) error {

	path := args[0]
	ctx := context.Background()
	desc, err := getManifestFromURI(ctx, path)
	if err != nil {
		return errors.Wrapf(err, "fetching backup manifest")
	}

	var meta = backupMetaDisplayMsg(desc)
	jsonBytes, err := json.MarshalIndent(meta, "" /*prefix*/, "\t" /*indent*/)
	if err != nil {
		return errors.Wrapf(err, "marshall backup manifest")
	}
	s := string(jsonBytes)
	fmt.Println(s)
	return nil
}

func runListBackupsCmd(cmd *cobra.Command, args []string) error {

	path := args[0]
	if !strings.Contains(path, "://") {
		path = nodelocal.MakeLocalStorageURI(path)
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

	cols := []string{"path"}
	rows := make([][]string, 0)
	for _, backupPath := range backupPaths {
		rows = append(rows, []string{"." + backupPath})
	}
	rowSliceIter := cli.NewRowSliceIter(rows, "l" /*align*/)
	return cli.PrintQueryOutput(os.Stdout, cols, rowSliceIter)
}

func runListIncrementalCmd(cmd *cobra.Command, args []string) error {

	path := args[0]
	if !strings.Contains(path, "://") {
		path = nodelocal.MakeLocalStorageURI(path)
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

	incPaths, err := backupccl.FindPriorBackups(ctx, store, backupccl.OmitManifest)
	if err != nil {
		return err
	}

	basepath := uri.Path
	manifestPaths := append([]string{""}, incPaths...)
	stores := make([]cloud.ExternalStorage, len(manifestPaths))
	stores[0] = store

	rows := make([][]string, 0)
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
		newRow := []string{uri.Path, startTime, endTime}
		rows = append(rows, newRow)
	}
	cols := []string{"path", "start time", "end time"}
	rowSliceIter := cli.NewRowSliceIter(rows, "lll" /*align*/)
	return cli.PrintQueryOutput(os.Stdout, cols, rowSliceIter)
}

func runExportDataCmd(cmd *cobra.Command, args []string) error {
	if debugBackupArgs.exportTableName == "" {
		return errors.New("export data requires table name specified by --table flag")
	}
	fullyQualifiedTableName := strings.ToLower(debugBackupArgs.exportTableName)
	manifestPaths := args

	ctx := context.Background()
	manifests := make([]backupccl.BackupManifest, 0, len(manifestPaths))
	for _, path := range manifestPaths {
		manifest, err := getManifestFromURI(ctx, path)
		if err != nil {
			return errors.Wrapf(err, "fetching backup manifests from %s", path)
		}
		manifests = append(manifests, manifest)
	}

	if debugBackupArgs.withRevisions && manifests[0].MVCCFilter != backupccl.MVCCFilter_All {
		return errors.WithHintf(
			errors.Newf("invalid flag: %s", cliflags.ExportRevisions.Name),
			"requires backup created with %q", backupOptRevisionHistory,
		)
	}

	endTime, err := evalAsOfTimestamp(debugBackupArgs.readTime, manifests)
	if err != nil {
		return errors.Wrapf(err, "eval as of timestamp %s", debugBackupArgs.readTime)
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

func evalAsOfTimestamp(
	readTime string, manifests []backupccl.BackupManifest,
) (hlc.Timestamp, error) {
	if readTime == "" {
		return manifests[len(manifests)-1].EndTime, nil
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
) error {

	buf := bytes.NewBuffer([]byte{})
	var writer *csv.Writer
	if debugBackupArgs.format != "csv" {
		return errors.Newf("only exporting to csv format is supported")
	}
	if debugBackupArgs.destination == "" {
		writer = csv.NewWriter(os.Stdout)
	} else {
		writer = csv.NewWriter(buf)
	}

	rf, err := makeRowFetcher(ctx, entry, codec)
	if err != nil {
		return errors.Wrapf(err, "make row fetcher")
	}
	defer rf.Close(ctx)

	if debugBackupArgs.withRevisions {
		startT := entry.LastSchemaChangeTime.GoTime().UTC()
		endT := endTime.GoTime().UTC()
		fmt.Fprintf(os.Stderr, "DETECTED SCHEMA CHANGE AT %s, ONLY SHOWING UPDATES IN RANGE [%s, %s]\n", startT, startT, endT)
	}

	for _, files := range entry.Files {
		if err := processEntryFiles(ctx, rf, files, entry.Span, entry.LastSchemaChangeTime, endTime, writer); err != nil {
			return err
		}
		if debugBackupArgs.maxRows != 0 && debugBackupArgs.rowCount >= debugBackupArgs.maxRows {
			break
		}
	}

	if debugBackupArgs.destination != "" {
		dir, file := filepath.Split(debugBackupArgs.destination)
		store, err := externalStorageFromURIFactory(ctx, dir, security.RootUserName())
		if err != nil {
			return errors.Wrapf(err, "unable to open store to write files: %s", debugBackupArgs.destination)
		}
		if err = cloud.WriteFile(ctx, store, file, bytes.NewReader(buf.Bytes())); err != nil {
			_ = store.Close()
			return err
		}
		return store.Close()
	}
	return nil
}

func makeIters(
	ctx context.Context, files backupccl.EntryFiles,
) ([]storage.SimpleMVCCIterator, func() error, error) {
	iters := make([]storage.SimpleMVCCIterator, len(files))
	dirStorage := make([]cloud.ExternalStorage, len(files))
	for i, file := range files {
		var err error
		clusterSettings := cluster.MakeClusterSettings()
		dirStorage[i], err = cloud.MakeExternalStorage(ctx, file.Dir, base.ExternalIODirConfig{},
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
	colDescs := make([]catalog.Column, len(entry.Desc.PublicColumns()))
	for i, col := range entry.Desc.PublicColumns() {
		colIdxMap.Set(col.GetID(), i)
		valNeededForCol.Add(i)
		colDescs[i] = col
	}

	if debugBackupArgs.withRevisions {
		newIndex := len(entry.Desc.PublicColumns())
		newCol, err := entry.Desc.FindColumnWithName(colinfo.MVCCTimestampColumnName)
		if err != nil {
			return row.Fetcher{}, errors.Wrapf(err, "get mvcc timestamp column")
		}
		colIdxMap.Set(newCol.GetID(), newIndex)
		valNeededForCol.Add(newIndex)
		colDescs = append(colDescs, newCol)
	}

	table := row.FetcherTableArgs{
		Spans:            []roachpb.Span{entry.Span},
		Desc:             entry.Desc,
		Index:            entry.Desc.GetPrimaryIndex(),
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

func processEntryFiles(
	ctx context.Context,
	rf row.Fetcher,
	files backupccl.EntryFiles,
	span roachpb.Span,
	startTime hlc.Timestamp,
	endTime hlc.Timestamp,
	writer *csv.Writer,
) (err error) {

	iters, cleanup, err := makeIters(ctx, files)
	defer func() {
		if cleanupErr := cleanup(); err == nil {
			err = cleanupErr
		}
	}()
	if err != nil {
		return errors.Wrapf(err, "make iters")
	}

	iter := storage.MakeMultiIterator(iters)
	defer iter.Close()

	startKeyMVCC, endKeyMVCC := storage.MVCCKey{Key: span.Key}, storage.MVCCKey{Key: span.EndKey}
	if len(debugBackupArgs.startKey.rawByte) != 0 {
		if debugBackupArgs.startKey.typ == "bytekey" {
			startKeyMVCC.Key = append(startKeyMVCC.Key, debugBackupArgs.startKey.rawByte...)
		} else {
			startKeyMVCC.Key = roachpb.Key(debugBackupArgs.startKey.rawByte)
		}
	}
	kvFetcher := row.MakeBackupSSTKVFetcher(startKeyMVCC, endKeyMVCC, iter, startTime, endTime, debugBackupArgs.withRevisions)

	if err := rf.StartScanFrom(ctx, &kvFetcher); err != nil {
		return errors.Wrapf(err, "row fetcher starts scan")
	}

	for {
		datums, _, _, err := rf.NextRowDecoded(ctx)
		if err != nil {
			return errors.Wrapf(err, "decode row")
		}
		if datums == nil {
			break
		}
		rowDisplay := make([]string, datums.Len())
		for i, datum := range datums {

			if debugBackupArgs.withRevisions && i == datums.Len()-1 {
				approx, err := tree.DecimalToInexactDTimestamp(datum.(*tree.DDecimal))
				if err != nil {
					return errors.Wrapf(err, "convert datum %s to mvcc timestamp", datum)
				}
				rowDisplay[i] = approx.UTC().String()
				break
			}

			if datum == tree.DNull {
				rowDisplay[i] = debugBackupArgs.nullas
			} else {
				rowDisplay[i] = datum.String()
			}
		}
		if err := writer.Write(rowDisplay); err != nil {
			return err
		}
		writer.Flush()

		if debugBackupArgs.maxRows != 0 {
			debugBackupArgs.rowCount++
			if debugBackupArgs.rowCount >= debugBackupArgs.maxRows {
				break
			}
		}
	}
	return nil
}

type backupMetaDisplayMsg backupccl.BackupManifest
type backupFileDisplayMsg backupccl.BackupManifest_File

func (f backupFileDisplayMsg) MarshalJSON() ([]byte, error) {
	fileDisplayMsg := struct {
		Path         string
		Span         string
		DataSize     string
		IndexEntries int64
		Rows         int64
	}{
		Path:         f.Path,
		Span:         fmt.Sprint(f.Span),
		DataSize:     humanizeutil.IBytes(f.EntryCounts.DataSize),
		IndexEntries: f.EntryCounts.IndexEntries,
		Rows:         f.EntryCounts.Rows,
	}
	return json.Marshal(fileDisplayMsg)
}

func (b backupMetaDisplayMsg) MarshalJSON() ([]byte, error) {

	fileMsg := make([]backupFileDisplayMsg, len(b.Files))
	for i, file := range b.Files {
		fileMsg[i] = backupFileDisplayMsg(file)
	}

	displayMsg := struct {
		StartTime           string
		EndTime             string
		DataSize            string
		Rows                int64
		IndexEntries        int64
		FormatVersion       uint32
		ClusterID           uuid.UUID
		NodeID              roachpb.NodeID
		BuildInfo           string
		Files               []backupFileDisplayMsg
		Spans               string
		DatabaseDescriptors map[descpb.ID]string
		TableDescriptors    map[descpb.ID]string
		TypeDescriptors     map[descpb.ID]string
		SchemaDescriptors   map[descpb.ID]string
	}{
		StartTime:           timeutil.Unix(0, b.StartTime.WallTime).Format(time.RFC3339),
		EndTime:             timeutil.Unix(0, b.EndTime.WallTime).Format(time.RFC3339),
		DataSize:            humanizeutil.IBytes(b.EntryCounts.DataSize),
		Rows:                b.EntryCounts.Rows,
		IndexEntries:        b.EntryCounts.IndexEntries,
		FormatVersion:       b.FormatVersion,
		ClusterID:           b.ClusterID,
		NodeID:              b.NodeID,
		BuildInfo:           b.BuildInfo.Short(),
		Files:               fileMsg,
		Spans:               fmt.Sprint(b.Spans),
		DatabaseDescriptors: make(map[descpb.ID]string),
		TableDescriptors:    make(map[descpb.ID]string),
		TypeDescriptors:     make(map[descpb.ID]string),
		SchemaDescriptors:   make(map[descpb.ID]string),
	}

	dbIDToName := make(map[descpb.ID]string)
	schemaIDToFullyQualifiedName := make(map[descpb.ID]string)
	schemaIDToFullyQualifiedName[keys.PublicSchemaID] = catconstants.PublicSchemaName
	typeIDToFullyQualifiedName := make(map[descpb.ID]string)
	tableIDToFullyQualifiedName := make(map[descpb.ID]string)

	for i := range b.Descriptors {
		d := &b.Descriptors[i]
		id := descpb.GetDescriptorID(d)
		tableDesc, databaseDesc, typeDesc, schemaDesc := descpb.FromDescriptor(d)
		if databaseDesc != nil {
			dbIDToName[id] = descpb.GetDescriptorName(d)
		} else if schemaDesc != nil {
			dbName := dbIDToName[schemaDesc.GetParentID()]
			schemaName := descpb.GetDescriptorName(d)
			schemaIDToFullyQualifiedName[id] = dbName + "." + schemaName
		} else if typeDesc != nil {
			parentSchema := schemaIDToFullyQualifiedName[typeDesc.GetParentSchemaID()]
			if parentSchema == catconstants.PublicSchemaName {
				parentSchema = dbIDToName[typeDesc.GetParentID()] + "." + parentSchema
			}
			typeName := descpb.GetDescriptorName(d)
			typeIDToFullyQualifiedName[id] = parentSchema + "." + typeName
		} else if tableDesc != nil {
			tbDesc := tabledesc.NewBuilder(tableDesc).BuildImmutable()
			parentSchema := schemaIDToFullyQualifiedName[tbDesc.GetParentSchemaID()]
			if parentSchema == catconstants.PublicSchemaName {
				parentSchema = dbIDToName[tableDesc.GetParentID()] + "." + parentSchema
			}
			tableName := descpb.GetDescriptorName(d)
			tableIDToFullyQualifiedName[id] = parentSchema + "." + tableName
		}
	}
	displayMsg.DatabaseDescriptors = dbIDToName
	displayMsg.TableDescriptors = tableIDToFullyQualifiedName
	displayMsg.SchemaDescriptors = schemaIDToFullyQualifiedName
	displayMsg.TypeDescriptors = typeIDToFullyQualifiedName

	return json.Marshal(displayMsg)
}
