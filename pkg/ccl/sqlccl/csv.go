// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package sqlccl

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

const (
	importOptionDelimiter     = "delimiter"
	importOptionComment       = "comment"
	importOptionDistributed   = "distributed"
	importOptionNullIf        = "nullif"
	importOptionTransformOnly = "transform_only"
	importOptionSSTSize       = "sstsize"
	importOptionTemp          = "temp"
)

var importOptionExpectValues = map[string]bool{
	importOptionDelimiter:     true,
	importOptionComment:       true,
	importOptionDistributed:   false,
	importOptionNullIf:        true,
	importOptionTransformOnly: false,
	importOptionSSTSize:       true,
	importOptionTemp:          true,
}

// LoadCSV converts CSV files into enterprise backup format.
func LoadCSV(
	ctx context.Context,
	table string,
	dataFiles []string,
	dest string,
	comma, comment rune,
	nullif *string,
	sstMaxSize int64,
	tempDir string,
) (csvCount, kvCount, sstCount int64, err error) {
	if table == "" {
		return 0, 0, 0, errors.New("no table specified")
	}
	table, err = storageccl.MakeLocalStorageURI(table)
	if err != nil {
		return 0, 0, 0, err
	}
	if dest == "" {
		return 0, 0, 0, errors.New("no destination specified")
	}
	if len(dataFiles) == 0 {
		dataFiles = []string{fmt.Sprintf("%s.dat", table)}
	}
	createTable, err := readCreateTableFromStore(ctx, table)
	if err != nil {
		return 0, 0, 0, err
	}

	var parentID = defaultCSVParentID
	walltime := timeutil.Now().UnixNano()

	tableDesc, err := makeCSVTableDescriptor(ctx, createTable, parentID, defaultCSVTableID, walltime)
	if err != nil {
		return 0, 0, 0, err
	}

	for i, f := range dataFiles {
		dataFiles[i], err = storageccl.MakeLocalStorageURI(f)
		if err != nil {
			return 0, 0, 0, err
		}
	}
	// TODO(mjibson): allow users to optionally specify a full URI to an export store.
	dest, err = storageccl.MakeLocalStorageURI(dest)
	if err != nil {
		return 0, 0, 0, err
	}

	rocksdbDir, err := ioutil.TempDir(tempDir, "cockroach-csv-rocksdb")
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() {
		if err := os.RemoveAll(rocksdbDir); err != nil {
			log.Infof(ctx, "could not remove temp directory %s: %s", rocksdbDir, err)
		}
	}()

	fileLimit, err := server.SetOpenFileLimitForOneStore()
	if err != nil {
		return 0, 0, 0, err
	}

	cache := engine.NewRocksDBCache(0)
	defer cache.Release()
	r, err := engine.NewRocksDB(engine.RocksDBConfig{
		Settings:     cluster.MakeTestingClusterSettings(),
		Dir:          rocksdbDir,
		MaxSizeBytes: 0,
		MaxOpenFiles: fileLimit,
	}, cache)
	if err != nil {
		return 0, 0, 0, errors.Wrap(err, "create rocksdb instance")
	}
	defer r.Close()

	return doLocalCSVTransform(
		ctx, nil, parentID, tableDesc, dest, dataFiles, comma, comment, nullif, sstMaxSize, r, walltime, nil,
	)
}

func doLocalCSVTransform(
	ctx context.Context,
	job *jobs.Job,
	parentID sqlbase.ID,
	tableDesc *sqlbase.TableDescriptor,
	dest string,
	dataFiles []string,
	comma, comment rune,
	nullif *string,
	sstMaxSize int64,
	tempEngine engine.Engine,
	walltime int64,
	execCfg *sql.ExecutorConfig,
) (csvCount, kvCount, sstCount int64, err error) {
	// Some channels are buffered because reads happen in bursts, so having lots
	// of pre-computed data improves overall performance.
	const chanSize = 10000

	recordCh := make(chan csvRecord, chanSize)
	kvCh := make(chan roachpb.KeyValue, chanSize)
	contentCh := make(chan sstContent)
	var backupDesc *BackupDescriptor
	conf, err := storageccl.ExportStorageConfFromURI(dest)
	if err != nil {
		return 0, 0, 0, err
	}
	es, err := storageccl.MakeExportStorage(ctx, conf)
	if err != nil {
		return 0, 0, 0, err
	}
	defer es.Close()

	group, gCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer close(recordCh)
		var err error
		csvCount, err = readCSV(gCtx, comma, comment, len(tableDesc.VisibleColumns()), dataFiles, recordCh)
		if job != nil {
			if err := job.Progressed(ctx, 1.0/3.0, jobs.Noop); err != nil {
				log.Warningf(ctx, "failed to update job progress: %s", err)
			}
		}
		return err
	})
	group.Go(func() error {
		defer close(kvCh)
		return groupWorkers(gCtx, runtime.NumCPU(), func(ctx context.Context) error {
			return convertRecord(ctx, recordCh, kvCh, nullif, tableDesc)
		})
	})
	group.Go(func() error {
		defer close(contentCh)
		var err error
		kvCount, err = writeRocksDB(gCtx, kvCh, tempEngine, sstMaxSize, contentCh, walltime)
		if job != nil {
			if err := job.Progressed(ctx, 2.0/3.0, jobs.Noop); err != nil {
				log.Warningf(ctx, "failed to update job progress: %s", err)
			}
		}
		return err
	})
	group.Go(func() error {
		var err error
		backupDesc, err = makeBackup(gCtx, contentCh, walltime, es)
		return err
	})
	if err := group.Wait(); err != nil {
		return 0, 0, 0, err
	}
	err = finalizeCSVBackup(ctx, backupDesc, parentID, tableDesc, es, execCfg)
	sstCount = int64(len(backupDesc.Files))

	return csvCount, kvCount, sstCount, err
}

const (
	// We need to choose arbitrary database and table IDs. These aren't important,
	// but they do match what would happen when creating a new database and
	// table on an empty cluster.
	defaultCSVParentID sqlbase.ID = keys.MaxReservedDescID + 1
	defaultCSVTableID  sqlbase.ID = defaultCSVParentID + 1
)

func readCreateTableFromStore(ctx context.Context, filename string) (*parser.CreateTable, error) {
	store, err := exportStorageFromURI(ctx, filename)
	if err != nil {
		return nil, err
	}
	defer store.Close()
	reader, err := store.ReadFile(ctx, "")
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	tableDefStr, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	stmt, err := parser.ParseOne(string(tableDefStr))
	if err != nil {
		return nil, err
	}
	create, ok := stmt.(*parser.CreateTable)
	if !ok {
		return nil, errors.New("expected CREATE TABLE statement in table file")
	}
	return create, nil
}

func makeCSVTableDescriptor(
	ctx context.Context, create *parser.CreateTable, parentID, tableID sqlbase.ID, walltime int64,
) (*sqlbase.TableDescriptor, error) {
	sql.HoistConstraints(create)
	if create.IfNotExists {
		return nil, errors.New("unsupported IF NOT EXISTS")
	}
	if create.Interleave != nil {
		return nil, errors.New("interleaved not supported")
	}
	if create.AsSource != nil {
		return nil, errors.New("CREATE AS not supported")
	}
	for _, def := range create.Defs {
		switch def := def.(type) {
		case *parser.CheckConstraintTableDef,
			*parser.FamilyTableDef,
			*parser.IndexTableDef,
			*parser.UniqueConstraintTableDef:
			// ignore
		case *parser.ColumnTableDef:
			if def.DefaultExpr.Expr != nil {
				return nil, errors.Errorf("DEFAULT expressions not supported: %s", parser.AsString(def))
			}
		case *parser.ForeignKeyConstraintTableDef:
			return nil, errors.Errorf("foreign keys not supported: %s", parser.AsString(def))
		default:
			return nil, errors.Errorf("unsupported table definition: %s", parser.AsString(def))
		}
	}
	tableDesc, err := sql.MakeTableDesc(
		ctx,
		nil, /* txn */
		sql.NilVirtualTabler,
		nil, /* SearchPath */
		create,
		parentID,
		tableID,
		hlc.Timestamp{WallTime: walltime},
		sqlbase.NewDefaultPrivilegeDescriptor(),
		nil, /* affected */
		"",  /* sessionDB */
		nil, /* EvalContext */
	)
	if err != nil {
		return nil, err
	}

	return &tableDesc, nil
}

// groupWorkers creates num worker go routines in an error group.
func groupWorkers(ctx context.Context, num int, f func(context.Context) error) error {
	group, ctx := errgroup.WithContext(ctx)
	for i := 0; i < num; i++ {
		group.Go(func() error {
			return f(ctx)
		})
	}
	return group.Wait()
}

// readCSV sends records on ch from CSV listed by dataFiles. comma, if
// non-zero, specifies the field separator. comment, if non-zero, specifies
// the comment character. It returns the number of rows read.
func readCSV(
	ctx context.Context,
	comma, comment rune,
	expectedCols int,
	dataFiles []string,
	recordCh chan<- csvRecord,
) (int64, error) {
	expectedColsExtra := expectedCols + 1
	done := ctx.Done()
	var count int64
	if comma == 0 {
		comma = ','
	}
	for _, dataFile := range dataFiles {
		select {
		case <-done:
			return 0, ctx.Err()
		default:
		}
		err := func() error {
			conf, err := storageccl.ExportStorageConfFromURI(dataFile)
			if err != nil {
				return err
			}
			es, err := storageccl.MakeExportStorage(ctx, conf)
			if err != nil {
				return err
			}
			defer es.Close()
			f, err := es.ReadFile(ctx, "")
			if err != nil {
				return err
			}
			cr := csv.NewReader(f)
			cr.Comma = comma
			cr.FieldsPerRecord = -1
			cr.LazyQuotes = true
			cr.Comment = comment
			for i := 1; ; i++ {
				record, err := cr.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					return errors.Wrapf(err, "row %d: reading CSV record", i)
				}
				if len(record) == expectedCols {
					// Expected number of columns.
				} else if len(record) == expectedColsExtra && record[expectedCols] == "" {
					// Line has the optional trailing comma, ignore the empty field.
					record = record[:expectedCols]
				} else {
					return errors.Errorf("row %d: expected %d fields, got %d", i, expectedCols, len(record))
				}
				cr := csvRecord{
					r:    record,
					file: dataFile,
					row:  i,
				}
				select {
				case <-done:
					return ctx.Err()
				case recordCh <- cr:
					count++
				}
			}
			return nil
		}()
		if err != nil {
			return 0, errors.Wrap(err, dataFile)
		}
	}
	return count, nil
}

type csvRecord struct {
	r    []string
	file string
	row  int
}

// convertRecord converts CSV records KV pairs and sends them on the kvCh chan.
func convertRecord(
	ctx context.Context,
	recordCh <-chan csvRecord,
	kvCh chan<- roachpb.KeyValue,
	nullif *string,
	tableDesc *sqlbase.TableDescriptor,
) error {
	done := ctx.Done()

	visibleCols := tableDesc.VisibleColumns()
	keyDatums := make(sqlbase.EncDatumRow, len(tableDesc.PrimaryIndex.ColumnIDs))
	// keyDatumIdx maps ColumnIDs to indexes in keyDatums.
	keyDatumIdx := make(map[sqlbase.ColumnID]int)
	for _, id := range tableDesc.PrimaryIndex.ColumnIDs {
		for _, col := range visibleCols {
			if col.ID == id {
				keyDatumIdx[id] = len(keyDatumIdx)
				break
			}
		}
	}

	ri, err := sqlbase.MakeRowInserter(nil /* txn */, tableDesc, nil, /* fkTables */
		tableDesc.Columns, false /* checkFKs */, &sqlbase.DatumAlloc{})
	if err != nil {
		return errors.Wrap(err, "make row inserter")
	}

	parse := parser.Parser{}
	evalCtx := parser.EvalContext{}
	// Although we don't yet support DEFAULT expressions on visible columns,
	// we do on hidden columns (which is only the default _rowid one). This
	// allows those expressions to run.
	cols, defaultExprs, err := sqlbase.ProcessDefaultColumns(tableDesc.Columns, tableDesc, &parse, &evalCtx)
	if err != nil {
		return errors.Wrap(err, "process default columns")
	}

	datums := make([]parser.Datum, len(visibleCols))
	var insertErr error
	for record := range recordCh {
		for i, r := range record.r {
			if nullif != nil && r == *nullif {
				datums[i] = parser.DNull
			} else {
				datums[i], err = parser.ParseStringAs(visibleCols[i].Type.ToDatumType(), r, time.UTC)
				if err != nil {
					return errors.Wrapf(err, "%s: row %d: parse %q as %s", record.file, record.row, visibleCols[i].Name, visibleCols[i].Type.SQLString())
				}
			}
			if idx, ok := keyDatumIdx[visibleCols[i].ID]; ok {
				keyDatums[idx] = sqlbase.DatumToEncDatum(visibleCols[i].Type, datums[i])
			}
		}

		row, err := sql.GenerateInsertRow(defaultExprs, ri.InsertColIDtoRowIndex, cols, evalCtx, tableDesc, datums)
		if err != nil {
			return errors.Wrapf(err, "generate insert row: %s: row %d", record.file, record.row)
		}
		insertErr = nil
		if err := ri.InsertRow(ctx, inserter(func(kv roachpb.KeyValue) {
			select {
			case kvCh <- kv:
			case <-done:
				insertErr = ctx.Err()
			}
		}), row, true /* ignoreConflicts */, false /* traceKV */); err != nil {
			return errors.Wrapf(err, "insert row: %s: row %d", record.file, record.row)
		}
		if insertErr != nil {
			return insertErr
		}
	}
	return nil
}

type sstContent struct {
	data []byte
	size int64
	span roachpb.Span
}

const errSSTCreationMaybeDuplicateTemplate = "SST creation error at %s; this can happen when a primary or unique index has duplicate keys"

// writeRocksDB writes kvs to a RocksDB instance that is created at
// rocksdbDir. After kvs is closed, sst files are created of size maxSize
// and sent on contents. It returns the number of KV pairs created.
func writeRocksDB(
	ctx context.Context,
	kvCh <-chan roachpb.KeyValue,
	tempEngine engine.Engine,
	sstMaxSize int64,
	contentCh chan<- sstContent,
	walltime int64,
) (int64, error) {
	store := engine.NewRocksDBMultiMap(tempEngine)
	writer := store.NewBatchWriter()
	for kv := range kvCh {
		if err := writer.Put(kv.Key, kv.Value.RawBytes); err != nil {
			return 0, err
		}
	}
	if err := writer.Close(ctx); err != nil {
		return 0, err
	}
	it := store.NewIterator()
	defer it.Close()
	sst, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		return 0, err
	}
	defer sst.Close()

	writeSST := func(key, endKey roachpb.Key) error {
		data, err := sst.Finish()
		if err != nil {
			return err
		}
		sc := sstContent{
			data: data,
			size: sst.DataSize,
			span: roachpb.Span{
				Key:    key,
				EndKey: endKey,
			},
		}
		select {
		case contentCh <- sc:
		case <-ctx.Done():
			return ctx.Err()
		}
		sst.Close()
		return nil
	}

	var kv engine.MVCCKeyValue
	kv.Key.Timestamp.WallTime = walltime
	// firstKey is always the first key of the span. lastKey, if nil, means the
	// current SST hasn't yet filled up. Once the SST has filled up, lastKey is
	// set to the key at which to stop adding KVs. We have to do this because
	// all column families for a row must be in one SST and the SST may have
	// filled up with only some of the KVs from the column families being added.
	var firstKey, lastKey roachpb.Key
	var count int64

	it.Rewind()
	if ok, err := it.Valid(); err != nil {
		return 0, err
	} else if !ok {
		return 0, errors.New("could not get first key")
	}
	firstKey = it.Key()

	for ; ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			return 0, err
		} else if !ok {
			break
		}
		count++

		kv.Key.Key = it.UnsafeKey()
		kv.Value = it.UnsafeValue()
		if lastKey != nil {
			if kv.Key.Key.Compare(lastKey) >= 0 {
				if err := writeSST(firstKey, lastKey); err != nil {
					return 0, err
				}
				firstKey = it.Key()
				lastKey = nil

				sst, err = engine.MakeRocksDBSstFileWriter()
				if err != nil {
					return 0, err
				}
				defer sst.Close()
			}
		}
		if err := sst.Add(kv); err != nil {
			return 0, errors.Wrapf(err, errSSTCreationMaybeDuplicateTemplate, kv.Key.Key)
		}
		if sst.DataSize > sstMaxSize && lastKey == nil {
			// When we would like to split the file, proceed until we aren't in the
			// middle of a row. Start by finding the next safe split key.
			lastKey, err = keys.EnsureSafeSplitKey(kv.Key.Key)
			if err != nil {
				return 0, err
			}
			lastKey = lastKey.PrefixEnd()
		}
	}
	if sst.DataSize > 0 {
		if err := writeSST(firstKey, kv.Key.Key.Next()); err != nil {
			return 0, err
		}
	}
	return count, nil
}

// makeBackup writes SST files from contents to es and creates a backup
// descriptor populated with the written SST files.
func makeBackup(
	ctx context.Context, contentCh <-chan sstContent, walltime int64, es storageccl.ExportStorage,
) (*BackupDescriptor, error) {
	backupDesc := BackupDescriptor{
		FormatVersion: BackupFormatInitialVersion,
		EndTime:       hlc.Timestamp{WallTime: walltime},
	}
	i := 0
	for sst := range contentCh {
		backupDesc.EntryCounts.DataSize += sst.size
		checksum, err := storageccl.SHA512ChecksumData(sst.data)
		if err != nil {
			return nil, err
		}
		i++
		name := fmt.Sprintf("%d.sst", i)
		if err := es.WriteFile(ctx, name, bytes.NewReader(sst.data)); err != nil {
			return nil, err
		}

		backupDesc.Files = append(backupDesc.Files, BackupDescriptor_File{
			Path:   name,
			Span:   sst.span,
			Sha512: checksum,
		})
	}
	return &backupDesc, nil
}

const csvDatabaseName = "csv"

func finalizeCSVBackup(
	ctx context.Context,
	backupDesc *BackupDescriptor,
	parentID sqlbase.ID,
	tableDesc *sqlbase.TableDescriptor,
	es storageccl.ExportStorage,
	execCfg *sql.ExecutorConfig,
) error {
	if len(backupDesc.Files) == 0 {
		return errors.New("no files in backup")
	}

	sort.Sort(backupFileDescriptors(backupDesc.Files))
	backupDesc.Spans = []roachpb.Span{tableDesc.TableSpan()}
	backupDesc.Descriptors = []sqlbase.Descriptor{
		*sqlbase.WrapDescriptor(&sqlbase.DatabaseDescriptor{
			Name: csvDatabaseName,
			ID:   parentID,
		}),
		*sqlbase.WrapDescriptor(tableDesc),
	}
	backupDesc.FormatVersion = BackupFormatInitialVersion
	backupDesc.BuildInfo = build.GetInfo()
	if execCfg != nil {
		backupDesc.NodeID = execCfg.NodeID.Get()
		backupDesc.ClusterID = execCfg.ClusterID()
	}
	descBuf, err := backupDesc.Marshal()
	if err != nil {
		return err
	}
	return es.WriteFile(ctx, BackupDescriptorName, bytes.NewReader(descBuf))
}

func importJobDescription(
	orig *parser.Import, defs parser.TableDefs, files []string, opts map[string]string,
) (string, error) {
	stmt := *orig
	stmt.CreateFile = nil
	stmt.CreateDefs = defs
	stmt.Files = nil
	for _, file := range files {
		clean, err := storageccl.SanitizeExportStorageURI(file)
		if err != nil {
			return "", err
		}
		stmt.Files = append(stmt.Files, parser.NewDString(clean))
	}
	stmt.Options = nil
	hasTransformOnly := false
	for k, v := range opts {
		if k == importOptionTemp {
			clean, err := storageccl.SanitizeExportStorageURI(v)
			if err != nil {
				return "", err
			}
			v = clean
		}
		if k == importOptionTransformOnly {
			hasTransformOnly = true
		}
		opt := parser.KVOption{Key: parser.Name(k)}
		if importOptionExpectValues[k] {
			opt.Value = parser.NewDString(v)
		}
		stmt.Options = append(stmt.Options, opt)
	}
	if !hasTransformOnly {
		stmt.Options = append(stmt.Options, parser.KVOption{Key: importOptionTransformOnly})
	}
	sort.Slice(stmt.Options, func(i, j int) bool { return stmt.Options[i].Key < stmt.Options[j].Key })
	return parser.AsStringWithFlags(&stmt, parser.FmtSimpleQualified), nil
}

const importCSVEnabledSetting = "experimental.importcsv.enabled"

var importCSVEnabled = settings.RegisterBoolSetting(
	importCSVEnabledSetting,
	"enable experimental IMPORT CSV statement",
	false,
)

func importPlanHook(
	stmt parser.Statement, p sql.PlanHookState,
) (func(context.Context, chan<- parser.Datums) error, sqlbase.ResultColumns, error) {
	importStmt, ok := stmt.(*parser.Import)
	if !ok {
		return nil, nil, nil
	}

	if !importCSVEnabled.Get(&p.ExecCfg().Settings.SV) {
		return nil, nil, errors.Errorf(
			`IMPORT is an experimental feature and is disabled by default; `+
				`enable by executing: SET CLUSTER SETTING %s = true`,
			importCSVEnabledSetting,
		)
	}

	if err := p.RequireSuperUser("IMPORT"); err != nil {
		return nil, nil, err
	}

	filesFn, err := p.TypeAsStringArray(importStmt.Files, "IMPORT")
	if err != nil {
		return nil, nil, err
	}

	var createFileFn func() (string, error)
	if importStmt.CreateDefs == nil {
		createFileFn, err = p.TypeAsString(importStmt.CreateFile, "IMPORT")
		if err != nil {
			return nil, nil, err
		}
	}

	if importStmt.FileFormat != "CSV" {
		// not possible with current parser rules.
		return nil, nil, errors.Errorf("unsupported import format: %q", importStmt.FileFormat)
	}

	optsFn, err := p.TypeAsStringOpts(importStmt.Options, importOptionExpectValues)
	if err != nil {
		return nil, nil, err
	}
	// TODO(dan): This entire method is a placeholder to get the distsql
	// plumbing worked out while mjibson works on the new processors and router.
	// Currently, it "uses" distsql to compute an int and this method returns
	// it.
	fn := func(ctx context.Context, resultsCh chan<- parser.Datums) error {
		walltime := timeutil.Now().UnixNano()

		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, importStmt.StatementTag())
		defer tracing.FinishSpan(span)

		opts, err := optsFn()
		if err != nil {
			return err
		}

		files, err := filesFn()
		if err != nil {
			return err
		}

		_, transformOnly := opts[importOptionTransformOnly]

		var targetDB string
		if !transformOnly {
			if override, ok := opts[restoreOptIntoDB]; !ok {
				if session := p.EvalContext().Database; session != "" {
					targetDB = session
				} else {
					return errors.Errorf("must specify target database with %q option", restoreOptIntoDB)
				}
			} else {
				targetDB = override
				// TODO(dt): verify db exists
			}
		}

		var comma rune
		if override, ok := opts[importOptionDelimiter]; ok {
			comma, err = util.GetSingleRune(override)
			if err != nil {
				return errors.Wrap(err, "invalid comma value")
			}
		}

		var comment rune
		if override, ok := opts[importOptionComment]; ok {
			comment, err = util.GetSingleRune(override)
			if err != nil {
				return errors.Wrap(err, "invalid comment value")
			}
		}

		var nullif *string
		if override, ok := opts[importOptionNullIf]; ok {
			nullif = &override
		}

		var temp string
		if override, ok := opts[importOptionTemp]; ok {
			temp = override
		} else {
			return errors.Errorf("must provide a temporary storage location")
		}

		sstSize := config.DefaultZoneConfig().RangeMaxBytes / 2
		if override, ok := opts[importOptionSSTSize]; ok {
			sz, err := humanizeutil.ParseBytes(override)
			if err != nil {
				return err
			}
			sstSize = sz
		}

		var create *parser.CreateTable
		if importStmt.CreateDefs != nil {
			normName := parser.NormalizableTableName{TableNameReference: importStmt.Table}
			create = &parser.CreateTable{Table: normName, Defs: importStmt.CreateDefs}
		} else {
			filename, err := createFileFn()
			if err != nil {
				return err
			}
			create, err = readCreateTableFromStore(ctx, filename)
			if err != nil {
				return err
			}
			if named, parsed := importStmt.Table.String(), create.Table.String(); parsed != named {
				return errors.Errorf("importing table %q, but file specifies a schema for table %q", named, parsed)
			}
		}

		parentID := defaultCSVParentID
		tableDesc, err := makeCSVTableDescriptor(ctx, create, parentID, defaultCSVTableID, walltime)
		if err != nil {
			return err
		}

		jobDesc, err := importJobDescription(importStmt, create.Defs, files, opts)
		if err != nil {
			return err
		}

		// NB: the post-conversion RESTORE will create and maintain its own job.
		// This job is thus only for tracking the conversion, and will be Finished()
		// before the restore starts.
		job := p.ExecCfg().JobRegistry.NewJob(jobs.Record{
			Description: jobDesc,
			Username:    p.User(),
			Details: jobs.ImportDetails{
				Tables: []jobs.ImportDetails_Table{{
					Desc:       tableDesc,
					URIs:       files,
					BackupPath: temp,
				}},
			},
		})
		if err := job.Created(ctx, jobs.WithoutCancel); err != nil {
			return err
		}
		if err := job.Started(ctx); err != nil {
			return err
		}

		var importErr error
		if _, distributed := opts[importOptionDistributed]; distributed {
			_, importErr = doDistributedCSVTransform(
				ctx, job, files, p, tableDesc, temp,
				comma, comment, nullif, walltime,
				sstSize,
			)
		} else {
			_, _, _, importErr = doLocalCSVTransform(
				ctx, job, parentID, tableDesc, temp, files,
				comma, comment, nullif, sstSize,
				p.ExecCfg().DistSQLSrv.TempStorage,
				walltime, p.ExecCfg(),
			)
		}
		if err := job.FinishedWith(ctx, importErr); err != nil {
			return err
		}
		if importErr != nil {
			return importErr
		}
		if transformOnly {
			resultsCh <- parser.Datums{
				parser.NewDInt(parser.DInt(*job.ID())),
				parser.NewDString(string(jobs.StatusSucceeded)),
				parser.NewDFloat(parser.DFloat(1.0)),
				parser.NewDInt(parser.DInt(0)),
				parser.NewDInt(parser.DInt(0)),
				parser.NewDInt(parser.DInt(0)),
				parser.NewDInt(parser.DInt(0)),
			}
			return nil
		}

		restore := &parser.Restore{
			Targets: parser.TargetList{
				Tables: []parser.TablePattern{&parser.AllTablesSelector{Database: csvDatabaseName}},
			},
			From: parser.Exprs{parser.NewDString(temp)},
		}
		from := []string{temp}
		opts = map[string]string{restoreOptIntoDB: targetDB}

		return doRestorePlan(ctx, restore, p, from, opts, resultsCh)
	}
	return fn, restoreHeader, nil
}

func doDistributedCSVTransform(
	ctx context.Context,
	job *jobs.Job,
	files []string,
	p sql.PlanHookState,
	tableDesc *sqlbase.TableDescriptor,
	temp string,
	comma, comment rune,
	nullif *string,
	walltime int64,
	sstSize int64,
) (int64, error) {
	evalCtx := p.EvalContext()

	// TODO(dan): Filter out unhealthy nodes.
	resp, err := p.ExecCfg().StatusServer.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return 0, err
	}
	var nodes []roachpb.NodeDescriptor
	for _, node := range resp.Nodes {
		nodes = append(nodes, node.Desc)
	}

	ci := sqlbase.ColTypeInfoFromColTypes([]sqlbase.ColumnType{
		{SemanticType: sqlbase.ColumnType_STRING},
		{SemanticType: sqlbase.ColumnType_INT},
		{SemanticType: sqlbase.ColumnType_BYTES},
		{SemanticType: sqlbase.ColumnType_BYTES},
		{SemanticType: sqlbase.ColumnType_BYTES},
	})
	rows := sqlbase.NewRowContainer(evalCtx.Mon.MakeBoundAccount(), ci, 0)
	defer func() {
		if rows != nil {
			rows.Close(ctx)
		}
	}()

	if err := p.DistLoader().LoadCSV(
		ctx,
		job,
		p.ExecCfg().DB,
		evalCtx,
		p.ExecCfg().NodeID.Get(),
		nodes,
		sql.NewRowResultWriter(parser.Rows, rows),
		tableDesc,
		files,
		temp,
		comma, comment,
		nullif,
		walltime,
		sstSize,
	); err != nil {
		return 0, err
	}

	backupDesc := BackupDescriptor{
		EndTime: hlc.Timestamp{WallTime: walltime},
	}
	n := rows.Len()
	for i := 0; i < n; i++ {
		row := rows.At(i)
		name := row[0].(*parser.DString)
		size := row[1].(*parser.DInt)
		checksum := row[2].(*parser.DBytes)
		spanStart := row[3].(*parser.DBytes)
		spanEnd := row[4].(*parser.DBytes)
		backupDesc.EntryCounts.DataSize += int64(*size)
		backupDesc.Files = append(backupDesc.Files, BackupDescriptor_File{
			Path: string(*name),
			Span: roachpb.Span{
				Key:    roachpb.Key(*spanStart),
				EndKey: roachpb.Key(*spanEnd),
			},
			Sha512: []byte(*checksum),
		})
	}

	dest, err := storageccl.ExportStorageConfFromURI(temp)
	if err != nil {
		return 0, err
	}
	es, err := storageccl.MakeExportStorage(ctx, dest)
	if err != nil {
		return 0, err
	}
	defer es.Close()

	if err := finalizeCSVBackup(ctx, &backupDesc, defaultCSVParentID, tableDesc, es, p.ExecCfg()); err != nil {
		return 0, err
	}
	total := int64(len(backupDesc.Files))
	return total, nil
}

var csvOutputTypes = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_BYTES},
	{SemanticType: sqlbase.ColumnType_BYTES},
}

func newReadCSVProcessor(
	flowCtx *distsqlrun.FlowCtx, spec distsqlrun.ReadCSVSpec, output distsqlrun.RowReceiver,
) (distsqlrun.Processor, error) {
	cp := &readCSVProcessor{
		csvOptions: spec.Options,
		sampleSize: spec.SampleSize,
		tableDesc:  spec.TableDesc,
		uri:        spec.Uri,
		output:     output,
	}
	if err := cp.out.Init(&distsqlrun.PostProcessSpec{}, csvOutputTypes, &flowCtx.EvalCtx, output); err != nil {
		return nil, err
	}
	return cp, nil
}

type readCSVProcessor struct {
	csvOptions roachpb.CSVOptions
	sampleSize int32
	tableDesc  sqlbase.TableDescriptor
	uri        string
	out        distsqlrun.ProcOutputHelper
	output     distsqlrun.RowReceiver
}

var _ distsqlrun.Processor = &readCSVProcessor{}

func (cp *readCSVProcessor) OutputTypes() []sqlbase.ColumnType {
	return csvOutputTypes
}

func (cp *readCSVProcessor) Run(ctx context.Context, wg *sync.WaitGroup) {
	ctx, span := tracing.ChildSpan(ctx, "readCSVProcessor")
	defer tracing.FinishSpan(span)

	if wg != nil {
		defer wg.Done()
	}

	group, gCtx := errgroup.WithContext(ctx)
	done := gCtx.Done()
	recordCh := make(chan csvRecord)
	kvCh := make(chan roachpb.KeyValue)
	sampleCh := make(chan sqlbase.EncDatumRow)

	// Read CSV into CSV records
	group.Go(func() error {
		sCtx, span := tracing.ChildSpan(gCtx, "readcsv")
		defer tracing.FinishSpan(span)
		defer close(recordCh)
		_, err := readCSV(sCtx, cp.csvOptions.Comma, cp.csvOptions.Comment,
			len(cp.tableDesc.VisibleColumns()), []string{cp.uri}, recordCh)
		return err
	})
	// Convert CSV records to KVs
	group.Go(func() error {
		sCtx, span := tracing.ChildSpan(gCtx, "convertcsv")
		defer tracing.FinishSpan(span)

		defer close(kvCh)
		return groupWorkers(sCtx, runtime.NumCPU(), func(ctx context.Context) error {
			return convertRecord(ctx, recordCh, kvCh, cp.csvOptions.Nullif, &cp.tableDesc)
		})
	})
	// Sample KVs
	group.Go(func() error {
		sCtx, span := tracing.ChildSpan(gCtx, "samplecsv")
		defer tracing.FinishSpan(span)

		defer close(sampleCh)
		var fn sampleFunc
		if cp.sampleSize == 0 {
			fn = sampleAll
		} else {
			sr := sampleRate{
				rnd:        rand.New(rand.NewSource(rand.Int63())),
				sampleSize: float64(cp.sampleSize),
			}
			fn = sr.sample
		}
		typeBytes := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES}
		for kv := range kvCh {
			if fn(kv) {
				row := sqlbase.EncDatumRow{
					sqlbase.DatumToEncDatum(typeBytes, parser.NewDBytes(parser.DBytes(kv.Key))),
					sqlbase.DatumToEncDatum(typeBytes, parser.NewDBytes(parser.DBytes(kv.Value.RawBytes))),
				}
				select {
				case <-done:
					return sCtx.Err()
				case sampleCh <- row:
				}
			}
		}
		return nil
	})
	// Send sampled KVs to dist sql
	group.Go(func() error {
		sCtx, span := tracing.ChildSpan(gCtx, "sendcsvkv")
		defer tracing.FinishSpan(span)

		for row := range sampleCh {
			cs, err := cp.out.EmitRow(sCtx, row)
			if err != nil {
				return err
			}
			if cs != distsqlrun.NeedMoreRows {
				return errors.New("unexpected closure of consumer")
			}
		}
		return nil
	})
	if err := group.Wait(); err != nil {
		distsqlrun.DrainAndClose(ctx, cp.output, err)
		return
	}

	cp.out.Close()
}

type sampleFunc func(roachpb.KeyValue) bool

// sampleRate is a sampleFunc that samples a row with a probability of the
// row's size / the sample size.
type sampleRate struct {
	rnd        *rand.Rand
	sampleSize float64
}

func (s sampleRate) sample(kv roachpb.KeyValue) bool {
	sz := float64(len(kv.Key) + len(kv.Value.RawBytes))
	prob := sz / s.sampleSize
	return prob > s.rnd.Float64()
}

func sampleAll(kv roachpb.KeyValue) bool {
	return true
}

var sstOutputTypes = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_STRING},
	{SemanticType: sqlbase.ColumnType_INT},
	{SemanticType: sqlbase.ColumnType_BYTES},
	{SemanticType: sqlbase.ColumnType_BYTES},
	{SemanticType: sqlbase.ColumnType_BYTES},
}

func newSSTWriterProcessor(
	flowCtx *distsqlrun.FlowCtx,
	spec distsqlrun.SSTWriterSpec,
	input distsqlrun.RowSource,
	output distsqlrun.RowReceiver,
) (distsqlrun.Processor, error) {
	sp := &sstWriter{
		uri:           spec.Destination,
		name:          spec.Name,
		walltimeNanos: spec.WalltimeNanos,
		input:         input,
		output:        output,
		tempStorage:   flowCtx.TempStorage,
	}
	if err := sp.out.Init(&distsqlrun.PostProcessSpec{}, sstOutputTypes, &flowCtx.EvalCtx, output); err != nil {
		return nil, err
	}
	return sp, nil
}

type sstWriter struct {
	uri           string
	name          string
	walltimeNanos int64
	input         distsqlrun.RowSource
	out           distsqlrun.ProcOutputHelper
	output        distsqlrun.RowReceiver
	tempStorage   engine.Engine
}

var _ distsqlrun.Processor = &sstWriter{}

func (sp *sstWriter) OutputTypes() []sqlbase.ColumnType {
	return sstOutputTypes
}

func (sp *sstWriter) Run(ctx context.Context, wg *sync.WaitGroup) {
	ctx, span := tracing.ChildSpan(ctx, "sstWriter")
	defer tracing.FinishSpan(span)

	if wg != nil {
		defer wg.Done()
	}

	defer distsqlrun.DrainAndForwardMetadata(ctx, sp.input, sp.output)
	err := func() error {
		// We need to produce a single SST file. engine.MakeRocksDBSstFileWriter is
		// able to do this in memory, but requires that rows added to it be done
		// in order. Thus, we first need to fetch all rows and sort them. We use
		// NewRocksDBMap to write the rows, then fetch them in order using an iterator.
		input := distsqlrun.MakeNoMetadataRowSource(sp.input, sp.output)
		alloc := &sqlbase.DatumAlloc{}
		store := engine.NewRocksDBMultiMap(sp.tempStorage)
		defer store.Close(ctx)
		batch := store.NewBatchWriter()
		var key, val []byte
		for {
			row, err := input.NextRow()
			if err != nil {
				return err
			}
			if row == nil {
				break
			}
			if len(row) != 2 {
				return errors.Errorf("expected 2 datums, got %d", len(row))
			}
			for i, ed := range row {
				if err := ed.EnsureDecoded(alloc); err != nil {
					return err
				}
				datum := ed.Datum.(*parser.DBytes)
				b := []byte(*datum)
				switch i {
				case 0:
					key = b
				case 1:
					val = b
				}
			}
			if err := batch.Put(key, val); err != nil {
				return err
			}
		}
		if err := batch.Close(ctx); err != nil {
			return err
		}
		iter := store.NewIterator()
		var kv engine.MVCCKeyValue
		kv.Key.Timestamp.WallTime = sp.walltimeNanos
		var firstKey roachpb.Key
		sst, err := engine.MakeRocksDBSstFileWriter()
		if err != nil {
			return err
		}
		defer sst.Close()
		var lastKey []byte
		for iter.Rewind(); ; iter.Next() {
			if ok, err := iter.Valid(); err != nil {
				return err
			} else if !ok {
				break
			}
			kv.Key.Key = iter.UnsafeKey()
			kv.Value = iter.UnsafeValue()
			if firstKey == nil {
				firstKey = iter.Key()
			}
			if err := sst.Add(kv); err != nil {
				return errors.Wrapf(err, errSSTCreationMaybeDuplicateTemplate, kv.Key.Key)
			}
			lastKey = append(lastKey[:0], kv.Key.Key.Next()...)
		}
		data, err := sst.Finish()
		if err != nil {
			return err
		}
		checksum, err := storageccl.SHA512ChecksumData(data)
		if err != nil {
			return err
		}
		conf, err := storageccl.ExportStorageConfFromURI(sp.uri)
		if err != nil {
			return err
		}
		es, err := storageccl.MakeExportStorage(ctx, conf)
		if err != nil {
			return err
		}
		defer es.Close()
		if err := es.WriteFile(ctx, sp.name, bytes.NewReader(data)); err != nil {
			return err
		}

		row := sqlbase.EncDatumRow{
			sqlbase.DatumToEncDatum(
				sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING},
				parser.NewDString(sp.name),
			),
			sqlbase.DatumToEncDatum(
				sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT},
				parser.NewDInt(parser.DInt(len(data))),
			),
			sqlbase.DatumToEncDatum(
				sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES},
				parser.NewDBytes(parser.DBytes(checksum)),
			),
			sqlbase.DatumToEncDatum(
				sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES},
				parser.NewDBytes(parser.DBytes(firstKey)),
			),
			sqlbase.DatumToEncDatum(
				sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES},
				parser.NewDBytes(parser.DBytes(lastKey)),
			),
		}
		cs, err := sp.out.EmitRow(ctx, row)
		if err != nil {
			return err
		}
		if cs != distsqlrun.NeedMoreRows {
			return errors.New("unexpected closure of consumer")
		}
		return nil
	}()
	if err != nil {
		distsqlrun.DrainAndClose(ctx, sp.output, err)
		return
	}

	sp.out.Close()
}

func init() {
	sql.AddPlanHook(importPlanHook)
	distsqlrun.NewReadCSVProcessor = newReadCSVProcessor
	distsqlrun.NewSSTWriterProcessor = newSSTWriterProcessor
}
