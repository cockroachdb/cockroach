// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

const (
	importOptionDelimiter = "delimiter"
	importOptionComment   = "comment"
	importOptionNullIf    = "nullif"
	importOptionTransform = "transform"
	importOptionSkip      = "skip"
	importOptionSSTSize   = "sstsize"
)

var importOptionExpectValues = map[string]bool{
	importOptionDelimiter: true,
	importOptionComment:   true,
	importOptionNullIf:    true,
	importOptionTransform: true,
	importOptionSkip:      true,
	importOptionSSTSize:   true,
}

const (
	// We need to choose arbitrary database and table IDs. These aren't important,
	// but they do match what would happen when creating a new database and
	// table on an empty cluster.
	defaultCSVParentID sqlbase.ID = keys.MaxReservedDescID + 1
	defaultCSVTableID  sqlbase.ID = defaultCSVParentID + 1
)

func readCreateTableFromStore(
	ctx context.Context, filename string, settings *cluster.Settings,
) (*tree.CreateTable, error) {
	store, err := storageccl.ExportStorageFromURI(ctx, filename, settings)
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
	create, ok := stmt.(*tree.CreateTable)
	if !ok {
		return nil, errors.New("expected CREATE TABLE statement in table file")
	}
	return create, nil
}

// MakeSimpleTableDescriptor creates a TableDescriptor from a CreateTable parse
// node without the full machinery. Many parts of the syntax are unsupported
// (see the implementation and TestMakeSimpleTableDescriptorErrors for details),
// but this is enough for our csv IMPORT and for some unit tests.
func MakeSimpleTableDescriptor(
	ctx context.Context,
	st *cluster.Settings,
	create *tree.CreateTable,
	parentID,
	tableID sqlbase.ID,
	walltime int64,
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
		case *tree.CheckConstraintTableDef,
			*tree.FamilyTableDef,
			*tree.IndexTableDef,
			*tree.UniqueConstraintTableDef:
			// ignore
		case *tree.ColumnTableDef:
			if def.Computed.Expr != nil {
				return nil, errors.Errorf("computed columns not supported: %s", tree.AsString(def))
			}
		case *tree.ForeignKeyConstraintTableDef:
			return nil, errors.Errorf("foreign keys not supported: %s", tree.AsString(def))
		default:
			return nil, errors.Errorf("unsupported table definition: %s", tree.AsString(def))
		}
	}
	semaCtx := tree.SemaContext{}
	evalCtx := tree.EvalContext{CtxProvider: ctxProvider{ctx}}
	tableDesc, err := sql.MakeTableDesc(
		ctx,
		nil, /* txn */
		nil, /* vt */
		st,
		create,
		parentID,
		tableID,
		hlc.Timestamp{WallTime: walltime},
		sqlbase.NewDefaultPrivilegeDescriptor(),
		nil, /* affected */
		&semaCtx,
		&evalCtx,
	)
	if err != nil {
		return nil, err
	}

	return &tableDesc, nil
}

type ctxProvider struct {
	context.Context
}

func (c ctxProvider) Ctx() context.Context {
	return c
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

// readCSV sends records on ch from CSV listed by dataFiles. The key part
// of dataFiles is the unique index of the CSV file among all CSV files in
// the IMPORT. comma, if non-zero, specifies the field separator. comment,
// if non-zero, specifies the comment character. It returns the number of rows
// read. progressFn, if not nil, is periodically invoked with a percentage of
// the total progress of reading through all of the files. This percentage
// attempts to use the Size() method of ExportStorage to determine how many
// bytes must be read of the CSV files, and reports the percent of bytes read
// among all dataFiles. If any Size() fails for any file, then progress is
// reported only after each file has been read.
func readCSV(
	ctx context.Context,
	comma, comment rune,
	skip uint32,
	expectedCols int,
	dataFiles map[int32]string,
	recordCh chan<- csvRecord,
	progressFn func(float32) error,
	settings *cluster.Settings,
) (int64, error) {
	const batchSize = 500
	expectedColsExtra := expectedCols + 1
	done := ctx.Done()
	var count int64
	if comma == 0 {
		comma = ','
	}

	var totalBytes, readBytes int64
	// Attempt to fetch total number of bytes for all files.
	for _, dataFile := range dataFiles {
		conf, err := storageccl.ExportStorageConfFromURI(dataFile)
		if err != nil {
			return 0, err
		}
		es, err := storageccl.MakeExportStorage(ctx, conf, settings)
		if err != nil {
			return 0, err
		}
		sz, err := es.Size(ctx, "")
		es.Close()
		if sz <= 0 {
			// Don't log dataFile here because it could leak auth information.
			log.Infof(ctx, "could not fetch file size; falling back to per-file progress: %v", err)
			totalBytes = 0
			break
		}
		totalBytes += sz
	}
	updateFromFiles := progressFn != nil && totalBytes == 0
	updateFromBytes := progressFn != nil && totalBytes > 0

	currentFile := 0
	for dataFileIndex, dataFile := range dataFiles {
		currentFile++
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
			es, err := storageccl.MakeExportStorage(ctx, conf, settings)
			if err != nil {
				return err
			}
			defer es.Close()
			f, err := es.ReadFile(ctx, "")
			if err != nil {
				return err
			}
			bc := byteCounter{r: f}
			cr := csv.NewReader(&bc)
			cr.Comma = comma
			cr.FieldsPerRecord = -1
			cr.LazyQuotes = true
			cr.Comment = comment

			batch := csvRecord{
				file:      dataFile,
				fileIndex: dataFileIndex,
				rowOffset: 1,
				r:         make([][]string, 0, batchSize),
			}

			for i := 1; ; i++ {
				record, err := cr.Read()
				if err == io.EOF || len(batch.r) >= batchSize {
					// if the batch isn't empty, we need to flush it.
					if len(batch.r) > 0 {
						select {
						case <-done:
							return ctx.Err()
						case recordCh <- batch:
							count += int64(len(batch.r))
						}
					}
					// progressBytes is the number of read bytes at which to report job progress. A
					// low value may cause excessive updates in the job table which can lead to
					// very large rows due to MVCC saving each version.
					const progressBytes = 100 << 20
					if updateFromBytes && (err == io.EOF || bc.n > progressBytes) {
						readBytes += bc.n
						bc.n = 0
						if err := progressFn(float32(readBytes) / float32(totalBytes)); err != nil {
							return err
						}
					}
					if err == io.EOF {
						break
					}
					batch.rowOffset = i
					batch.r = make([][]string, 0, batchSize)
				}
				if err != nil {
					return errors.Wrapf(err, "row %d: reading CSV record", i)
				}
				// Ignore the first N lines.
				if uint32(i) <= skip {
					continue
				}
				if len(record) == expectedCols {
					// Expected number of columns.
				} else if len(record) == expectedColsExtra && record[expectedCols] == "" {
					// Line has the optional trailing comma, ignore the empty field.
					record = record[:expectedCols]
				} else {
					return errors.Errorf("row %d: expected %d fields, got %d", i, expectedCols, len(record))
				}
				batch.r = append(batch.r, record)
			}
			return nil
		}()
		if err != nil {
			return 0, errors.Wrap(err, dataFile)
		}
		if updateFromFiles {
			if err := progressFn(float32(currentFile) / float32(len(dataFiles))); err != nil {
				return 0, err
			}
		}
	}
	return count, nil
}

type byteCounter struct {
	r io.Reader
	n int64
}

func (b *byteCounter) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n += int64(n)
	return n, err
}

type csvRecord struct {
	r         [][]string
	file      string
	fileIndex int32
	rowOffset int
}

// convertRecord converts CSV records into KV pairs and sends them on the
// kvCh chan.
func convertRecord(
	ctx context.Context,
	recordCh <-chan csvRecord,
	kvCh chan<- []roachpb.KeyValue,
	nullif *string,
	tableDesc *sqlbase.TableDescriptor,
) error {
	done := ctx.Done()

	const kvBatchSize = 1000
	padding := 2 * (len(tableDesc.Indexes) + len(tableDesc.Families))
	visibleCols := tableDesc.VisibleColumns()
	cenv := &tree.CollationEnvironment{}

	ri, err := sqlbase.MakeRowInserter(nil /* txn */, tableDesc, nil, /* fkTables */
		tableDesc.Columns, false /* checkFKs */, &sqlbase.DatumAlloc{})
	if err != nil {
		return errors.Wrap(err, "make row inserter")
	}

	var txCtx transform.ExprTransformContext
	evalCtx := tree.EvalContext{SessionData: &sessiondata.SessionData{Location: time.UTC}}
	// Although we don't yet support DEFAULT expressions on visible columns,
	// we do on hidden columns (which is only the default _rowid one). This
	// allows those expressions to run.
	cols, defaultExprs, err := sqlbase.ProcessDefaultColumns(tableDesc.Columns, tableDesc, &txCtx, &evalCtx)
	if err != nil {
		return errors.Wrap(err, "process default columns")
	}

	datums := make([]tree.Datum, len(visibleCols), len(cols))

	// Check for a hidden column. This should be the unique_rowid PK if present.
	hidden := -1
	for i, col := range cols {
		if col.Hidden {
			if col.DefaultExpr == nil || *col.DefaultExpr != "unique_rowid()" || hidden != -1 {
				return errors.New("unexpected hidden column")
			}
			hidden = i
			datums = append(datums, nil)
		}
	}
	if len(datums) != len(cols) {
		return errors.New("unexpected hidden column")
	}

	kvBatch := make([]roachpb.KeyValue, 0, kvBatchSize+padding)

	for batch := range recordCh {
		for batchIdx, record := range batch.r {
			rowNum := batch.rowOffset + batchIdx
			for i, v := range record {
				col := visibleCols[i]
				if nullif != nil && v == *nullif {
					datums[i] = tree.DNull
				} else {
					datums[i], err = parser.ParseStringAs(col.Type.ToDatumType(), v, &evalCtx, cenv)
					if err != nil {
						return errors.Wrapf(err, "%s: row %d: parse %q as %s", batch.file, rowNum, col.Name, col.Type.SQLString())
					}
				}
			}
			if hidden >= 0 {
				// We don't want to call unique_rowid() for the hidden PK column because
				// it is not idempotent. The sampling from the first stage will be useless
				// during the read phase, producing a single range split with all of the
				// data. Instead, we will call our own function that mimics that function,
				// but more-or-less guarantees that it will not interfere with the numbers
				// that will be produced by it. The lower 15 bits mimic the node id, but as
				// the CSV file number. The upper 48 bits are the line number and mimic the
				// timestamp. It would take a file with many more than 2**32 lines to even
				// begin approaching what unique_rowid would return today, so we assume it
				// to be safe. Since the timestamp is won't overlap, it is safe to use any
				// number in the node id portion. The 15 bits in that portion should account
				// for up to 32k CSV files in a single IMPORT. In the case of > 32k files,
				// the data is xor'd so the final bits are flipped instead of set.
				datums[hidden] = tree.NewDInt(builtins.GenerateUniqueID(batch.fileIndex, uint64(rowNum)))
			}

			// TODO(justin): we currently disallow computed columns in import statements.
			var computeExprs []tree.TypedExpr
			var computedCols []sqlbase.ColumnDescriptor

			row, err := sql.GenerateInsertRow(defaultExprs, computeExprs, ri.InsertColIDtoRowIndex, cols, computedCols, evalCtx, tableDesc, datums)
			if err != nil {
				return errors.Wrapf(err, "generate insert row: %s: row %d", batch.file, rowNum)
			}
			// TODO(bram): Is the checking of FKs here required? If not, turning them
			// off may provide a speed boost.
			if err := ri.InsertRow(
				ctx,
				inserter(func(kv roachpb.KeyValue) {
					kv.Value.InitChecksum(kv.Key)
					kvBatch = append(kvBatch, kv)
				}),
				row,
				true, /* ignoreConflicts */
				sqlbase.CheckFKs,
				false, /* traceKV */
			); err != nil {
				return errors.Wrapf(err, "insert row: %s: row %d", batch.file, rowNum)
			}
			if len(kvBatch) >= kvBatchSize {
				select {
				case kvCh <- kvBatch:
				case <-done:
					return ctx.Err()
				}
				kvBatch = make([]roachpb.KeyValue, 0, kvBatchSize+padding)
			}
		}
	}
	select {
	case kvCh <- kvBatch:
	case <-done:
		return ctx.Err()
	}
	return nil
}

type sstContent struct {
	data []byte
	size int64
	span roachpb.Span
	more bool
}

const errSSTCreationMaybeDuplicateTemplate = "SST creation error at %s; this can happen when a primary or unique index has duplicate keys"

// makeSSTs creates SST files in memory of size maxSize and sent on
// contentCh. progressFn, if not nil, is periodically invoked with the number
// of KVs that have been written to SSTs and sent on contentCh. endKey,
// if not nil, will stop processing at the specified key.
func makeSSTs(
	ctx context.Context,
	it engine.SortedDiskMapIterator,
	sstMaxSize int64,
	contentCh chan<- sstContent,
	walltime int64,
	endKey roachpb.Key,
	progressFn func(int) error,
) error {
	sst, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		return err
	}
	defer sst.Close()

	var writtenKVs int
	writeSST := func(key, endKey roachpb.Key, more bool) error {
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
			more: more,
		}
		select {
		case contentCh <- sc:
		case <-ctx.Done():
			return ctx.Err()
		}
		sst.Close()
		if progressFn != nil {
			if err := progressFn(writtenKVs); err != nil {
				return err
			}
		}
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

	if ok, err := it.Valid(); err != nil {
		return err
	} else if !ok {
		// Empty file.
		return nil
	}
	firstKey = it.Key()

	for ; ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}

		// Check this before setting kv.Key.Key because it is used below in the
		// final writeSST invocation.
		if endKey != nil && endKey.Compare(it.UnsafeKey()) <= 0 {
			// If we are at the end, break the loop and return the data. There is no
			// need to back up one key, because the iterator is pointing at the start
			// of the next block already, and Next won't be called until after the key
			// has been extracted again during the next call to this function.
			break
		}

		writtenKVs++

		kv.Key.Key = it.Key()
		kv.Value = it.UnsafeValue()

		if lastKey != nil {
			if kv.Key.Key.Compare(lastKey) >= 0 {
				if err := writeSST(firstKey, lastKey, true); err != nil {
					return err
				}
				firstKey = it.Key()
				lastKey = nil

				sst, err = engine.MakeRocksDBSstFileWriter()
				if err != nil {
					return err
				}
				defer sst.Close()
			}
		}
		if err := sst.Add(kv); err != nil {
			return errors.Wrapf(err, errSSTCreationMaybeDuplicateTemplate, kv.Key.Key)
		}
		if sst.DataSize > sstMaxSize && lastKey == nil {
			// When we would like to split the file, proceed until we aren't in the
			// middle of a row. Start by finding the next safe split key.
			lastKey, err = keys.EnsureSafeSplitKey(kv.Key.Key)
			if err != nil {
				return err
			}
			lastKey = lastKey.PrefixEnd()
		}
	}
	if sst.DataSize > 0 {
		// Although we don't need to avoid row splitting here because there aren't any
		// more keys to read, we do still want to produce the same kind of lastKey
		// argument for the span as in the case above. lastKey <= the most recent
		// sst.Add call, but since we call PrefixEnd below, it will be guaranteed
		// to be > the most recent added key.
		lastKey, err = keys.EnsureSafeSplitKey(kv.Key.Key)
		if err != nil {
			return err
		}
		if err := writeSST(firstKey, lastKey.PrefixEnd(), false); err != nil {
			return err
		}
	}
	return nil
}

const csvDatabaseName = "csv"

func finalizeCSVBackup(
	ctx context.Context,
	backupDesc *backupccl.BackupDescriptor,
	parentID sqlbase.ID,
	tableDesc *sqlbase.TableDescriptor,
	es storageccl.ExportStorage,
	execCfg *sql.ExecutorConfig,
) error {
	sort.Sort(backupccl.BackupFileDescriptors(backupDesc.Files))
	backupDesc.Spans = []roachpb.Span{tableDesc.TableSpan()}
	backupDesc.Descriptors = []sqlbase.Descriptor{
		*sqlbase.WrapDescriptor(&sqlbase.DatabaseDescriptor{
			Name: csvDatabaseName,
			ID:   parentID,
		}),
		*sqlbase.WrapDescriptor(tableDesc),
	}
	backupDesc.FormatVersion = backupccl.BackupFormatInitialVersion
	backupDesc.BuildInfo = build.GetInfo()
	if execCfg != nil {
		backupDesc.NodeID = execCfg.NodeID.Get()
		backupDesc.ClusterID = execCfg.ClusterID()
	}
	descBuf, err := protoutil.Marshal(backupDesc)
	if err != nil {
		return err
	}
	return es.WriteFile(ctx, backupccl.BackupDescriptorName, bytes.NewReader(descBuf))
}

func importJobDescription(
	orig *tree.Import, defs tree.TableDefs, files []string, opts map[string]string,
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
		stmt.Files = append(stmt.Files, tree.NewDString(clean))
	}
	stmt.Options = nil
	for k, v := range opts {
		switch k {
		case importOptionTransform:
			clean, err := storageccl.SanitizeExportStorageURI(v)
			if err != nil {
				return "", err
			}
			v = clean
		}
		opt := tree.KVOption{Key: tree.Name(k)}
		if importOptionExpectValues[k] {
			opt.Value = tree.NewDString(v)
		}
		stmt.Options = append(stmt.Options, opt)
	}
	sort.Slice(stmt.Options, func(i, j int) bool { return stmt.Options[i].Key < stmt.Options[j].Key })
	return tree.AsStringWithFlags(&stmt, tree.FmtAlwaysQualifyTableNames), nil
}

func importPlanHook(
	_ context.Context, stmt tree.Statement, p sql.PlanHookState,
) (func(context.Context, chan<- tree.Datums) error, sqlbase.ResultColumns, error) {
	importStmt, ok := stmt.(*tree.Import)
	if !ok {
		return nil, nil, nil
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

	fn := func(ctx context.Context, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, importStmt.StatementTag())
		defer tracing.FinishSpan(span)

		walltime := p.ExecCfg().Clock.Now().WallTime

		if err := p.RequireSuperUser(ctx, "IMPORT"); err != nil {
			return err
		}

		if !p.ExtendedEvalContext().TxnImplicit {
			return errors.Errorf("IMPORT cannot be used inside a transaction")
		}

		opts, err := optsFn()
		if err != nil {
			return err
		}

		files, err := filesFn()
		if err != nil {
			return err
		}

		// Normalize must be called regardles of whether there is a
		// transform because it prepares a TableName with the right
		// structure and stores it back into the statement AST, which we
		// need later when computing the job title.
		name, err := importStmt.Table.Normalize()
		if err != nil {
			return errors.Wrap(err, "normalize create table")
		}
		parentID := defaultCSVParentID
		transform := opts[importOptionTransform]
		if transform == "" {
			found, descI, err := name.ResolveTarget(ctx,
				p, p.SessionData().Database, p.SessionData().SearchPath)
			if err != nil {
				return errors.Wrap(err, "resolving target import name")
			}
			if !found {
				// Check if database exists right now. It might not after the import is done,
				// but it's better to fail fast than wait until restore.
				return errors.Errorf("database does not exist: %q", name)
			}
			parentID = descI.(*sqlbase.DatabaseDescriptor).ID
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

		var skip int
		if override, ok := opts[importOptionSkip]; ok {
			skip, err = strconv.Atoi(override)
			if err != nil {
				return errors.Wrapf(err, "invalid %s value", importOptionSkip)
			}
			if skip < 0 {
				return errors.Errorf("%s must be >= 0", importOptionSkip)
			}
			// We need to handle the case where the user wants to skip records and the node
			// interpreting the statement might be newer than other nodes in the cluster.
			if !p.ExecCfg().Settings.Version.IsMinSupported(cluster.VersionImportSkipRecords) {
				return errors.Errorf("Using %s requires all nodes to be upgraded to %s",
					importOptionSkip, cluster.VersionByKey(cluster.VersionImportSkipRecords))
			}
		}

		// sstSize, if 0, will be set to an appropriate default by the specific
		// implementation (local or distributed) since each has different optimal
		// settings.
		var sstSize int64
		if override, ok := opts[importOptionSSTSize]; ok {
			sz, err := humanizeutil.ParseBytes(override)
			if err != nil {
				return err
			}
			sstSize = sz
		}

		var create *tree.CreateTable
		if importStmt.CreateDefs != nil {
			create = &tree.CreateTable{Table: importStmt.Table, Defs: importStmt.CreateDefs}
		} else {
			filename, err := createFileFn()
			if err != nil {
				return err
			}
			create, err = readCreateTableFromStore(ctx, filename, p.ExecCfg().Settings)
			if err != nil {
				return err
			}

			if named, err := importStmt.Table.Normalize(); err != nil {
				return errors.Wrap(err, "normalize import table")
			} else if parsed, err := create.Table.Normalize(); err != nil {
				return errors.Wrap(err, "normalize create table")
			} else if named.TableName != parsed.TableName {
				return errors.Errorf("importing table %s, but file specifies a schema for table %s", named.TableName, parsed.TableName)
			}
		}

		tableDesc, err := MakeSimpleTableDescriptor(
			ctx, p.ExecCfg().Settings, create, parentID, defaultCSVTableID, walltime)
		if err != nil {
			return err
		}

		jobDesc, err := importJobDescription(importStmt, create.Defs, files, opts)
		if err != nil {
			return err
		}

		if transform != "" {
			transformStorage, err := storageccl.ExportStorageFromURI(ctx, transform, p.ExecCfg().Settings)
			if err != nil {
				return err
			}
			// Delay writing the BACKUP-CHECKPOINT file until as late as possible.
			err = backupccl.VerifyUsableExportTarget(ctx, transformStorage, transform)
			transformStorage.Close()
			if err != nil {
				return err
			}
		} else {
			if err := backupccl.CheckTableExists(ctx, p.Txn(), parentID, tableDesc.Name); err != nil {
				return err
			}

			// Verification steps have passed, generate a new table ID if we're
			// restoring. We do this last because we want to avoid calling
			// GenerateUniqueDescID if there's any kind of error above.
			// Reserving a table ID now means we can avoid the rekey work during restore.
			tableDesc.ID, err = sql.GenerateUniqueDescID(ctx, p.ExecCfg().DB)
			if err != nil {
				return err
			}
		}

		var nullifVal *jobs.ImportDetails_Table_Nullif
		if nullif != nil {
			nullifVal = &jobs.ImportDetails_Table_Nullif{Nullif: *nullif}
		}

		_, errCh, err := p.ExecCfg().JobRegistry.StartJob(ctx, resultsCh, jobs.Record{
			Description: jobDesc,
			Username:    p.User(),
			Details: jobs.ImportDetails{
				Tables: []jobs.ImportDetails_Table{{
					Desc:       tableDesc,
					URIs:       files,
					BackupPath: transform,
					ParentID:   parentID,
					Comma:      comma,
					Comment:    comment,
					Nullif:     nullifVal,
					Skip:       uint32(skip),
					SSTSize:    sstSize,
					Walltime:   walltime,
				}},
			},
		})
		if err != nil {
			return err
		}
		return <-errCh
	}
	return fn, backupccl.RestoreHeader, nil
}

func doDistributedCSVTransform(
	ctx context.Context,
	job *jobs.Job,
	files []string,
	p sql.PlanHookState,
	parentID sqlbase.ID,
	tableDesc *sqlbase.TableDescriptor,
	temp string,
	comma, comment rune,
	skip uint32,
	nullif *string,
	walltime int64,
	sstSize int64,
) error {
	evalCtx := p.ExtendedEvalContext()

	// TODO(dan): Filter out unhealthy nodes.
	resp, err := p.ExecCfg().StatusServer.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return err
	}
	var nodes []roachpb.NodeDescriptor
	for _, node := range resp.Nodes {
		nodes = append(nodes, node.Desc)
	}
	// Shuffle node order so that multiple IMPORTs done in parallel will not
	// identically schedule CSV reading. For example, if there are 3 nodes and 4
	// files, the first node will get 2 files while the other nodes will each get 1
	// file. Shuffling will make that first node random instead of always the same.
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})

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
		sql.NewRowResultWriter(rows),
		tableDesc,
		files,
		temp,
		comma, comment,
		skip,
		nullif,
		walltime,
		sstSize,
	); err != nil {
		// If the job was canceled, any of the distsql processors could have been
		// the first to encounter the .Progress error. This error's string is sent
		// through distsql back here, so we can't examine the err type in this case
		// to see if it's a jobs.InvalidStatusError. Instead, attempt to update the
		// job progress to coerce out the correct error type. If the update succeeds
		// then return the original error, otherwise return this error instead so
		// it can be cleaned up at a higher level.
		if err := job.Progressed(ctx, func(ctx context.Context, details jobs.Details) float32 {
			d := details.(*jobs.Payload_Import).Import
			return d.Tables[0].Completed()
		}); err != nil {
			return err
		}
		return err
	}
	if temp == "" {
		return nil
	}

	backupDesc := backupccl.BackupDescriptor{
		EndTime: hlc.Timestamp{WallTime: walltime},
	}
	n := rows.Len()
	for i := 0; i < n; i++ {
		row := rows.At(i)
		name := row[0].(*tree.DString)
		size := row[1].(*tree.DInt)
		checksum := row[2].(*tree.DBytes)
		spanStart := row[3].(*tree.DBytes)
		spanEnd := row[4].(*tree.DBytes)
		backupDesc.EntryCounts.DataSize += int64(*size)
		backupDesc.Files = append(backupDesc.Files, backupccl.BackupDescriptor_File{
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
		return err
	}
	es, err := storageccl.MakeExportStorage(ctx, dest, p.ExecCfg().Settings)
	if err != nil {
		return err
	}
	defer es.Close()

	return finalizeCSVBackup(ctx, &backupDesc, parentID, tableDesc, es, p.ExecCfg())
}

var csvOutputTypes = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_BYTES},
	{SemanticType: sqlbase.ColumnType_BYTES},
}

func newReadCSVProcessor(
	flowCtx *distsqlrun.FlowCtx, spec distsqlrun.ReadCSVSpec, output distsqlrun.RowReceiver,
) (distsqlrun.Processor, error) {
	cp := &readCSVProcessor{
		flowCtx:    flowCtx,
		csvOptions: spec.Options,
		sampleSize: spec.SampleSize,
		tableDesc:  spec.TableDesc,
		uri:        spec.Uri,
		output:     output,
		settings:   flowCtx.Settings,
		registry:   flowCtx.JobRegistry,
		progress:   spec.Progress,
	}
	if err := cp.out.Init(&distsqlrun.PostProcessSpec{}, csvOutputTypes, flowCtx.NewEvalCtx(), output); err != nil {
		return nil, err
	}
	return cp, nil
}

type readCSVProcessor struct {
	flowCtx    *distsqlrun.FlowCtx
	csvOptions roachpb.CSVOptions
	sampleSize int32
	tableDesc  sqlbase.TableDescriptor
	uri        map[int32]string
	out        distsqlrun.ProcOutputHelper
	output     distsqlrun.RowReceiver
	settings   *cluster.Settings
	registry   *jobs.Registry
	progress   distsqlrun.JobProgress
}

var _ distsqlrun.Processor = &readCSVProcessor{}

func (cp *readCSVProcessor) OutputTypes() []sqlbase.ColumnType {
	return csvOutputTypes
}

func (cp *readCSVProcessor) Run(wg *sync.WaitGroup) {
	ctx, span := tracing.ChildSpan(cp.flowCtx.Ctx, "readCSVProcessor")
	defer tracing.FinishSpan(span)

	if wg != nil {
		defer wg.Done()
	}

	group, gCtx := errgroup.WithContext(ctx)
	done := gCtx.Done()
	recordCh := make(chan csvRecord)
	kvCh := make(chan []roachpb.KeyValue)
	sampleCh := make(chan sqlbase.EncDatumRow)

	// Read CSV into CSV records
	group.Go(func() error {
		sCtx, span := tracing.ChildSpan(gCtx, "readcsv")
		defer tracing.FinishSpan(span)
		defer close(recordCh)

		job, err := cp.registry.LoadJob(gCtx, cp.progress.JobID)
		if err != nil {
			return err
		}

		progFn := func(pct float32) error {
			return job.Progressed(ctx, func(ctx context.Context, details jobs.Details) float32 {
				d := details.(*jobs.Payload_Import).Import
				slotpct := pct * cp.progress.Contribution
				if len(d.Tables[0].SamplingProgress) > 0 {
					d.Tables[0].SamplingProgress[cp.progress.Slot] = slotpct
				} else {
					d.Tables[0].ReadProgress[cp.progress.Slot] = slotpct
				}
				return d.Tables[0].Completed()
			})
		}

		_, err = readCSV(sCtx, cp.csvOptions.Comma, cp.csvOptions.Comment, cp.csvOptions.Skip,
			len(cp.tableDesc.VisibleColumns()), cp.uri, recordCh, progFn, cp.settings)
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
		for kvBatch := range kvCh {
			for _, kv := range kvBatch {
				if fn(kv) {
					row := sqlbase.EncDatumRow{
						sqlbase.DatumToEncDatum(typeBytes, tree.NewDBytes(tree.DBytes(kv.Key))),
						sqlbase.DatumToEncDatum(typeBytes, tree.NewDBytes(tree.DBytes(kv.Value.RawBytes))),
					}
					select {
					case <-done:
						return sCtx.Err()
					case sampleCh <- row:
					}
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
		distsqlrun.DrainAndClose(ctx, cp.output, err, func(context.Context) {} /* pushTrailingMeta */)
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
		flowCtx:     flowCtx,
		spec:        spec,
		input:       input,
		output:      output,
		tempStorage: flowCtx.TempStorage,
		settings:    flowCtx.Settings,
		registry:    flowCtx.JobRegistry,
		progress:    spec.Progress,
		db:          flowCtx.EvalCtx.Txn.DB(),
	}
	if err := sp.out.Init(&distsqlrun.PostProcessSpec{}, sstOutputTypes, flowCtx.NewEvalCtx(), output); err != nil {
		return nil, err
	}
	return sp, nil
}

type sstWriter struct {
	flowCtx     *distsqlrun.FlowCtx
	spec        distsqlrun.SSTWriterSpec
	input       distsqlrun.RowSource
	out         distsqlrun.ProcOutputHelper
	output      distsqlrun.RowReceiver
	tempStorage engine.Engine
	settings    *cluster.Settings
	registry    *jobs.Registry
	progress    distsqlrun.JobProgress
	db          *client.DB
}

var _ distsqlrun.Processor = &sstWriter{}

func (sp *sstWriter) OutputTypes() []sqlbase.ColumnType {
	return sstOutputTypes
}

func (sp *sstWriter) Run(wg *sync.WaitGroup) {
	ctx, span := tracing.ChildSpan(sp.flowCtx.Ctx, "sstWriter")
	defer tracing.FinishSpan(span)

	if wg != nil {
		defer wg.Done()
	}

	err := func() error {
		job, err := sp.registry.LoadJob(ctx, sp.progress.JobID)
		if err != nil {
			return err
		}

		// Sort incoming KVs, which will be from multiple spans, into a single
		// RocksDB instance.
		types := sp.input.OutputTypes()
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
				if err := ed.EnsureDecoded(&types[i], alloc); err != nil {
					return err
				}
				datum := ed.Datum.(*tree.DBytes)
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

		// Fetch all the keys in each span and write them to storage.
		iter := store.NewIterator()
		defer iter.Close()
		iter.Rewind()
		maxSize := storageccl.MaxImportBatchSize(sp.settings)
		for i, span := range sp.spec.Spans {
			// Since we sampled the CSVs, it is possible for an SST to end up larger
			// than the max raft command size. Split them up into correctly sized chunks.
			contentCh := make(chan sstContent)
			group, gCtx := errgroup.WithContext(ctx)
			group.Go(func() error {
				defer close(contentCh)
				return makeSSTs(gCtx, iter, maxSize, contentCh, sp.spec.WalltimeNanos, span.End, nil)
			})
			group.Go(func() error {
				chunk := -1
				for sst := range contentCh {
					chunk++

					var checksum []byte
					name := span.Name
					if chunk > 0 {
						name = fmt.Sprintf("%d-%s", chunk, name)
					}

					if sp.spec.Destination == "" {
						end := span.End
						if sst.more {
							end = sst.span.EndKey
						}
						if err := sp.db.AdminSplit(gCtx, end, end); err != nil {
							return err
						}

						log.VEventf(gCtx, 1, "scattering key %s", roachpb.PrettyPrintKey(nil, end))
						scatterReq := &roachpb.AdminScatterRequest{
							Span: sst.span,
						}
						if _, pErr := client.SendWrapped(gCtx, sp.db.GetSender(), scatterReq); pErr != nil {
							// TODO(dan): Unfortunately, Scatter is still too unreliable to
							// fail the IMPORT when Scatter fails. I'm uncomfortable that
							// this could break entirely and not start failing the tests,
							// but on the bright side, it doesn't affect correctness, only
							// throughput.
							log.Errorf(gCtx, "failed to scatter span %s: %s", roachpb.PrettyPrintKey(nil, end), pErr)
						}
						if err := storageccl.AddSSTable(gCtx, sp.db, sst.span.Key, sst.span.EndKey, sst.data); err != nil {
							return err
						}
					} else {
						checksum, err = storageccl.SHA512ChecksumData(sst.data)
						if err != nil {
							return err
						}
						conf, err := storageccl.ExportStorageConfFromURI(sp.spec.Destination)
						if err != nil {
							return err
						}
						es, err := storageccl.MakeExportStorage(gCtx, conf, sp.settings)
						if err != nil {
							return err
						}
						err = es.WriteFile(gCtx, name, bytes.NewReader(sst.data))
						es.Close()
						if err != nil {
							return err
						}
					}

					row := sqlbase.EncDatumRow{
						sqlbase.DatumToEncDatum(
							sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING},
							tree.NewDString(name),
						),
						sqlbase.DatumToEncDatum(
							sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT},
							tree.NewDInt(tree.DInt(len(sst.data))),
						),
						sqlbase.DatumToEncDatum(
							sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES},
							tree.NewDBytes(tree.DBytes(checksum)),
						),
						sqlbase.DatumToEncDatum(
							sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES},
							tree.NewDBytes(tree.DBytes(sst.span.Key)),
						),
						sqlbase.DatumToEncDatum(
							sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES},
							tree.NewDBytes(tree.DBytes(sst.span.EndKey)),
						),
					}
					cs, err := sp.out.EmitRow(gCtx, row)
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
				return err
			}

			if err := job.Progressed(ctx, func(ctx context.Context, details jobs.Details) float32 {
				d := details.(*jobs.Payload_Import).Import
				d.Tables[0].WriteProgress[sp.progress.Slot] = float32(i+1) / float32(len(sp.spec.Spans)) * sp.progress.Contribution
				return d.Tables[0].Completed()
			}); err != nil {
				return err
			}
		}
		return nil
	}()
	distsqlrun.DrainAndClose(
		ctx, sp.output, err, func(context.Context) {} /* pushTrailingMeta */, sp.input)
}

type importResumer struct {
	settings *cluster.Settings
	res      roachpb.BulkOpSummary
}

func (r *importResumer) Resume(
	ctx context.Context, job *jobs.Job, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	details := job.Record.Details.(jobs.ImportDetails).Tables[0]
	p := phs.(sql.PlanHookState)
	comma := details.Comma
	comment := details.Comment
	walltime := details.Walltime
	transform := details.BackupPath
	files := details.URIs
	tableDesc := details.Desc
	parentID := details.ParentID
	skip := details.Skip
	sstSize := details.SSTSize
	var nullif *string
	if details.Nullif != nil {
		nullif = &details.Nullif.Nullif
	}

	if sstSize == 0 {
		// The distributed importer will correctly chunk up large ranges into
		// multiple ssts that can be imported. In order to reduce the number of
		// ranges and increase the average range size after import, set a target of
		// some arbitrary multiple larger than the maximum sst size. Without this
		// the range sizes were somewhere between 1MB and > 64MB. Targeting a much
		// higher size should cause many ranges to be somewhere around the max range
		// size. This should also cause the distsql plan and range router to be much
		// smaller since there are fewer overall ranges.
		sstSize = storageccl.MaxImportBatchSize(r.settings) * 5
	}
	return doDistributedCSVTransform(
		ctx, job, files, p, parentID, tableDesc, transform,
		comma, comment, skip, nullif, walltime,
		sstSize,
	)
}

// OnFailOrCancel removes KV data that has been committed from a import that
// has failed or been canceled. It does this by adding the table descriptors
// in DROP state, which causes the schema change stuff to delete the keys
// in the background.
func (r *importResumer) OnFailOrCancel(ctx context.Context, txn *client.Txn, job *jobs.Job) error {
	details := job.Record.Details.(jobs.ImportDetails).Tables[0]
	if details.BackupPath != "" {
		return nil
	}

	// Needed to trigger the schema change manager.
	if err := txn.SetSystemConfigTrigger(); err != nil {
		return err
	}
	b := txn.NewBatch()
	tableDesc := details.Desc
	tableDesc.State = sqlbase.TableDescriptor_DROP
	b.CPut(sqlbase.MakeDescMetadataKey(tableDesc.ID), sqlbase.WrapDescriptor(tableDesc), nil)
	return txn.Run(ctx, b)
}

func (r *importResumer) OnSuccess(ctx context.Context, txn *client.Txn, job *jobs.Job) error {
	log.Event(ctx, "making tables live")
	details := job.Record.Details.(jobs.ImportDetails).Tables[0]

	if details.BackupPath == "" {
		// Write the new TableDescriptors and flip the namespace entries over to
		// them. After this call, any queries on a table will be served by the newly
		// imported data.
		if err := backupccl.WriteTableDescs(ctx, txn, nil, []*sqlbase.TableDescriptor{details.Desc}, job.Record.Username, r.settings); err != nil {
			return errors.Wrapf(err, "creating table %q", details.Desc.Name)
		}
	}

	return nil
}

func (r *importResumer) OnTerminal(
	ctx context.Context, job *jobs.Job, status jobs.Status, resultsCh chan<- tree.Datums,
) {
	details := job.Record.Details.(jobs.ImportDetails).Tables[0]

	if transform := details.BackupPath; transform != "" {
		transformStorage, err := storageccl.ExportStorageFromURI(ctx, transform, r.settings)
		if err != nil {
			log.Warningf(ctx, "unable to create storage: %+v", err)
		} else {
			// Always attempt to cleanup the checkpoint even if the import failed.
			if err := transformStorage.Delete(ctx, backupccl.BackupDescriptorCheckpointName); err != nil {
				log.Warningf(ctx, "unable to delete checkpointed backup descriptor: %+v", err)
			}
			transformStorage.Close()
		}
	}

	if status == jobs.StatusSucceeded {
		resultsCh <- tree.Datums{
			tree.NewDInt(tree.DInt(*job.ID())),
			tree.NewDString(string(jobs.StatusSucceeded)),
			tree.NewDFloat(tree.DFloat(1.0)),
			tree.NewDInt(tree.DInt(r.res.Rows)),
			tree.NewDInt(tree.DInt(r.res.IndexEntries)),
			tree.NewDInt(tree.DInt(r.res.SystemRecords)),
			tree.NewDInt(tree.DInt(r.res.DataSize)),
		}
	}
}

var _ jobs.Resumer = &importResumer{}

func importResumeHook(typ jobs.Type, settings *cluster.Settings) jobs.Resumer {
	if typ != jobs.TypeImport {
		return nil
	}

	return &importResumer{
		settings: settings,
	}
}

func init() {
	sql.AddPlanHook(importPlanHook)
	distsqlrun.NewReadCSVProcessor = newReadCSVProcessor
	distsqlrun.NewSSTWriterProcessor = newSSTWriterProcessor
	jobs.AddResumeHook(importResumeHook)
}
