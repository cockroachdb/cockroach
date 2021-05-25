// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
)

const (
	// NodelocalFileUploadTable is used internally to identify a COPY initiated by
	// nodelocal upload.
	NodelocalFileUploadTable = "nodelocal_file_upload"
	// UserFileUploadTable is used internally to identify a COPY initiated by
	// userfile upload.
	UserFileUploadTable = "user_file_upload"
)

var _ copyMachineInterface = &fileUploadMachine{}

type fileUploadMachine struct {
	c              *copyMachine
	w              io.WriteCloser
	cancel         func()
	failureCleanup func()
}

var _ io.ReadSeeker = &noopReadSeeker{}

type noopReadSeeker struct {
	*io.PipeReader
}

func (n *noopReadSeeker) Seek(int64, int) (int64, error) {
	return 0, errors.New("illegal seek")
}

func checkIfFileExists(ctx context.Context, c *copyMachine, dest, copyTargetTable string) error {
	if copyTargetTable == UserFileUploadTable {
		dest = strings.TrimSuffix(dest, ".tmp")
	}
	store, err := c.p.execCfg.DistSQLSrv.ExternalStorageFromURI(ctx, dest, c.p.User())
	if err != nil {
		return err
	}
	defer store.Close()

	_, err = store.ReadFile(ctx, "")
	if err == nil {
		// Can ignore this parse error as it would have been caught when creating a
		// new ExternalStorage above and so we never expect it to non-nil.
		uri, _ := url.Parse(dest)
		return errors.Newf("destination file already exists for %s", uri.Path)
	}

	return nil
}

func newFileUploadMachine(
	ctx context.Context,
	conn pgwirebase.Conn,
	n *tree.CopyFrom,
	txnOpt copyTxnOpt,
	execCfg *ExecutorConfig,
) (f *fileUploadMachine, retErr error) {
	if len(n.Columns) != 0 {
		return nil, errors.New("expected 0 columns specified for file uploads")
	}
	c := &copyMachine{
		conn: conn,
		// The planner will be prepared before use.
		p: planner{execCfg: execCfg, alloc: &rowenc.DatumAlloc{}},
	}
	f = &fileUploadMachine{
		c: c,
	}

	// We need a planner to do the initial planning, even if a planner
	// is not required after that.
	cleanup := c.p.preparePlannerForCopy(ctx, txnOpt)
	defer func() {
		retErr = cleanup(ctx, retErr)
	}()
	c.parsingEvalCtx = c.p.EvalContext()

	if n.Table.Table() == NodelocalFileUploadTable {
		if err := c.p.RequireAdminRole(ctx, "upload to nodelocal"); err != nil {
			return nil, err
		}
	}

	if n.Options.Destination == nil {
		return nil, errors.Newf("destination required")
	}
	destFn, err := f.c.p.TypeAsString(ctx, n.Options.Destination, "COPY")
	if err != nil {
		return nil, err
	}
	dest, err := destFn()
	if err != nil {
		return nil, err
	}

	store, err := c.p.execCfg.DistSQLSrv.ExternalStorageFromURI(ctx, dest, c.p.User())
	if err != nil {
		return nil, err
	}

	err = checkIfFileExists(ctx, c, dest, n.Table.Table())
	if err != nil {
		return nil, err
	}

	writeCtx, canecelWriteCtx := context.WithCancel(ctx)
	f.cancel = canecelWriteCtx
	f.w, err = store.Writer(writeCtx, "")
	if err != nil {
		return nil, err
	}

	f.failureCleanup = func() {
		// Ignoring this error because deletion would only fail
		// if the file was not created in the first place.
		_ = store.Delete(ctx, "")
	}

	c.resultColumns = make(colinfo.ResultColumns, 1)
	c.resultColumns[0] = colinfo.ResultColumn{Typ: types.Bytes}
	c.parsingEvalCtx = c.p.EvalContext()
	c.rowsMemAcc = c.p.extendedEvalCtx.Mon.MakeBoundAccount()
	c.bufMemAcc = c.p.extendedEvalCtx.Mon.MakeBoundAccount()
	c.processRows = f.writeFile
	c.forceNotNull = true
	c.format = tree.CopyFormatText
	c.null = `\N`
	c.delimiter = '\t'
	return
}

// CopyInFileStmt creates a COPY FROM statement which can be used
// to upload files, and be prepared with Tx.Prepare().
func CopyInFileStmt(destination, schema, table string) string {
	return fmt.Sprintf(
		`COPY %s.%s FROM STDIN WITH destination = %s`,
		pq.QuoteIdentifier(schema),
		pq.QuoteIdentifier(table),
		lex.EscapeSQLString(destination),
	)
}

func (f *fileUploadMachine) run(ctx context.Context) error {
	err := f.c.run(ctx)
	if err != nil && f.cancel != nil {
		f.cancel()
	}
	err = errors.CombineErrors(f.w.Close(), err)

	if err != nil {
		f.failureCleanup()
	}
	return err
}

func (f *fileUploadMachine) writeFile(ctx context.Context) error {
	for _, r := range f.c.rows {
		b := []byte(*r[0].(*tree.DBytes))
		n, err := f.w.Write(b)
		if err != nil {
			return err
		}
		if n < len(b) {
			return io.ErrShortWrite
		}
	}

	// Issue a final zero-byte write to ensure we observe any errors in the pipe.
	_, err := f.w.Write(nil)
	if err != nil {
		return err
	}
	f.c.insertedRows += len(f.c.rows)
	f.c.rows = f.c.rows[:0]
	f.c.rowsMemAcc.Clear(ctx)
	return nil
}
