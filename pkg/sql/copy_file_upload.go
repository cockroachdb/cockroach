// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
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

	_, _, err = store.ReadFile(ctx, "", cloud.ReadOptions{})
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
	p *planner,
	parentMon *mon.BytesMonitor,
) (f *fileUploadMachine, retErr error) {
	if len(n.Columns) != 0 {
		return nil, errors.New("expected 0 columns specified for file uploads")
	}
	c := &copyMachine{
		conn:   conn,
		txnOpt: txnOpt,
		// The planner will be prepared before use.
		p: p,
	}
	f = &fileUploadMachine{
		c: c,
	}

	// We need a planner to do the initial planning, even if a planner
	// is not required after that.
	c.txnOpt.initPlanner(ctx, c.p)
	cleanup := c.p.preparePlannerForCopy(ctx, &c.txnOpt, false /* finalBatch */, c.implicitTxn)
	defer func() {
		retErr = cleanup(ctx, retErr)
	}()
	c.parsingEvalCtx = c.p.EvalContext()

	if n.Table.Table() == NodelocalFileUploadTable {
		if hasAdmin, err := p.HasAdminRole(ctx); err != nil {
			return nil, err
		} else if !hasAdmin {
			return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
				"only users with the admin role are allowed to upload to nodelocal")
		}
	}

	if n.Options.Destination == nil {
		return nil, errors.Newf("destination required")
	}
	dest, err := f.c.p.ExprEvaluator("COPY").String(ctx, n.Options.Destination)
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
	c.initMonitoring(ctx, parentMon)
	c.processRows = f.writeFile
	c.forceNotNull = true
	c.format = tree.CopyFormatText
	c.null = `\N`
	c.delimiter = '\t'
	c.rows.Init(c.rowsMemAcc, colinfo.ColTypeInfoFromResCols(c.resultColumns), CopyBatchRowSize)
	c.scratchRow = make(tree.Datums, len(c.resultColumns))
	return
}

// CopyInFileStmt creates a COPY FROM statement which can be used
// to upload files, and be prepared with Tx.Prepare().
func CopyInFileStmt(destination, schema, table string) string {
	return fmt.Sprintf(
		`COPY %s.%s FROM STDIN WITH destination = %s`,
		pq.QuoteIdentifier(schema),
		pq.QuoteIdentifier(table),
		lexbase.EscapeSQLString(destination),
	)
}

func (f *fileUploadMachine) numInsertedRows() int {
	if f == nil {
		return 0
	}
	return f.c.numInsertedRows()
}

func (f *fileUploadMachine) Close(ctx context.Context) {
	f.c.Close(ctx)
}

func (f *fileUploadMachine) run(ctx context.Context) error {
	runErr := f.c.run(ctx)
	err := errors.CombineErrors(f.w.Close(), runErr)
	if runErr != nil && f.cancel != nil {
		f.cancel()
	}

	if err != nil {
		f.failureCleanup()
	}
	return err
}

func (f *fileUploadMachine) writeFile(ctx context.Context, finalBatch bool) error {
	for i := 0; i < f.c.rows.Len(); i++ {
		r := f.c.rows.At(i)
		b := r[0].(*tree.DBytes).UnsafeBytes()
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
	return f.c.doneWithRows(ctx)
}
