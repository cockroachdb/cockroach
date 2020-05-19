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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
)

const (
	fileUploadTable = "file_upload"
	copyOptionDest  = "destination"
)

var copyFileOptionExpectValues = map[string]KVStringOptValidate{
	copyOptionDest: KVStringOptRequireValue,
}

var _ copyMachineInterface = &fileUploadMachine{}

type fileUploadMachine struct {
	c              *copyMachine
	writeToFile    *io.PipeWriter
	wg             *sync.WaitGroup
	failureCleanup func()
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
		p: planner{execCfg: execCfg, alloc: &sqlbase.DatumAlloc{}},
	}
	f = &fileUploadMachine{
		c:  c,
		wg: &sync.WaitGroup{},
	}

	// We need a planner to do the initial planning, even if a planner
	// is not required after that.
	cleanup := c.p.preparePlannerForCopy(ctx, txnOpt)
	defer func() {
		retErr = cleanup(ctx, retErr)
	}()
	c.parsingEvalCtx = c.p.EvalContext()

	if err := c.p.RequireAdminRole(ctx, "upload to nodelocal"); err != nil {
		return nil, err
	}

	optsFn, err := f.c.p.TypeAsStringOpts(ctx, n.Options, copyFileOptionExpectValues)
	if err != nil {
		return nil, err
	}
	opts, err := optsFn()
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	localStorage, err := blobs.NewLocalStorage(c.p.execCfg.Settings.ExternalIODir)
	if err != nil {
		return nil, err
	}
	// Check that the file does not already exist
	_, err = localStorage.Stat(opts[copyOptionDest])
	if err == nil {
		return nil, fmt.Errorf("destination file already exists for %s", opts[copyOptionDest])
	}
	f.wg.Add(1)
	go func() {
		err := localStorage.WriteFile(opts[copyOptionDest], pr)
		if err != nil {
			_ = pr.CloseWithError(err)
		}
		f.wg.Done()
	}()
	f.writeToFile = pw
	f.failureCleanup = func() {
		// Ignoring this error because deletion would only fail
		// if the file was not created in the first place.
		_ = localStorage.Delete(opts[copyOptionDest])
	}

	c.resultColumns = make(sqlbase.ResultColumns, 1)
	c.resultColumns[0] = sqlbase.ResultColumn{Typ: types.Bytes}
	c.parsingEvalCtx = c.p.EvalContext()
	c.rowsMemAcc = c.p.extendedEvalCtx.Mon.MakeBoundAccount()
	c.bufMemAcc = c.p.extendedEvalCtx.Mon.MakeBoundAccount()
	c.processRows = f.writeFile
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

func (f *fileUploadMachine) run(ctx context.Context) (err error) {
	err = f.c.run(ctx)
	_ = f.writeToFile.Close()
	if err != nil {
		f.failureCleanup()
	}
	f.wg.Wait()
	return
}

func (f *fileUploadMachine) writeFile(ctx context.Context) error {
	for _, r := range f.c.rows {
		b := []byte(*r[0].(*tree.DBytes))
		n, err := f.writeToFile.Write(b)
		if err != nil {
			return err
		}
		if n < len(b) {
			return io.ErrShortWrite
		}
	}

	// Issue a final zero-byte write to ensure we observe any errors in the pipe.
	_, err := f.writeToFile.Write(nil)
	if err != nil {
		return err
	}
	f.c.insertedRows += len(f.c.rows)
	f.c.rows = f.c.rows[:0]
	f.c.rowsMemAcc.Clear(ctx)
	return nil
}
