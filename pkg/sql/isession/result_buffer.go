// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package isession

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/errors"
)

type resultBuffer struct {
	// resultBuffer does not contain a mutex. It does not require syncronization
	// because InternalSession calls Step to run query execution instead of
	// pushing commands to another goroutine and using the resultBuffer to
	// syncronize the query caller and the query executor.

	lastFlushed sql.CmdPos

	// results are command results that have not yet been consumed by the
	// internal session.
	results []*internalCommandResult

	// scratch is a stack of re-usable command result objects.
	scratch []*internalCommandResult
}

func newResultBuffer() *resultBuffer {
	r := &resultBuffer{}
	return r
}

func (r *resultBuffer) Next() (*internalCommandResult, bool) {
	if len(r.results) == 0 {
		return nil, false
	}

	result := r.results[0]
	r.results = r.results[1:]

	return result, true
}

func (r *resultBuffer) newCommand(pos sql.CmdPos) *internalCommandResult {
	var res *internalCommandResult

	if len(r.scratch) > 0 {
		res = r.scratch[len(r.scratch)-1]
		r.scratch = r.scratch[:len(r.scratch)-1]
	} else {
		res = &internalCommandResult{}
	}

	res.pos = pos
	res.resultBuffer = r

	return res
}

func (r *resultBuffer) ready(res *internalCommandResult) {
	r.results = append(r.results, res)
	r.lastFlushed = res.pos
}

func (r *resultBuffer) free(res *internalCommandResult) {
	// Reset the command when adding it to the free list to ensure we don't hold
	// onto any memory pointed to by the command result.
	*res = internalCommandResult{}
	r.scratch = append(r.scratch, res)
}

var _ sql.ClientLock = &resultBuffer{}

func (r *resultBuffer) ClientPos() sql.CmdPos {
	return r.lastFlushed
}

func (r *resultBuffer) Close() {
	// TODO(jeffswenson): why does this method exist if all the implementations
	// do nothing?
}

func (r *resultBuffer) RTrim(ctx context.Context, pos sql.CmdPos) {
	if pos <= r.lastFlushed {
		panic(errors.AssertionFailedf("asked to trim to pos: %d, below the last flush: %d", pos, r.lastFlushed))
	}

	trimTo := len(r.results)

	for i, res := range r.results {
		if pos <= res.pos {
			trimTo = i
			break
		}
	}

	for i := trimTo; i < len(r.results); i++ {
		r.free(r.results[i])
	}

	r.results = r.results[:trimTo]
}

var _ sql.ClientComm = &resultBuffer{}

// CreateStatementResult implements ClientComm.
func (i *resultBuffer) CreateStatementResult(
	stmt tree.Statement,
	descOpt sql.RowDescOpt,
	pos sql.CmdPos,
	formatCodes []pgwirebase.FormatCode,
	conv sessiondatapb.DataConversionConfig,
	location *time.Location,
	limit int,
	portalName string,
	implicitTxn bool,
	portalPausability sql.PortalPausablity,
) sql.CommandResult {
	cmd := i.newCommand(pos)

	return cmd
}

// CreateSyncResult implements ClientComm.
func (i *resultBuffer) CreateSyncResult(pos sql.CmdPos) sql.SyncResult {
	return i.newCommand(pos)
}

// CreatePrepareResult implements ClientComm.
func (i *resultBuffer) CreatePrepareResult(pos sql.CmdPos) sql.ParseResult {
	return i.newCommand(pos)
}

// CreateBindResult implements ClientComm.
func (i *resultBuffer) CreateBindResult(pos sql.CmdPos) sql.BindResult {
	return i.newCommand(pos)
}

// Flush implements ClientComm.
func (i *resultBuffer) Flush(pos sql.CmdPos) error {
	// We may need to implement Flush in the future if we want to support
	// streaming results to the InternalSession. In general, we can probably get
	// by without it. Either by explicitly pagenating large queries or pushing
	// aggregation into the SQL query. Streaming results is undesirable because
	// it leads to weird txn restart rules.
	return nil
}

// LockCommunication implements ClientComm.
func (i *resultBuffer) LockCommunication() sql.ClientLock {
	return i
}

// CreateCopyInResult implements ClientComm.
func (i *resultBuffer) CreateCopyInResult(cmd sql.CopyIn, pos sql.CmdPos) sql.CopyInResult {
	return i.newCommand(pos)
}

// CreateCopyOutResult implements ClientComm.
func (i *resultBuffer) CreateCopyOutResult(cmd sql.CopyOut, pos sql.CmdPos) sql.CopyOutResult {
	return i.newCommand(pos)
}

// CreateDeleteResult implements ClientComm.
func (i *resultBuffer) CreateDeleteResult(pos sql.CmdPos) sql.DeleteResult {
	return i.newCommand(pos)
}

// CreateDescribeResult implements ClientComm.
func (i *resultBuffer) CreateDescribeResult(pos sql.CmdPos) sql.DescribeResult {
	return i.newCommand(pos)
}

// CreateDrainResult implements ClientComm.
func (i *resultBuffer) CreateDrainResult(pos sql.CmdPos) sql.DrainResult {
	return i.newCommand(pos)
}

// CreateEmptyQueryResult implements ClientComm.
func (i *resultBuffer) CreateEmptyQueryResult(pos sql.CmdPos) sql.EmptyQueryResult {
	return i.newCommand(pos)
}

// CreateErrorResult implements ClientComm.
func (i *resultBuffer) CreateErrorResult(pos sql.CmdPos) sql.ErrorResult {
	return i.newCommand(pos)
}

// CreateFlushResult implements ClientComm.
func (i *resultBuffer) CreateFlushResult(pos sql.CmdPos) sql.FlushResult {
	return i.newCommand(pos)
}
