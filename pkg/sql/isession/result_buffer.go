// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package isession

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type resultBuffer struct {
	syncutil.Mutex
	lastFlushed sql.CmdPos
	cond        sync.Cond

	results []*internalCommandResult
	scratch []*internalCommandResult
}

func newResultBuffer() *resultBuffer {
	r := &resultBuffer{}
	r.cond.L = &r.Mutex
	return r
}

func (r *resultBuffer) Next() (*internalCommandResult, bool) {
	r.Lock()
	defer r.Unlock()

	for len(r.results) == 0 {
		return nil, false
	}

	result := r.results[0]
	r.results = r.results[1:]

	return result, true
}

func (r *resultBuffer) newCommand(pos sql.CmdPos) *internalCommandResult {
	r.Lock()
	defer r.Unlock()

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
	r.Lock()
	defer r.Unlock()

	r.results = append(r.results, res)
	r.lastFlushed = res.pos
	r.cond.Broadcast()
}

func (r *resultBuffer) freeLocked(res *internalCommandResult) {
	*res = internalCommandResult{}
	r.scratch = append(r.scratch, res)
}

var _ sql.ClientLock = &resultBuffer{}

func (r *resultBuffer) ClientPos() sql.CmdPos {
	r.Lock()
	defer r.Unlock()
	return r.lastFlushed
}

func (r *resultBuffer) Close() {
	// No-op.
	// TODO(jeffswenson): why does this method exist if all the implementations
	// do nothing?
}

func (r *resultBuffer) RTrim(ctx context.Context, pos sql.CmdPos) {
	r.Lock()
	defer r.Unlock()
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
		r.freeLocked(r.results[i])
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
