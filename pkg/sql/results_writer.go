// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// WireFailureError is used when sending data over pgwire fails.
type WireFailureError struct {
	err error
}

func (e WireFailureError) Error() string {
	return fmt.Sprintf("WireFailureError: %s", e.err.Error())
}

// NewWireFailureError returns a new WireFailureError which wraps err.
func NewWireFailureError(err error) error {
	return WireFailureError{err}
}

// ResultsWriter is the interface used to by the Executor to produce results for
// query execution for a SQL client. The main implementer is v3Conn, which
// streams the results on a SQL network connection. There's also bufferedWriter,
// which buffers all results in memory.
//
// ResultsWriter is built with the SQL session model in mind: queries from a
// given SQL client (which we'll call the consumer to not confuse it with
// clients of this interface - the Executor) keep coming out of band and all of
// their results (generally, datum tuples) are pushed to a single ResultsWriter.
// The ResultsWriter needs to be made aware of which results pertain to which
// statement, as implementations need to split results accordingly. The
// ResultsWriter also supports the notion of a "results group": the
// ResultsWriter sequentially goes through groups of results and the group is
// the level at which a client can request for results to be dropped; a group
// can be reset, meaning that the consumer will not receive any of them. Only
// the current group can be reset; the client gives up the ability to reset a
// group the moment it closes it.  This feature is used to support the automatic
// retries that we do for SQL transactions - groups will correspond to
// transactions and, when the Executor decides to automatically retry a
// transaction, it will reset its group (as it can't automatically retry if any
// results have been sent to the consumer).
//
// Example usage:
//
//  var rw ResultsWriter
//  group := rw.NewResultsGroup()
//  defer group.Close()
//  sr := group.NewStatementResult()
//  for each result row {
//    if err := sr.AddRow(...); err != nil {
//      // send err to the client in another way
//    }
//  }
//  sr.CloseResult()
//  group.Close()
//
type ResultsWriter interface {
	// NewResultsGroup creates a new ResultGroup and indicates that future results
	// are part of a new result group.
	//
	// A single group can be ongoing on a ResultsWriter at a time; it is illegal to
	// create a new group before the previous one has been Close()d.
	NewResultsGroup() ResultsGroup

	// SetEmptyQuery is used to indicate that there are no statements to run.
	// Empty queries are different than queries with no results.
	SetEmptyQuery()
}

// ResultsGroup is used to produce a result group (see ResultsWriter).
type ResultsGroup interface {
	// Close should be called once all the results for a group have been produced
	// (i.e. after all StatementResults have been closed). It informs the
	// implementation that the results are not going to be reset any more, and
	// thus can be sent to the client.
	//
	// Close has to be called before new groups are created.  It's illegal to call
	// Close before CloseResult() has been called on all of the group's
	// StatementResults.
	Close()

	// NewStatementResult creates a new StatementResult, indicating that future
	// results are part of a new SQL query.
	//
	// A single StatementResult can be active on a ResultGroup at a time; it is
	// illegal to create a new StatementResult before CloseResult() has been
	// called on the previous one.
	NewStatementResult() StatementResult

	// Flush informs the ResultsGroup that the caller relinquishes the capability
	// to Reset() the results that have been already been accumulated on this
	// group. This means that future Reset() calls will only reset up to the
	// current point in the stream - only future results will be discarded. This
	// is used to ensure that some results are always sent to the client even if
	// further statements are retried automatically; it supports the statements
	// run in the AutoRetry state: these statements are not executed again when
	// doing an automatic retry, and so their results shouldn't be reset.
	//
	// It is illegal to call this while any StatementResults on this group are
	// open.
	//
	// Like StatementResult.AddRow(), Flush returns communication errors, if any.
	// TODO(andrei): provide guidance on handling these errors.
	Flush(context.Context) error

	// ResultsSentToClient returns true if any results pertaining to this group
	// beyond the last Flush() point have been sent to the consumer.
	// Remember that the implementation is free to buffer or send results to the
	// client whenever it pleases. This method checks to see if the implementation
	// has in fact sent anything so far.
	//
	// TODO(andrei): add a note about the synchronous nature of the implementation
	// imposed by this interface.
	ResultsSentToClient() bool

	// Reset discards all the accumulated results from the last Flush() call
	// onwards (or from the moment the group was created if Flush() was never
	// called).
	// It is illegal to call Reset if any results have already been sent to the
	// consumer; this can be tested with ResultsSentToClient().
	Reset(context.Context)
}

// StatementResult is used to produce results for a single query (see
// ResultsWriter).
type StatementResult interface {
	// BeginResult should be called prior to any of the other methods.
	// TODO(andrei): remove BeginResult and SetColumns, and have
	// NewStatementResult() take in a tree.Statement. But that might not work
	// because a statement's tag might depend on the state that it's being
	// executed in?
	BeginResult(stmt tree.Statement)
	// GetPGTag returns the PGTag of the statement passed into BeginResult.
	PGTag() string
	// GetStatementType returns the StatementType that corresponds to the type of
	// results that should be sent to this interface.
	StatementType() tree.StatementType
	// SetColumns should be called after BeginResult and before AddRow if the
	// StatementType is tree.Rows.
	SetColumns(columns sqlbase.ResultColumns)
	// AddRow takes the passed in row and adds it to the current result. If an
	// error is returned, it is a communication error; in this case the only
	// further allowed call on the ResultWriter set of interfaces is
	// ResultsGroup.Close(). In particular, StatementResult.CloseResult() cannot
	// be called.
	AddRow(ctx context.Context, row tree.Datums) error
	// IncrementRowsAffected increments a counter by n. This is used for all
	// result types other than tree.Rows.
	IncrementRowsAffected(n int)
	// RowsAffected returns either the number of times AddRow was called, or the
	// sum of all n passed into IncrementRowsAffected.
	RowsAffected() int
	// CloseResult ends the current result. The v3Conn will send control codes to
	// the client informing it that the result for a statement is now complete.
	//
	// CloseResult cannot be called unless there's a corresponding BeginResult
	// prior.
	CloseResult() error

	// SetError allows an error to be  stored on the StatementResult.
	SetError(err error)
	// Err returns the error previously set with SetError(), if any.
	Err() error
}

type bufferedWriter struct {
	acc mon.BoundAccount

	// pastResults spans transactions.
	pastResults ResultList

	// currentGroupResults spans a transaction.
	currentGroupResults ResultList

	// currentResult and resultInProgress spans a statement.
	currentResult    Result
	resultInProgress bool

	err error
}

func newBufferedWriter(acc mon.BoundAccount) *bufferedWriter {
	return &bufferedWriter{acc: acc}
}

// SetError is part of the ResultsWriter interface.
func (b *bufferedWriter) SetError(err error) {
	b.err = err
}

// Err is part of the ResultsWriter interface.
func (b *bufferedWriter) Err() error {
	return b.err
}

func (b *bufferedWriter) results() StatementResults {
	b.pastResults = append(b.pastResults, b.currentGroupResults...)
	return StatementResults{b.pastResults, len(b.pastResults) == 0}
}

// NewResultsGroup is part of the ResultsWriter interface.
func (b *bufferedWriter) NewResultsGroup() ResultsGroup {
	return b
}

// SetEmptyQuery is part of the ResultsWriter interface.
func (b *bufferedWriter) SetEmptyQuery() {
}

// NewStatementResult is part of the ResultsGroup interface.
func (b *bufferedWriter) NewStatementResult() StatementResult {
	return b
}

// ResultsSentToClient implements the ResultsGroup interface.
func (b *bufferedWriter) ResultsSentToClient() bool {
	return false
}

// Close implements the ResultsGroup interface.
func (b *bufferedWriter) Close() {
	// TODO(andrei): The work that's duplicated from CloseResult() should not be
	// performed by this method.
	if b.resultInProgress {
		b.currentGroupResults = append(b.currentGroupResults, b.currentResult)
		b.resultInProgress = false
	}
	b.pastResults = append(b.pastResults, b.currentGroupResults...)
	b.currentGroupResults = nil
	b.resultInProgress = false
}

// Flush implements the ResultsGroup interface.
func (b *bufferedWriter) Flush(context.Context) error {
	if b.resultInProgress {
		panic("can't flush while a StatementResult is in progress")
	}
	b.pastResults = append(b.pastResults, b.currentGroupResults...)
	b.currentGroupResults = nil
	return nil
}

// Reset implements the ResultsGroup interface.
func (b *bufferedWriter) Reset(ctx context.Context) {
	if b.currentGroupResults != nil {
		b.currentGroupResults.Close(ctx)
		b.currentGroupResults = nil
	}
	b.resultInProgress = false
}

// BeginResult implements the StatementResult interface.
func (b *bufferedWriter) BeginResult(stmt tree.Statement) {
	if b.resultInProgress {
		panic("can't start new result before ending the previous")
	}
	b.resultInProgress = true
	b.currentResult = Result{PGTag: stmt.StatementTag(), Type: stmt.StatementType()}
}

// GetPGTag implements the StatementResult interface.
func (b *bufferedWriter) PGTag() string {
	return b.currentResult.PGTag
}

// SetColumns implements the StatementResult interface.
func (b *bufferedWriter) SetColumns(columns sqlbase.ResultColumns) {
	if !b.resultInProgress {
		panic("no result in progress")
	}
	b.currentResult.Columns = columns

	if b.currentResult.Type == tree.Rows {
		b.currentResult.Rows = sqlbase.NewRowContainer(
			b.acc, sqlbase.ColTypeInfoFromResCols(columns), 0,
		)
	}
}

// RowsAffected implements the StatementResult interface.
func (b *bufferedWriter) RowsAffected() int {
	if b.currentResult.Type == tree.Rows {
		return b.currentResult.Rows.Len()
	}
	return b.currentResult.RowsAffected
}

// CloseResult implements the StatementResult interface.
func (b *bufferedWriter) CloseResult() error {
	if !b.resultInProgress {
		panic("no result in progress")
	}
	b.currentGroupResults = append(b.currentGroupResults, b.currentResult)
	b.resultInProgress = false
	return nil
}

// StatementType implements the StatementResult interface.
func (b *bufferedWriter) StatementType() tree.StatementType {
	return b.currentResult.Type
}

// IncrementRowsAffected implements the StatementResult interface.
func (b *bufferedWriter) IncrementRowsAffected(n int) {
	if !b.resultInProgress {
		panic("no result in progress")
	}
	b.currentResult.RowsAffected += n
}

// AddRow implements the StatementResult interface.
func (b *bufferedWriter) AddRow(ctx context.Context, row tree.Datums) error {
	if !b.resultInProgress {
		panic("no result in progress")
	}
	_, err := b.currentResult.Rows.AddRow(ctx, row)
	return err
}
