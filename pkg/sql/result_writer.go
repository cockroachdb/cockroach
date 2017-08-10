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
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"golang.org/x/net/context"
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

// ResultWriter is an interface which is used to store results from query
// execution. Implementations could be buffered, immediately stream to the
// client, or a bit of both. There are two implementations: v3conn and
// bufferedWriter.
type ResultWriter interface {
	NewGroupResultWriter() GroupResultWriter
	// SetEmptyQuery is used to indicate that there are no statements to run.
	// Empty queries are different than queries with no results.
	SetEmptyQuery()
}

// GroupResultWriter provides an interface for a single transaction. Its methods
// deal with automatic retries primarily.
type GroupResultWriter interface {
	NewStatementResultWriter() StatementResultWriter
	// ResultsSentToClient returns true if this group has sent any results to the
	// client in the current transaction.
	ResultsSentToClient() bool
	// End should be called before new groups can be created.
	End()
	// Reset discards all the current buffered results (if any) when we attempt
	// to automatically retry the current transaction.
	Reset(ctx context.Context)
}

// StatementResultWriter provides a writer interface for a single statement.
type StatementResultWriter interface {
	// BeginResult should be called prior to any of the other methods.
	// TODO(andrei): remove BeginResult and SetColumns, and have
	// NewStatementResultWriter take in a parser.Statement
	BeginResult(stmt parser.Statement)
	// GetPGTag returns the PGTag of the statement passed into BeginResult.
	PGTag() string
	// GetStatementType returns the StatementType that corresponds to the type of
	// results that should be sent to this interface.
	StatementType() parser.StatementType
	// SetColumns should be called after BeginResult and before AddRow if the
	// StatementType is parser.Rows.
	SetColumns(columns sqlbase.ResultColumns)
	// AddRow takes the passed in row and adds it to the current result.
	AddRow(ctx context.Context, row parser.Datums) error
	// IncrementRowsAffected increments a counter by n. This is used for all
	// result types other than parser.Rows.
	IncrementRowsAffected(n int)
	// RowsAffected returns either the number of times AddRow was called, or the
	// sum of all n passed into IncrementRowsAffected.
	RowsAffected() int
	// EndResult ends the current result. Cannot be called unless there's a
	// corresponding BeginResult prior.
	EndResult() error
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
}

func newBufferedWriter(acc mon.BoundAccount) *bufferedWriter {
	return &bufferedWriter{acc: acc}
}

func (b *bufferedWriter) results() StatementResults {
	b.pastResults = append(b.pastResults, b.currentGroupResults...)
	return StatementResults{b.pastResults, len(b.pastResults) == 0}
}

// NewGroupResultWriter implements the ResultWriter interface.
func (b *bufferedWriter) NewGroupResultWriter() GroupResultWriter {
	return b
}

// SetEmptyQuery implements the ResultWriter interface.
func (b *bufferedWriter) SetEmptyQuery() {
}

// SetEmptyQuery implements the GroupResultWriter interface.
func (b *bufferedWriter) NewStatementResultWriter() StatementResultWriter {
	return b
}

// CanAutomaticallyRetry implements the GroupResultWriter interface.
func (b *bufferedWriter) ResultsSentToClient() bool {
	return false
}

// End implements the GroupResultWriter interface.
func (b *bufferedWriter) End() {
	if b.resultInProgress {
		b.currentGroupResults = append(b.currentGroupResults, b.currentResult)
		b.resultInProgress = false
	}
	b.pastResults = append(b.pastResults, b.currentGroupResults...)
	b.currentGroupResults = nil
	b.resultInProgress = false
}

// Reset implements the GroupResultWriter interface.
func (b *bufferedWriter) Reset(ctx context.Context) {
	if b.currentGroupResults != nil {
		b.currentGroupResults.Close(ctx)
	}
	b.resultInProgress = false
}

// BeginResult implements the StatementResultWriter interface.
func (b *bufferedWriter) BeginResult(stmt parser.Statement) {
	if b.resultInProgress {
		panic("can't start new result before ending the previous")
	}
	b.resultInProgress = true
	b.currentResult = Result{PGTag: stmt.StatementTag(), Type: stmt.StatementType()}
}

// GetPGTag implements the StatementResultWriter interface.
func (b *bufferedWriter) PGTag() string {
	return b.currentResult.PGTag
}

// SetColumns implements the StatementResultWriter interface.
func (b *bufferedWriter) SetColumns(columns sqlbase.ResultColumns) {
	if !b.resultInProgress {
		panic("no result in progress")
	}
	b.currentResult.Columns = columns

	if b.currentResult.Type == parser.Rows {
		b.currentResult.Rows = sqlbase.NewRowContainer(
			b.acc, sqlbase.ColTypeInfoFromResCols(columns), 0,
		)
	}
}

// RowsAffected implements the StatementResultWriter interface.
func (b *bufferedWriter) RowsAffected() int {
	if b.currentResult.Type == parser.Rows {
		return b.currentResult.Rows.Len()
	}
	return b.currentResult.RowsAffected
}

// EndResult implements the StatementResultWriter interface.
func (b *bufferedWriter) EndResult() error {
	if !b.resultInProgress {
		panic("no result in progress")
	}
	b.currentGroupResults = append(b.currentGroupResults, b.currentResult)
	b.resultInProgress = false
	return nil
}

// StatementType implements the StatementResultWriter interface.
func (b *bufferedWriter) StatementType() parser.StatementType {
	return b.currentResult.Type
}

// IncrementRowsAffected implements the StatementResultWriter interface.
func (b *bufferedWriter) IncrementRowsAffected(n int) {
	if !b.resultInProgress {
		panic("no result in progress")
	}
	b.currentResult.RowsAffected += n
}

// AddRow implements the StatementResultWriter interface.
func (b *bufferedWriter) AddRow(ctx context.Context, row parser.Datums) error {
	if !b.resultInProgress {
		panic("no result in progress")
	}
	_, err := b.currentResult.Rows.AddRow(ctx, row)
	return err
}
