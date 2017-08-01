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
//
// Author: Tristan Ohlson (tsohlson@gmail.com)

package sql

import (
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"golang.org/x/net/context"
)

// WireFailureError is used when sending data over pgwire fails.
type WireFailureError struct {
	err error
}

func (e WireFailureError) Error() string {
	return fmt.Sprintf("pgwire: unable to write to connection: %s", e.err.Error())
}

// NewWireFailureError returns a new WireFailureError which wraps err.
func NewWireFailureError(err error) WireFailureError {
	return WireFailureError{err}
}

// StatementResultWriter provides a writer interface for a single statement.
type StatementResultWriter interface {
	// BeginResult should be called prior to any of the other methods.
	BeginResult(stmt parser.Statement)
	// GetPGTag returns the PGTag of the stmt passed into BeginResult.
	GetPGTag() string
	// GetStatementType returns the StatementType of the stmt passed into
	// BeginResult.
	GetStatementType() parser.StatementType
	// SetColumns should be called after BeginResult and before AddRow if the
	// StatementType is parser.Rows.
	SetColumns(columns sqlbase.ResultColumns)
	// IncrementRowsAffected increments a counter. This is used for
	// parser.RowsAffected and parser.DDL.
	IncrementRowsAffected(n int)
	// AddRow takes the passed in row and adds it to the current result.
	AddRow(ctx context.Context, row parser.Datums) error
	// GetRowsAffected returns either the number of times AddRow was called, or
	// the sum of all n passed into IncrementRowsAffected.
	GetRowsAffected() int
	// EndResult ends the current result. Cannot be called unless there's a
	// corresponding BeginResult prior.
	EndResult() error
	// SetError sets an error. Depending on the implementation all other functions
	// become no-ops.
	SetError(err error) error
}

// GroupResultWriter provides an interface for a single transaction. Its methods
// deal with automatic retries primarily.
type GroupResultWriter interface {
	StatementResultWriter
	// CanAutomaticallyRetry returns true if we have not sent any results to the
	// client in the current transaction.
	CanAutomaticallyRetry() bool
	// ResetForNewTransaction is called when a new transaction begins. We reset
	// state, and if this isn't the first transaction finalize the results of the
	// previous either by sending them or putting them away somewhere.
	ResetForNewTransaction() error
	// ResetForRetry throws all the current buffered results (if any) so that we
	// can automatically retry our transaction.
	ResetForRetry()
}

// ResultWriter is an interface which is used to store results from query
// execution. Implementations could be buffered, immediately stream to the
// client, or a bit of both. There are two implementations: v3conn and
// bufferedWriter.
type ResultWriter interface {
	GroupResultWriter
	// GetError returns either nil, or the error from SetError. Used for testing.
	GetError() error
	// SetEmptyQuery is used to indicate that there are no statements to run.
	// Empty queries are different than queries with no results.
	SetEmptyQuery()
}

type bufferedWriter struct {
	session *Session

	// pastResults spans transactions
	pastResults ResultList

	// currentGroupResults spans a transaction
	currentGroupResults ResultList

	// currentResult and resultInProgress spans a statement
	currentResult    Result
	resultInProgress bool
}

func newBufferedWriter(s *Session) *bufferedWriter {
	return &bufferedWriter{session: s}
}

func (b *bufferedWriter) BeginResult(stmt parser.Statement) {
	if b.resultInProgress {
		panic("can't start new result before ending the previous")
	}
	b.resultInProgress = true
	b.currentResult = Result{PGTag: stmt.StatementTag(), Type: stmt.StatementType()}
}

func (b *bufferedWriter) GetPGTag() string {
	return b.currentResult.PGTag
}

func (b *bufferedWriter) GetStatementType() parser.StatementType {
	return b.currentResult.Type
}

func (b *bufferedWriter) SetColumns(columns sqlbase.ResultColumns) {
	if !b.resultInProgress {
		panic("no result in progress")
	}
	b.currentResult.Columns = columns

	if b.currentResult.Type == parser.Rows {
		b.currentResult.Rows = sqlbase.NewRowContainer(
			b.session.makeBoundAccount(), sqlbase.ColTypeInfoFromResCols(columns), 0,
		)
	}
}

func (b *bufferedWriter) IncrementRowsAffected(n int) {
	if !b.resultInProgress {
		panic("no result in progress")
	}
	b.currentResult.RowsAffected += n
}

func (b *bufferedWriter) AddRow(ctx context.Context, row parser.Datums) error {
	if !b.resultInProgress {
		panic("no result in progress")
	}
	_, err := b.currentResult.Rows.AddRow(ctx, row)
	return err
}

func (b *bufferedWriter) GetRowsAffected() int {
	if b.currentResult.Type == parser.Rows {
		return b.currentResult.Rows.Len()
	}
	return b.currentResult.RowsAffected
}

func (b *bufferedWriter) EndResult() error {
	if !b.resultInProgress {
		panic("no result in progress")
	}
	b.resultInProgress = false
	b.currentGroupResults = append(b.currentGroupResults, b.currentResult)
	return nil
}

func (b *bufferedWriter) SetError(err error) error {
	b.resultInProgress = true
	b.currentResult.Err = err
	return b.EndResult()
}

func (b *bufferedWriter) CanAutomaticallyRetry() bool {
	return true
}

func (b *bufferedWriter) ResetForNewTransaction() error {
	b.pastResults = append(b.pastResults, b.currentGroupResults...)
	b.currentGroupResults = nil
	b.resultInProgress = false
	return nil
}

func (b *bufferedWriter) ResetForRetry() {
	if b.currentGroupResults != nil {
		b.currentGroupResults.Close(b.session.Ctx())
	}
	b.resultInProgress = false
}

func (b *bufferedWriter) GetError() error {
	if len(b.currentGroupResults) > 0 {
		return b.currentGroupResults[len(b.currentGroupResults)-1].Err
	}
	if len(b.pastResults) > 0 {
		return b.pastResults[len(b.pastResults)-1].Err
	}
	return b.currentResult.Err
}

func (b *bufferedWriter) SetEmptyQuery() {
}

func (b *bufferedWriter) results() StatementResults {
	b.pastResults = append(b.pastResults, b.currentGroupResults...)
	return StatementResults{b.pastResults, len(b.pastResults) == 0}
}
