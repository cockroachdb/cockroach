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

// ErrWireFailure is used when sending data over pgwire fails.
type ErrWireFailure struct {
	err error
}

func (e ErrWireFailure) Error() string {
	return fmt.Sprintf("pgwire: unable to write to connection: %s", e.err.Error())
}

// NewWireFailureError wraps err in an ErrWriteFailure.
func NewWireFailureError(err error) ErrWireFailure {
	return ErrWireFailure{err}
}

// ResultWriter is an interface which is used to store results from query
// execution. Implementations could be buffered or immediately stream to
// the client.
type ResultWriter interface {
	// BeginResult and SetColumns should be called any time we wish to start
	// a result -- regardless of what the parser.StatementType is.
	BeginResult(stmt parser.Statement)
	PGTag() string
	StatementType() parser.StatementType
	// SetColumns should be called prior to AddRow.
	SetColumns(columns sqlbase.ResultColumns)
	// IncrementRowsAffected increments a counter. This is used for
	// parser.RowsAffected and parser.DDL since we care only about the number of
	// rows affected. Should be called after BeginResult.
	IncrementRowsAffected(n int)
	// AddRow takes the passed in row and adds it to the current result. Should
	// be called after BeginResult and SetColumns.
	AddRow(ctx context.Context, row parser.Datums) error
	// RowsAffected returns either the number of times AddRow was called, or the
	// sum of all n passed into IncrementRowsAffected.
	RowsAffected() int
	// EndResult ends the current result. Cannot be called unless there's a
	// corresponding BeginResult prior.
	EndResult() error
	// Error sets an error. Depending on the implementation all other functions
	// become no-ops.
	Error(err error) error
	// LastError returns either nil, or the error set in Error.
	LastError() error
	// EmptyQuery is used to indicate that there are no statements in the current
	// sql string. Empty queries are different than queries with no results.
	EmptyQuery()
	// HasSentResults returns true if we've sent any results to the client. If we
	// have then we cannot automatically retry.
	HasSentResults() bool
}

type bufferedWriter struct {
	session          *Session
	results          ResultList
	currentResult    Result
	resultInProgress bool
}

func (b *bufferedWriter) BeginResult(stmt parser.Statement) {
	if b.resultInProgress {
		panic("can't start new result before ending the previous")
	}
	b.resultInProgress = true
	b.currentResult = Result{PGTag: stmt.StatementTag(), Type: stmt.StatementType()}
}

func (b *bufferedWriter) PGTag() string {
	return b.currentResult.PGTag
}

func (b *bufferedWriter) StatementType() parser.StatementType {
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

func (b *bufferedWriter) RowsAffected() int {
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
	b.results = append(b.results, b.currentResult)
	return nil
}

func (b *bufferedWriter) EmptyQuery() {
}

func (b *bufferedWriter) Error(err error) error {
	b.resultInProgress = true
	b.currentResult.Err = err
	return b.EndResult()
}

func (b *bufferedWriter) LastError() error {
	return b.results[len(b.results)-1].Err
}

func (b *bufferedWriter) HasSentResults() bool {
	return false
}
