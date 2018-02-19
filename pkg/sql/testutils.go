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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// CreateTestTableDescriptor converts a SQL string to a table for test purposes.
// Will fail on complex tables where that operation requires e.g. looking up
// other tables.
func CreateTestTableDescriptor(
	ctx context.Context,
	parentID, id sqlbase.ID,
	schema string,
	privileges *sqlbase.PrivilegeDescriptor,
) (sqlbase.TableDescriptor, error) {
	st := cluster.MakeTestingClusterSettings()
	stmt, err := parser.ParseOne(schema)
	if err != nil {
		return sqlbase.TableDescriptor{}, err
	}
	semaCtx := tree.MakeSemaContext(false /* privileged */)
	evalCtx := tree.MakeTestingEvalContext(st)
	return MakeTableDesc(
		ctx,
		nil, /* txn */
		nil, /* vt */
		st,
		stmt.(*tree.CreateTable),
		parentID, id,
		hlc.Timestamp{}, /* creationTime */
		privileges,
		nil, /* affected */
		&semaCtx,
		&evalCtx,
	)
}

func makeTestingExtendedEvalContext(st *cluster.Settings) extendedEvalContext {
	return extendedEvalContext{
		EvalContext: tree.MakeTestingEvalContext(st),
	}
}

// StmtBufReader is an exported interface for reading a StmtBuf.
// Normally only the write interface of the buffer is exported, as it is used by
// the pgwire.
type StmtBufReader struct {
	buf *StmtBuf
}

// MakeStmtBufReader creates a StmtBufReader.
func MakeStmtBufReader(buf *StmtBuf) StmtBufReader {
	return StmtBufReader{buf: buf}
}

// CurCmd returns the current command in the buffer.
func (r StmtBufReader) CurCmd() (Command, error) {
	cmd, _ /* pos */, err := r.buf.curCmd()
	return cmd, err
}

// AdvanceOne moves the cursor one position over.
func (r *StmtBufReader) AdvanceOne() {
	r.buf.advanceOne()
}

// SeekToNextBatch skips to the beginning of the next batch of commands.
func (r *StmtBufReader) SeekToNextBatch() error {
	return r.buf.seekToNextBatch()
}
