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
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// CreateTestTableDescriptor converts a SQL string to a table for test purposes.
// Will fail on complex tables where that operation requires e.g. looking up
// other tables or otherwise utilizing a planner, since the planner used here is
// just a zero value placeholder.
func CreateTestTableDescriptor(
	ctx context.Context,
	parentID, id sqlbase.ID,
	schema string,
	privileges *sqlbase.PrivilegeDescriptor,
) (sqlbase.TableDescriptor, error) {
	stmt, err := parser.ParseOne(schema)
	if err != nil {
		return sqlbase.TableDescriptor{}, err
	}
	p := planner{session: new(Session)}
	p.evalCtx = tree.MakeTestingEvalContext()
	return p.makeTableDesc(ctx, stmt.(*tree.CreateTable), parentID, id, hlc.Timestamp{}, privileges, nil)
}
