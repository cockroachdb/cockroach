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
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// PrepareStmt implements the PREPARE statement.
// See https://www.postgresql.org/docs/current/static/sql-prepare.html for details.
func (e *Executor) PrepareStmt(session *Session, s *tree.Prepare) error {
	name := s.Name.String()
	if session.PreparedStatements.Exists(name) {
		return pgerror.NewErrorf(pgerror.CodeDuplicatePreparedStatementError,
			"prepared statement %q already exists", name)
	}
	typeHints := make(tree.PlaceholderTypes, len(s.Types))
	for i, t := range s.Types {
		typeHints[strconv.Itoa(i+1)] = coltypes.CastTargetToDatumType(t)
	}
	_, err := session.PreparedStatements.New(
		e, name, Statement{AST: s.Statement}, s.Statement.String(), typeHints,
	)
	return err
}
