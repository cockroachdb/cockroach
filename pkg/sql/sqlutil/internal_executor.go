// Copyright 2016 The Cockroach Authors.
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
// Author: Andrei Matei (andreimatei1@gmail.com)

package sqlutil

import "github.com/cockroachdb/cockroach/internal/client"

// InternalExecutor is meant to be used by layers below SQL in the system that
// nevertheless want to execute SQL queries (presumably against system tables).
// It is extracted in this "sql/util" package to avoid circular references and
// is implemented by sql.InternalExecutor.
type InternalExecutor interface {
	// ExecuteStatementInTransaction executes the supplied SQL statement as part of
	// the supplied transaction. Statements are currently executed as the root user.
	ExecuteStatementInTransaction(
		txn *client.Txn, statement string, params ...interface{}) (int, error)
}
