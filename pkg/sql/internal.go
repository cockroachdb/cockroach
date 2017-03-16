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
// Author: Matt Tracy (matt@cockroachlabs.com)

package sql

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// InternalExecutor can be used internally by cockroach to execute SQL
// statements without needing to open a SQL connection. InternalExecutor assumes
// that the caller has access to a cockroach KV client to handle connection and
// transaction management.
type InternalExecutor struct {
	LeaseManager *LeaseManager
}

var _ sqlutil.InternalExecutor = InternalExecutor{}

// ExecuteStatementInTransaction executes the supplied SQL statement as part of
// the supplied transaction. Statements are currently executed as the root user.
func (ie InternalExecutor) ExecuteStatementInTransaction(
	ctx context.Context, opName string, txn *client.Txn, statement string, qargs ...interface{},
) (int, error) {
	p := makeInternalPlanner(opName, txn, security.RootUser, ie.LeaseManager.memMetrics)
	defer finishInternalPlanner(p)
	p.leaseMgr = ie.LeaseManager
	return p.exec(ctx, statement, qargs...)
}
