// Copyright 2018 The Cockroach Authors.
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

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

var preparedXactsTableName = tree.NewTableName("system", "prepared_xacts")

type commitPreparedRun struct {
	rowsAffected int
}

type commitPreparedNode struct {
	txnName string
	txn     *roachpb.Transaction
	commit  bool
	run     commitPreparedRun
}

// CommitPrepared commits a previously prepared transaction and
// deletes its associated entry from the system.prepared_xacts table.
// This is called from COMMIT PREPARED.
func (p *planner) CommitPrepared(ctx context.Context, n *tree.CommitPrepared) (planNode, error) {
	return p.commitPreparedNode(ctx, n.Transaction, true /* commit */)
}

// commitPreparedNode creates a plan node for either commit or rollback
// of a prepared transaction according to the commit parameter.
// Privileges: DELETE on system.prepared_xacts or matching owner.
func (p *planner) commitPreparedNode(
	ctx context.Context, txnName string, commit bool,
) (*commitPreparedNode, error) {
	txn, owner, err := selectPreparedXact(ctx, p, txnName)
	if err != nil {
		return nil, err
	}
	// If the requesting user isn't the owner of the prepared transaction,
	// See if they do have DELETE privileges on the prepared_xacts table.
	if user := p.SessionData().User; user != owner {
		tDesc, err := getTableDesc(ctx, p.Txn(), p.getVirtualTabler(), preparedXactsTableName)
		if err != nil {
			return nil, err
		}
		if err := p.CheckPrivilege(ctx, tDesc, privilege.DELETE); err != nil {
			return nil, errors.Errorf("user %s is not the owner of prepared transaction %s (%s)",
				user, txnName, owner)
		}
	}
	return &commitPreparedNode{
		txnName: txnName,
		txn:     txn,
		commit:  commit,
	}, nil
}

// selectPreparedXact queries the prepared transaction from the
// prepared_xacts table and if found returns the JSON transaction
// object and the owner.
func selectPreparedXact(
	ctx context.Context, p *planner, txnName string,
) (*roachpb.Transaction, string, error) {
	internalExecutor := InternalExecutor{ExecCfg: p.ExecCfg()}
	rows, _ /* cols */, err := internalExecutor.QueryRowsInTransaction(
		ctx,
		"select-prepared-xact",
		p.Txn(),
		`SELECT transaction, owner, status FROM system.prepared_xacts WHERE gid = $1`,
		txnName,
	)
	if err != nil {
		return nil, "", err
	}
	if len(rows) == 0 {
		return nil, "", errors.Errorf("prepared transaction %s does not exist", txnName)
	}
	var txn roachpb.Transaction
	if err := protoutil.Unmarshal(([]byte)(tree.MustBeDBytes(rows[0][0])), &txn); err != nil {
		return nil, "", err
	}
	owner := tree.MustBeDString(rows[0][1])
	status := tree.MustBeDString(rows[0][2])
	if len(status) > 0 {
		return nil, "", errors.Errorf("prepared transaction %s in state %q", txn, status)
	}
	return &txn, string(owner), nil
}

func deletePreparedXact(params runParams, txnName string) (int, error) {
	internalExecutor := InternalExecutor{ExecCfg: params.extendedEvalCtx.ExecCfg}
	return internalExecutor.ExecuteStatementInTransaction(
		params.ctx,
		"delete-prepared-xact",
		params.p.txn,
		`DELETE FROM system.prepared_xacts WHERE gid = $1`,
		txnName,
	)
}

func (cp *commitPreparedNode) startExec(params runParams) error {
	// Cleanup the prepared xact record regardless.
	defer func() {
		if _, err := deletePreparedXact(params, cp.txnName); err != nil {
			log.Warningf(params.ctx, "unable to cleanup prepared transaction record: %s", err)
		}
	}()
	// If the original transaction was read-only, this is a noop.
	if !cp.txn.Writing {
		return nil
	}
	clientTxn := client.NewTxnWithProto(
		params.extendedEvalCtx.ExecCfg.DB,
		0, /* gatewayNodeID */
		client.RootTxn,
		*cp.txn,
	)
	if cp.commit {
		return clientTxn.Commit(params.ctx)
	}
	return clientTxn.Rollback(params.ctx)
}

// Next implements the planNode interface.
func (*commitPreparedNode) Next(runParams) (bool, error) { return false, nil }

// Values implements the planNode interface.
func (*commitPreparedNode) Values() tree.Datums { return tree.Datums{} }

// Close implements the planNode interface.
func (*commitPreparedNode) Close(context.Context) {}

// FastPathResults implements the planNodeFastPath interface.
func (cp *commitPreparedNode) FastPathResults() (int, bool) { return cp.run.rowsAffected, true }
