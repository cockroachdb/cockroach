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
// Author: Nikhil Benesch (nikhil.benesch@gmail.com)

package sql

import (
	"fmt"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
)

func (p *planner) PauseJob(ctx context.Context, n *parser.PauseJob) (planNode, error) {
	return nil, pgerror.Unimplemented("pause-job", "unimplemented")
}

func (p *planner) ResumeJob(ctx context.Context, n *parser.ResumeJob) (planNode, error) {
	return nil, pgerror.Unimplemented("resume-job", "unimplemented")
}

func (p *planner) CancelJob(ctx context.Context, n *parser.CancelJob) (planNode, error) {
	return nil, pgerror.Unimplemented("cancel-job", "unimplemented")
}

type cancelQueryNode struct {
	p       *planner
	queryID parser.TypedExpr
}

func (*cancelQueryNode) Values() parser.Datums { return nil }

func (n *cancelQueryNode) Start(params runParams) error {
	statusServer := n.p.session.execCfg.StatusServer

	queryIDDatum, err := n.queryID.Eval(&n.p.evalCtx)
	if err != nil {
		return err
	}

	queryIDString := parser.AsStringWithFlags(queryIDDatum, parser.FmtBareStrings)
	queryID, err := uint128.FromString(queryIDString)
	if err != nil {
		return errors.Wrapf(err, "Invalid query ID '%s'", queryIDString)
	}

	// Get the lowest 32 bits of the query ID.
	nodeID := 0xFFFFFFFF & queryID.Lo

	request := &serverpb.CancelQueryRequest{
		NodeId:   fmt.Sprintf("%d", nodeID),
		QueryID:  queryIDString,
		Username: n.p.session.User,
	}

	response, err := statusServer.CancelQuery(params.ctx, request)
	if err != nil {
		return err
	}

	if !response.Cancelled {
		return fmt.Errorf("Could not cancel query %s: %s", queryID, response.Error)
	}

	return nil
}

func (*cancelQueryNode) Close(context.Context) {}

func (n *cancelQueryNode) Next(runParams) (bool, error) {
	return false, nil
}

func (p *planner) CancelQuery(ctx context.Context, n *parser.CancelQuery) (planNode, error) {

	typedQueryID, err := p.analyzeExpr(
		ctx,
		n.ID,
		nil,
		parser.IndexedVarHelper{},
		parser.TypeString,
		true, /* requireType */
		"CANCEL QUERY",
	)
	if err != nil {
		return nil, err
	}

	return &cancelQueryNode{
		p:       p,
		queryID: typedQueryID,
	}, nil
}

type cancelTransactionNode struct {
	p     *planner
	txnID parser.TypedExpr
}

func (*cancelTransactionNode) Values() parser.Datums { return nil }

func (n *cancelTransactionNode) Start(params runParams) error {
	statusServer := n.p.session.execCfg.StatusServer

	txnIDDatum, err := n.txnID.Eval(&n.p.evalCtx)
	if err != nil {
		return err
	}
	txnID := parser.AsStringWithFlags(txnIDDatum, parser.FmtBareStrings)
	// The check is not required, but if the len is invalid better abort now.
	idLen := len(txnID)
	if idLen != 8 {
		return pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError,
			"invalid txn id length, expected 8 but got %d", idLen)
	}

	request := &serverpb.CancelTransactionRequest{
		TransactionId: txnID,
		Username:      n.p.session.User,
	}

	response, err := statusServer.CancelTransaction(params.ctx, request)
	if err != nil {
		return err
	}

	if !response.Cancelled {
		return fmt.Errorf("could not cancel transaction %s: %s", txnID, response.Error)
	}

	return nil
}

func (*cancelTransactionNode) Close(context.Context) {}

func (n *cancelTransactionNode) Next(runParams) (bool, error) {
	return false, nil
}

func (p *planner) CancelTransaction(ctx context.Context, n *parser.CancelTransaction) (planNode, error) {

	typedTxnID, err := p.analyzeExpr(
		ctx,
		n.ID,
		nil,
		parser.IndexedVarHelper{},
		parser.TypeString,
		true, /* requireType */
		"CANCEL TRANSACTION",
	)
	if err != nil {
		return nil, err
	}

	return &cancelTransactionNode{
		p:     p,
		txnID: typedTxnID,
	}, nil
}
