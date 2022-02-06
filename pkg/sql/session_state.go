// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// SerializeSessionState is a wrapper for serializeSessionState, and uses the
// planner.
func (p *planner) SerializeSessionState() (*tree.DBytes, error) {
	evalCtx := p.EvalContext()
	return serializeSessionState(
		!evalCtx.TxnImplicit,
		evalCtx.PreparedStatementState,
		p.SessionData(),
	)
}

// serializeSessionState serializes the current session's state into bytes.
//
// NOTE: This is used within an observer statement directly, and should not rely
// on the planner because those statements do not get planned.
func serializeSessionState(
	inTxn bool, prepStmtsState tree.PreparedStatementState, sd *sessiondata.SessionData,
) (*tree.DBytes, error) {
	if inTxn {
		return nil, pgerror.Newf(
			pgcode.InvalidTransactionState,
			"cannot serialize a session which is inside a transaction",
		)
	}

	if prepStmtsState.HasPrepared() {
		return nil, pgerror.Newf(
			pgcode.InvalidTransactionState,
			"cannot serialize a session which has portals or prepared statements",
		)
	}

	if sd == nil {
		return nil, pgerror.Newf(
			pgcode.InvalidTransactionState,
			"no session is active",
		)
	}

	if len(sd.DatabaseIDToTempSchemaID) > 0 {
		return nil, pgerror.Newf(
			pgcode.InvalidTransactionState,
			"cannot serialize session with temporary schemas",
		)
	}

	var m sessiondatapb.MigratableSession
	m.SessionData = sd.SessionData
	sessiondata.MarshalNonLocal(sd, &m.SessionData)
	m.LocalOnlySessionData = sd.LocalOnlySessionData

	b, err := protoutil.Marshal(&m)
	if err != nil {
		return nil, err
	}

	return tree.NewDBytes(tree.DBytes(b)), nil
}

// DeserializeSessionState deserializes the given state into the current session.
func (p *planner) DeserializeSessionState(state *tree.DBytes) (*tree.DBool, error) {
	evalCtx := p.EvalContext()

	if !evalCtx.TxnImplicit {
		return nil, pgerror.Newf(
			pgcode.InvalidTransactionState,
			"cannot deserialize a session whilst inside a transaction",
		)
	}

	var m sessiondatapb.MigratableSession
	if err := protoutil.Unmarshal([]byte(*state), &m); err != nil {
		return nil, pgerror.WithCandidateCode(
			errors.Wrapf(err, "error deserializing session"),
			pgcode.InvalidParameterValue,
		)
	}
	sd, err := sessiondata.UnmarshalNonLocal(m.SessionData)
	if err != nil {
		return nil, err
	}
	sd.SessionData = m.SessionData
	sd.LocalUnmigratableSessionData = evalCtx.SessionData().LocalUnmigratableSessionData
	sd.LocalOnlySessionData = m.LocalOnlySessionData
	if sd.SessionUser().Normalized() != evalCtx.SessionData().SessionUser().Normalized() {
		return nil, pgerror.Newf(
			pgcode.InsufficientPrivilege,
			"can only deserialize matching session users",
		)
	}
	if err := p.CheckCanBecomeUser(evalCtx.Context, sd.User()); err != nil {
		return nil, err
	}
	*p.SessionData() = *sd

	return tree.MakeDBool(true), nil
}
