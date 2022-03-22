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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/lib/pq/oid"
)

var maxSerializedSessionSize = settings.RegisterByteSizeSetting(
	settings.TenantReadOnly,
	"sql.session_transfer.max_session_size",
	"if set to non-zero, then serializing a session will fail if it requires more"+
		"than the specified size",
	0,
	settings.NonNegativeInt,
)

// SerializeSessionState is a wrapper for serializeSessionState, and uses the
// planner.
func (p *planner) SerializeSessionState() (*tree.DBytes, error) {
	evalCtx := p.EvalContext()
	return serializeSessionState(
		!evalCtx.TxnImplicit,
		evalCtx.PreparedStatementState,
		p.SessionData(),
		p.ExecCfg(),
	)
}

// serializeSessionState serializes the current session's state into bytes.
//
// NOTE: This is used within an observer statement directly, and should not rely
// on the planner because those statements do not get planned.
func serializeSessionState(
	inExplicitTxn bool,
	prepStmtsState tree.PreparedStatementState,
	sd *sessiondata.SessionData,
	execCfg *ExecutorConfig,
) (*tree.DBytes, error) {
	if inExplicitTxn {
		return nil, pgerror.Newf(
			pgcode.InvalidTransactionState,
			"cannot serialize a session which is inside a transaction",
		)
	}

	if prepStmtsState.HasActivePortals() {
		return nil, pgerror.Newf(
			pgcode.InvalidTransactionState,
			"cannot serialize a session which has active portals",
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
	m.PreparedStatements = prepStmtsState.MigratablePreparedStatements()

	b, err := protoutil.Marshal(&m)
	if err != nil {
		return nil, err
	}

	maxSize := maxSerializedSessionSize.Get(&execCfg.Settings.SV)
	if maxSize > 0 && int64(len(b)) > maxSize {
		return nil, pgerror.Newf(
			pgcode.ProgramLimitExceeded,
			"serialized session size %s exceeds max allowed size %s",
			humanizeutil.IBytes(int64(len(b))),
			humanizeutil.IBytes(maxSize),
		)
	}

	return tree.NewDBytes(tree.DBytes(b)), nil
}

// DeserializeSessionState deserializes the given state into the current session.
func (p *planner) DeserializeSessionState(state *tree.DBytes) (*tree.DBool, error) {
	evalCtx := p.ExtendedEvalContext()
	if !evalCtx.TxnImplicit {
		return nil, pgerror.Newf(
			pgcode.InvalidTransactionState,
			"cannot deserialize a session whilst inside a transaction",
		)
	}

	var m sessiondatapb.MigratableSession
	if err := protoutil.Unmarshal([]byte(*state), &m); err != nil {
		return nil, pgerror.Wrapf(err, pgcode.InvalidParameterValue, "error deserializing session")
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
	if err := p.checkCanBecomeUser(evalCtx.Context, sd.User()); err != nil {
		return nil, err
	}

	for _, prepStmt := range m.PreparedStatements {
		parserStmt, err := parser.ParseOneWithInt(
			prepStmt.SQL,
			parser.NakedIntTypeFromDefaultIntSize(sd.DefaultIntSize),
		)
		if err != nil {
			return nil, err
		}
		id := GenerateClusterWideID(evalCtx.ExecCfg.Clock.Now(), evalCtx.ExecCfg.NodeID.SQLInstanceID())
		stmt := makeStatement(parserStmt, id)

		var placeholderTypes tree.PlaceholderTypes
		if len(prepStmt.PlaceholderTypeHints) > 0 {
			// Prepare the mapping of SQL placeholder names to types. Pre-populate it
			// with the type hints that were serialized.
			placeholderTypes = make(tree.PlaceholderTypes, stmt.NumPlaceholders)
			for i, t := range prepStmt.PlaceholderTypeHints {
				// If the OID is user defined or unknown, then skip it and let the
				// statementPreparer resolve the type.
				if t == 0 || t == oid.T_unknown || types.IsOIDUserDefinedType(t) {
					placeholderTypes[i] = nil
					continue
				}
				v, ok := types.OidToType[t]
				if !ok {
					err := pgwirebase.NewProtocolViolationErrorf("unknown oid type: %v", t)
					return nil, err
				}
				placeholderTypes[i] = v
			}
		}

		_, err = evalCtx.statementPreparer.addPreparedStmt(
			evalCtx.Context,
			prepStmt.Name, stmt, placeholderTypes, prepStmt.PlaceholderTypeHints,
			PreparedStatementOriginSessionMigration,
		)
		if err != nil {
			return nil, err
		}
	}

	*p.SessionData() = *sd

	return tree.MakeDBool(true), nil
}
