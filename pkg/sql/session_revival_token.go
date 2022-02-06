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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/sessionrevival"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// AllowSessionRevival is true if the cluster is allowed to create session
// revival tokens and use them to authenticate a session. It is a non-public
// setting since this is only intended to be used by CockroachDB-serverless
// at the time of this writing.
var AllowSessionRevival = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"server.user_login.session_revival_token.enabled",
	"if set, the cluster is able to create session revival tokens and use them "+
		"to authenticate a new session",
	false,
)

// CreateSessionRevivalToken is a wrapper for createSessionRevivalToken, and
// uses the planner.
func (p *planner) CreateSessionRevivalToken() (*tree.DBytes, error) {
	cm, err := p.ExecCfg().RPCContext.SecurityContext.GetCertificateManager()
	if err != nil {
		return nil, err
	}
	return createSessionRevivalToken(
		AllowSessionRevival.Get(&p.ExecCfg().Settings.SV) && !p.ExecCfg().Codec.ForSystemTenant(),
		p.SessionData(),
		cm,
	)
}

// createSessionRevivalToken creates a session revival token for the current
// user.
//
// NOTE: This is used within an observer statement directly, and should not rely
// on the planner because those statements do not get planned.
func createSessionRevivalToken(
	allowSessionRevival bool, sd *sessiondata.SessionData, cm *security.CertificateManager,
) (*tree.DBytes, error) {
	if !allowSessionRevival {
		return nil, pgerror.New(pgcode.FeatureNotSupported, "session revival tokens are not supported on this cluster")
	}

	// Note that we use SessionUser here and not CurrentUser, since when the
	// token is used to create a new session, it should be for the user who was
	// originally authenticated. (Whereas CurrentUser could be a different user
	// if SET ROLE had been used.)
	user := sd.SessionUser()
	if user.IsRootUser() {
		return nil, pgerror.New(pgcode.InsufficientPrivilege, "cannot create token for root user")
	}

	tokenBytes, err := sessionrevival.CreateSessionRevivalToken(cm, user)
	if err != nil {
		return nil, err
	}
	return tree.NewDBytes(tree.DBytes(tokenBytes)), nil
}

// ValidateSessionRevivalToken validates a session revival token.
func (p *planner) ValidateSessionRevivalToken(token *tree.DBytes) (*tree.DBool, error) {
	if !AllowSessionRevival.Get(&p.ExecCfg().Settings.SV) || p.ExecCfg().Codec.ForSystemTenant() {
		return nil, pgerror.New(pgcode.FeatureNotSupported, "session revival tokens are not supported on this cluster")
	}
	cm, err := p.ExecCfg().RPCContext.SecurityContext.GetCertificateManager()
	if err != nil {
		return nil, err
	}
	if err := sessionrevival.ValidateSessionRevivalToken(cm, p.SessionData().SessionUser(), []byte(*token)); err != nil {
		return nil, err
	}
	return tree.DBoolTrue, nil
}
