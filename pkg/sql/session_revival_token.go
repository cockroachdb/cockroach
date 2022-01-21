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
	"github.com/cockroachdb/cockroach/pkg/security/sessionrevival"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (p *planner) CreateSessionRevivalToken() (*tree.DBytes, error) {
	if !p.ExecCfg().AllowSessionRevival {
		return nil, pgerror.New(pgcode.FeatureNotSupported, "session revival tokens are not supported on this cluster")
	}
	user := p.SessionData().User()
	if user.IsRootUser() {
		return nil, pgerror.New(pgcode.InsufficientPrivilege, "cannot create token for root user")
	}
	cm, err := p.ExecCfg().RPCContext.SecurityContext.GetCertificateManager()
	if err != nil {
		return nil, err
	}
	tokenBytes, err := sessionrevival.CreateSessionRevivalToken(cm, user)
	if err != nil {
		return nil, err
	}
	return tree.NewDBytes(tree.DBytes(tokenBytes)), nil
}

func (p *planner) ValidateSessionRevivalToken(token *tree.DBytes) (*tree.DBool, error) {
	if !p.ExecCfg().AllowSessionRevival {
		return nil, pgerror.New(pgcode.FeatureNotSupported, "session revival tokens are not supported on this cluster")
	}
	cm, err := p.ExecCfg().RPCContext.SecurityContext.GetCertificateManager()
	if err != nil {
		return nil, err
	}
	valid, err := sessionrevival.ValidateSessionRevivalToken(cm, p.User(), []byte(*token))
	if err != nil {
		return nil, err
	}
	return tree.MakeDBool(tree.DBool(valid)), nil
}
