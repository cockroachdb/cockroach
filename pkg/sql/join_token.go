// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// FeatureTLSAutoJoinEnabled is used to enable and disable the TLS auto-join
// feature.
var FeatureTLSAutoJoinEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"feature.tls_auto_join.enabled",
	"set to true to enable tls auto join through join tokens, false to disable; default is false",
	false,
)

// CreateJoinToken implements the tree.JoinTokenCreator interface.
func (p *planner) CreateJoinToken(ctx context.Context) (string, error) {
	hasAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return "", err
	}
	if !hasAdmin {
		return "", pgerror.New(pgcode.InsufficientPrivilege, "must be admin to create join token")
	}
	if err = featureflag.CheckEnabled(
		ctx, p.ExecCfg(), FeatureTLSAutoJoinEnabled, "create join tokens"); err != nil {
		return "", err
	}

	cm, err := p.ExecCfg().RPCContext.SecurityContext.GetCertificateManager()
	if err != nil {
		return "", errors.Wrap(err, "error when getting certificate manager")
	}

	jt, err := security.GenerateJoinToken(cm)
	if err != nil {
		return "", errors.Wrap(err, "error when generating join token")
	}
	token, err := jt.MarshalText()
	if err != nil {
		return "", errors.Wrap(err, "error when marshaling join token")
	}
	expiration := timeutil.Now().Add(security.JoinTokenExpiration)
	if _, err := p.ExecCfg().InternalDB.Executor().Exec(
		ctx, "insert-join-token", nil, /* txn */
		"insert into system.join_tokens(id, secret, expiration) "+
			"values($1, $2, $3)",
		jt.TokenID.String(), jt.SharedSecret, expiration.Format(time.RFC3339),
	); err != nil {
		return "", errors.Wrap(err, "could not persist join token in system table")
	}
	return string(token), nil
}
