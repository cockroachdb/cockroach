// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package jwtauthccl

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/lestrrat-go/jwx/jwt"
)

const (
	counterPrefix           = "auth.jwt."
	beginAuthCounterName    = counterPrefix + "begin_auth"
	loginSuccessCounterName = counterPrefix + "login_success"
	enableCounterName       = counterPrefix + "enable"
)

var (
	beginAuthUseCounter    = telemetry.GetCounterOnce(beginAuthCounterName)
	loginSuccessUseCounter = telemetry.GetCounterOnce(loginSuccessCounterName)
	enableUseCounter       = telemetry.GetCounterOnce(enableCounterName)
)

type jwtAuthenticator struct {
	mutex       syncutil.RWMutex
	conf        jwtAuthenticatorConf
	enabled     bool
	clusterUUID uuid.UUID
}

type jwtAuthenticatorConf struct {
	enabled bool
	issuers []string
	jwks    jwk.Set
}

func (authenticator *jwtAuthenticator) reloadConfig(
	ctx context.Context,
	st *cluster.Settings,
) {
	authenticator.mutex.Lock()
	defer authenticator.mutex.Unlock()
	authenticator.reloadConfigLocked(ctx, st)
}

func (authenticator *jwtAuthenticator) reloadConfigLocked(
	ctx context.Context,
	st *cluster.Settings,
) {
	conf := jwtAuthenticatorConf{
		enabled: JWTAuthEnabled.Get(&st.SV),
		issuers: mustParseIssuers(JWTAuthIssuers.Get(&st.SV)),
		jwks:    mustParseJWKS(JWTAuthJWKS.Get(&st.SV)),
	}

	if !authenticator.conf.enabled && conf.enabled {
		telemetry.Inc(enableUseCounter)
	}

	authenticator.conf = conf
	authenticator.enabled = authenticator.conf.enabled

	log.Infof(ctx, "initialized JWT authenticator")
}

func (authenticator *jwtAuthenticator) ValidateJWTLogin(
	st *cluster.Settings,
	user username.SQLUsername,
	tokenBytes []byte,
) error {
	if !authenticator.enabled {
		return errors.Newf("JWT Auth: not enabled")
	}

	telemetry.Inc(beginAuthUseCounter)

	parsedToken, err := jwt.Parse(tokenBytes, jwt.WithKeySet(authenticator.conf.jwks), jwt.WithValidate(true))
	if err != nil {
		return errors.Newf("JWT Auth: invalid token")
	}
	if parsedToken.Subject() != user.Normalized() {
		return errors.Newf("JWT Auth: invalid subject. Token issued for %s but login was for %s", parsedToken.Subject(), user.Normalized())
	}
	audienceMatch := false
	for _, audience := range parsedToken.Audience() {
		if audience == authenticator.clusterUUID.String() {
			audienceMatch = true
			break
		}
	}
	if !audienceMatch {
		return errors.Newf("JWT Auth: invalid audience. Token issued for %s but cluster UUID is %s", parsedToken.Audience(), authenticator.clusterUUID.String())
	}

	issuerMatch := false
	for _, issuer := range authenticator.conf.issuers {
		if issuer == parsedToken.Issuer() {
			issuerMatch = true
			break
		}
	}
	if !issuerMatch {
		return errors.Newf("JWT Auth: invalid issuer. Token issued by %s but configured issuers are %s", parsedToken.Issuer(), authenticator.conf.issuers)
	}

	org := sql.ClusterOrganization.Get(&st.SV)
	if err = utilccl.CheckEnterpriseEnabled(st, authenticator.clusterUUID, org, "JWT Auth"); err != nil {
		return err
	}

	telemetry.Inc(loginSuccessUseCounter)
	return nil
}

var ConfigureJWTAuth = func(
	serverCtx context.Context,
	ambientCtx log.AmbientContext,
	st *cluster.Settings,
	clusterUUID uuid.UUID,
) pgwire.JWTVerifier {
	jwtAuthentication := jwtAuthenticator{}
	jwtAuthentication.clusterUUID = clusterUUID
	jwtAuthentication.reloadConfig(serverCtx, st)
	JWTAuthEnabled.SetOnChange(&st.SV, func(ctx context.Context) {
		jwtAuthentication.reloadConfig(ambientCtx.AnnotateCtx(ctx), st)
	})
	JWTAuthIssuers.SetOnChange(&st.SV, func(ctx context.Context) {
		jwtAuthentication.reloadConfig(ambientCtx.AnnotateCtx(ctx), st)
	})
	JWTAuthJWKS.SetOnChange(&st.SV, func(ctx context.Context) {
		jwtAuthentication.reloadConfig(ambientCtx.AnnotateCtx(ctx), st)
	})
	return &jwtAuthentication
}

func init() {
	pgwire.ConfigureJWTAuth = ConfigureJWTAuth
}
