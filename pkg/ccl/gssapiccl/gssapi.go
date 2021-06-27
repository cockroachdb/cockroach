// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package gssapiccl

import (
	"context"
	"crypto/tls"
	"os"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/errors"
	"github.com/jcmturner/gofork/encoding/asn1"
	"gopkg.in/jcmturner/goidentity.v3"
	"gopkg.in/jcmturner/gokrb5.v7/keytab"
	"gopkg.in/jcmturner/gokrb5.v8/gssapi"
	"gopkg.in/jcmturner/gokrb5.v8/spnego"
)

const (
	authTypeGSS int32 = 7
	// authTypeGSSContinue int32 = 8
)

// authGSS performs GSS authentication. See:
// https://github.com/postgres/postgres/blob/0f9cdd7dca694d487ab663d463b308919f591c02/src/backend/libpq/auth.c#L1090
func authGSS(
	ctx context.Context,
	c pgwire.AuthConn,
	tlsState tls.ConnectionState,
	_ pgwire.PasswordRetrievalFn,
	_ pgwire.PasswordValidUntilFn,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
) (security.UserAuthHook, error) {
	return func(ctx context.Context, requestedUser security.SQLUsername, clientConnection bool) (func(), error) {
		ktPath := os.Getenv("KRB5_KTNAME")
		if ktPath == "" {
			return nil, errors.New("KRB5_KTNAME is not set")
		}
		kt, err := keytab.Load(ktPath)
		if err != nil {
			return nil, err
		}

		if err := c.SendAuthRequest(authTypeGSS, nil); err != nil {
			return nil, err
		}

		tokenBytes, err := c.GetPwdData()
		if err != nil {
			return nil, err
		}
		var token spnego.SPNEGOToken
		err = token.Unmarshal(tokenBytes)
		if err != nil {
			// If this is a raw KRB5 token, wrap it (see jcmturner/gokrb5#347).
			var rawk5Token spnego.KRB5Token
			if rawk5Token.Unmarshal(tokenBytes) != nil {
				return nil, err
			}
			token.Init = true
			token.NegTokenInit = spnego.NegTokenInit{
				MechTypes:      []asn1.ObjectIdentifier{rawk5Token.OID},
				MechTokenBytes: tokenBytes,
			}
		}

		sp := spnego.SPNEGOService(kt)
		authed, reqCtx, status := sp.AcceptSecContext(&token)

		if status.Code == gssapi.StatusContinueNeeded {
			// TODO: in krb5 the gss_accept_sec_context function provides an output
			// token here, to reply with to the initiator, to continue negotiation. We
			// could potentially do something similar if we constructed a NegTokenResp
			// and marshaled it to send as c.SendAuthRequest(authTypeGSSContinue, t).
			return nil, errors.Errorf("accepting GSS security context requires unsupported continue: %s", status.Message)
		}

		if status.Code != gssapi.StatusComplete || !authed {
			return nil, errors.Errorf("accepting GSS security context failed: %s", status.Message)
		}

		id := reqCtx.Value(spnego.CTXKeyCredentials).(goidentity.Identity)
		if id == nil {
			return nil, errors.Errorf("accepting GSS security context failed: credentials missing")
		}

		gssUser := id.DisplayName()

		realms := entry.GetOptions("krb_realm")

		if realm := id.Domain(); realm != "" {
			if len(realms) > 0 {
				matched := false
				for _, krbRealm := range realms {
					if realm == krbRealm {
						matched = true
						break
					}
				}
				if !matched {
					return nil, errors.Errorf("GSSAPI realm (%s) didn't match any configured realm", realm)
				}
			}
			if entry.GetOption("include_realm") == "1" {
				gssUser = gssUser + "@" + realm
			}
		} else if len(realms) > 0 {
			return nil, errors.New("GSSAPI did not return realm but realm matching was requested")
		}

		gssUsername, _ := security.MakeSQLUsernameFromUserInput(gssUser, security.UsernameValidation)
		if gssUsername != requestedUser {
			return nil, errors.Errorf("requested user is %s, but GSSAPI auth is for %s", requestedUser, gssUser)
		}

		// Do the license check last so that administrators are able to test whether
		// their GSS configuration is correct. That is, the presence of this error
		// message means they have a correctly functioning GSS/Kerberos setup,
		// but now need to enable enterprise features.
		return nil, utilccl.CheckEnterpriseEnabled(execCfg.Settings, execCfg.ClusterID(), execCfg.Organization(), "GSS authentication")
	}, nil
}

func checkEntry(entry hba.Entry) error {
	hasInclude0 := false
	for _, op := range entry.Options {
		switch op[0] {
		case "include_realm":
			if op[1] == "0" {
				hasInclude0 = true
			} else {
				return errors.Errorf("include_realm must be set to 0: %s", op[1])
			}
		case "krb_realm":
		default:
			return errors.Errorf("unsupported option %s", op[0])
		}
	}
	if !hasInclude0 {
		return errors.New(`missing "include_realm=0" option in GSS entry`)
	}
	return nil
}

func init() {
	pgwire.RegisterAuthMethod("gss", authGSS, hba.ConnHostSSL, checkEntry)
}
