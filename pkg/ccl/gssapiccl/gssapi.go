// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// We use a non-standard build tag here because we want to only build on
// linux-gnu targets (i.e., not musl). Since go doesn't have a builtin way
// to do that, we have to set this in the top-level Makefile.

// +build gss

package gssapiccl

import (
	"context"
	"crypto/tls"
	"strings"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/errors"
)

// #cgo LDFLAGS: -lgssapi_krb5 -lcom_err -lkrb5 -lkrb5support -ldl -lk5crypto -lresolv
//
// #include <gssapi/gssapi.h>
// #include <stdlib.h>
import "C"

const (
	authTypeGSS         int32 = 7
	authTypeGSSContinue int32 = 8
)

// authGSS performs GSS authentication. See:
// https://github.com/postgres/postgres/blob/0f9cdd7dca694d487ab663d463b308919f591c02/src/backend/libpq/auth.c#L1090
func authGSS(
	_ context.Context,
	c pgwire.AuthConn,
	_ tls.ConnectionState,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
	identMap *identmap.Conf,
) (pgwire.RoleMapper, pgwire.Authorizer) {
	// We enforce that the "map" xor "include_realm=0" options are set
	// in the HBA validation function below.
	var mapper pgwire.RoleMapper
	if entry.GetOption("map") != "" {
		mapper = pgwire.HbaMapper(entry, identMap)
	} else /* include_realm = 0 */ {
		// Strip the trailing realm information from the gssapi username.
		mapper = func(_ context.Context, systemIdentity security.SQLUsername) ([]security.SQLUsername, error) {
			norm := systemIdentity.Normalized()
			idx := strings.Index(norm, "@")
			if idx == -1 {
				return nil, errors.Newf("HBA entry set include_realm=0, but no realm could be determined from %q", norm)
			}
			ret, err := security.MakeSQLUsernameFromUserInput(norm[:idx], security.UsernameValidation)
			return []security.SQLUsername{ret}, err
		}
	}

	return mapper,
		func(_ context.Context, _ security.SQLUsername, _ bool, _ pgwire.PasswordRetrievalFn) (func(), error) {
			var (
				majStat, minStat, lminS, gflags C.OM_uint32
				gbuf                            C.gss_buffer_desc
				contextHandle                   C.gss_ctx_id_t  = C.GSS_C_NO_CONTEXT
				acceptorCredHandle              C.gss_cred_id_t = C.GSS_C_NO_CREDENTIAL
				srcName                         C.gss_name_t
				outputToken                     C.gss_buffer_desc

				token []byte
				err   error
			)

			if err = c.SendAuthRequest(authTypeGSS, nil); err != nil {
				return nil, err
			}

			// This cleanup function must be called at the
			// "completion of a communications session", not
			// merely at the end of an authentication init. See
			// https://tools.ietf.org/html/rfc2744.html, section
			// `1. Introduction`, stage `d`:
			//
			//   At the completion of a communications session (which
			//   may extend across several transport connections),
			//   each application calls a GSS-API routine to delete
			//   the security context.
			//
			// See https://github.com/postgres/postgres/blob/f4d59369d2ddf0ad7850112752ec42fd115825d4/src/backend/libpq/pqcomm.c#L269
			connClose := func() {
				C.gss_delete_sec_context(&lminS, &contextHandle, C.GSS_C_NO_BUFFER)
			}

			for {
				token, err = c.GetPwdData()
				if err != nil {
					return connClose, err
				}

				gbuf.length = C.ulong(len(token))
				gbuf.value = C.CBytes([]byte(token))

				majStat = C.gss_accept_sec_context(
					&minStat,
					&contextHandle,
					acceptorCredHandle,
					&gbuf,
					C.GSS_C_NO_CHANNEL_BINDINGS,
					&srcName,
					nil,
					&outputToken,
					&gflags,
					nil,
					nil,
				)
				C.free(unsafe.Pointer(gbuf.value))

				if outputToken.length != 0 {
					outputBytes := C.GoBytes(outputToken.value, C.int(outputToken.length))
					C.gss_release_buffer(&lminS, &outputToken)
					if err = c.SendAuthRequest(authTypeGSSContinue, outputBytes); err != nil {
						return connClose, err
					}
				}
				if majStat != C.GSS_S_COMPLETE && majStat != C.GSS_S_CONTINUE_NEEDED {
					return connClose, gssError("accepting GSS security context failed", majStat, minStat)
				}
				if majStat != C.GSS_S_CONTINUE_NEEDED {
					break
				}
			}

			majStat = C.gss_display_name(&minStat, srcName, &gbuf, nil)
			if majStat != C.GSS_S_COMPLETE {
				return connClose, gssError("retrieving GSS user name failed", majStat, minStat)
			}
			gssUser := C.GoStringN((*C.char)(gbuf.value), C.int(gbuf.length))
			C.gss_release_buffer(&lminS, &gbuf)

			// Enforce krb_realm option, if any.
			if realms := entry.GetOptions("krb_realm"); len(realms) > 0 {
				if idx := strings.IndexByte(gssUser, '@'); idx >= 0 {
					realm := gssUser[idx+1:]
					matched := false
					for _, krbRealm := range realms {
						if realm == krbRealm {
							matched = true
							break
						}
					}
					if !matched {
						return connClose, errors.Errorf("GSSAPI realm (%s) didn't match any configured realm", realm)
					}
				} else {
					return connClose, errors.New("GSSAPI did not return realm but realm matching was requested")
				}
			}

			// Do the license check last so that administrators are able to test whether
			// their GSS configuration is correct. That is, the presence of this error
			// message means they have a correctly functioning GSS/Kerberos setup,
			// but now need to enable enterprise features.
			return connClose, utilccl.CheckEnterpriseEnabled(execCfg.Settings, execCfg.ClusterID(), execCfg.Organization(), "GSS authentication")
		}
}

func gssError(msg string, majStat, minStat C.OM_uint32) error {
	var (
		gmsg          C.gss_buffer_desc
		lminS, msgCtx C.OM_uint32
	)

	msgCtx = 0
	C.gss_display_status(&lminS, majStat, C.GSS_C_GSS_CODE, C.GSS_C_NO_OID, &msgCtx, &gmsg)
	msgMajor := C.GoString((*C.char)(gmsg.value))
	C.gss_release_buffer(&lminS, &gmsg)

	msgCtx = 0
	C.gss_display_status(&lminS, minStat, C.GSS_C_MECH_CODE, C.GSS_C_NO_OID, &msgCtx, &gmsg)
	msgMinor := C.GoString((*C.char)(gmsg.value))
	C.gss_release_buffer(&lminS, &gmsg)

	return errors.Errorf("%s: %s: %s", msg, msgMajor, msgMinor)
}

// checkEntry validates that the HBA entry contains exactly one of the
// include_realm=0 directive or an identity-mapping configuration.
func checkEntry(entry hba.Entry) error {
	hasInclude0 := false
	hasMap := false
	for _, op := range entry.Options {
		switch op[0] {
		case "include_realm":
			if op[1] == "0" {
				hasInclude0 = true
			} else {
				return errors.Errorf("include_realm must be set to 0: %s", op[1])
			}
		case "krb_realm":
		// OK.
		case "map":
			hasMap = true
		default:
			return errors.Errorf("unsupported option %s", op[0])
		}
	}
	// There's no boolean xor in go, but this does the job.
	if hasInclude0 == hasMap {
		return errors.New(`missing one of "include_realm=0" or "map" options in GSS entry`)
	}
	return nil
}

func init() {
	pgwire.RegisterAuthMethod("gss", authGSS, hba.ConnHostSSL, checkEntry)
}
