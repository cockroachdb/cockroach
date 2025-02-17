// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// See comment on build tag in gssapi.go.

//go:build gss

package gssapiccl

// This file contains the code that calls out to the GSSAPI library
// to retrieve the current user.

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/errors"
)

// #cgo LDFLAGS: -lgssapi_krb5 -lcom_err -lkrb5 -lkrb5support -ldl -lk5crypto -lresolv
//
// #include <gssapi/gssapi.h>
// #include <stdlib.h>
import "C"

func getGssUser(c pgwire.AuthConn) (connClose func(), gssUser string, _ error) {
	var (
		majStat, minStat, lminS, gflags C.OM_uint32
		gbuf                            C.gss_buffer_desc
		contextHandle                   C.gss_ctx_id_t  = C.GSS_C_NO_CONTEXT
		acceptorCredHandle              C.gss_cred_id_t = C.GSS_C_NO_CREDENTIAL
		srcName                         C.gss_name_t    = C.GSS_C_NO_NAME
		outputToken                     C.gss_buffer_desc
	)

	if err := c.SendAuthRequest(authTypeGSS, nil); err != nil {
		return nil, "", err
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
	connClose = func() {
		var minorStatus C.OM_uint32
		C.gss_delete_sec_context(&minorStatus, &contextHandle, C.GSS_C_NO_BUFFER)
		C.gss_release_name(&minorStatus, &srcName)
	}

	for {
		token, err := c.GetPwdData()
		if err != nil {
			return connClose, "", err
		}

		gbuf.length = C.ulong(len(token))
		gbuf.value = C.CBytes(token)

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
		C.free(gbuf.value)

		if outputToken.length != 0 {
			outputBytes := C.GoBytes(outputToken.value, C.int(outputToken.length))
			C.gss_release_buffer(&lminS, &outputToken)
			if err := c.SendAuthRequest(authTypeGSSContinue, outputBytes); err != nil {
				return connClose, "", err
			}
		}
		if majStat != C.GSS_S_COMPLETE && majStat != C.GSS_S_CONTINUE_NEEDED {
			return connClose, "", gssError("accepting GSS security context failed", majStat, minStat)
		}
		if majStat != C.GSS_S_CONTINUE_NEEDED {
			break
		}
	}

	majStat = C.gss_display_name(&minStat, srcName, &gbuf, nil)
	if majStat != C.GSS_S_COMPLETE {
		return connClose, "", gssError("retrieving GSS user name failed", majStat, minStat)
	}
	gssUser = C.GoStringN((*C.char)(gbuf.value), C.int(gbuf.length))
	C.gss_release_buffer(&lminS, &gbuf)

	return connClose, gssUser, nil
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
