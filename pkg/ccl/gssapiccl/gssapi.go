// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// +build linux

package gssapiccl

import (
	"crypto/tls"
	"strings"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/pkg/errors"
)

// #cgo LDFLAGS: -lgssapi_krb5 -lcom_err -lkrb5 -lkrb5support -ldl -lk5crypto -lresolv
//
// #include <gssapi/gssapi.h>
// #include <stdlib.h>
import "C"

// authGSS performs GSS authentication. See:
// https:github.com/postgres/postgres/blob/0f9cdd7dca694d487ab663d463b308919f591c02/src/backend/libpq/auth.c#L1090
func authGSS(
	c pgwire.AuthConn, tlsState tls.ConnectionState, insecure bool, hashedPassword []byte,
) (security.UserAuthHook, error) {
	return func(requestedUser string, clientConnection bool) error {
		var (
			maj_stat, min_stat, lmin_s, gflags C.OM_uint32
			gbuf                               C.gss_buffer_desc
			context_handle                     C.gss_ctx_id_t  = C.GSS_C_NO_CONTEXT
			acceptor_cred_handle               C.gss_cred_id_t = C.GSS_C_NO_CREDENTIAL
			src_name                           C.gss_name_t
			output_token                       C.gss_buffer_desc

			token []byte
			err   error
		)

		if err = c.SendAuthRequest(pgwire.AuthTypeGSS, nil); err != nil {
			return err
		}

		for {
			token, err = c.ReadGSSResponse()
			if err != nil {
				return err
			}

			gbuf.length = C.ulong(len(token))
			gbuf.value = C.CBytes([]byte(token))

			maj_stat = C.gss_accept_sec_context(
				&min_stat,
				&context_handle,
				acceptor_cred_handle,
				&gbuf,
				C.GSS_C_NO_CHANNEL_BINDINGS,
				&src_name,
				nil,
				&output_token,
				&gflags,
				nil,
				nil,
			)
			C.free(unsafe.Pointer(gbuf.value))

			if output_token.length != 0 {
				output_bytes := C.GoBytes(output_token.value, C.int(output_token.length))
				C.gss_release_buffer(&lmin_s, &output_token)
				if err = c.SendAuthRequest(pgwire.AuthTypeGSSContinue, output_bytes); err != nil {
					return err
				}
			}
			if maj_stat != C.GSS_S_COMPLETE && maj_stat != C.GSS_S_CONTINUE_NEEDED {
				C.gss_delete_sec_context(&lmin_s, &context_handle, C.GSS_C_NO_BUFFER)
				return gss_error("accepting GSS security context failed", maj_stat, min_stat)
			}
			if maj_stat != C.GSS_S_CONTINUE_NEEDED {
				break
			}
		}

		maj_stat = C.gss_display_name(&min_stat, src_name, &gbuf, nil)
		if maj_stat != C.GSS_S_COMPLETE {
			return gss_error("retrieving GSS user name failed", maj_stat, min_stat)
		}
		gssUser := string(C.GoBytes(gbuf.value, C.int(gbuf.length)))
		C.gss_release_buffer(&lmin_s, &gbuf)

		if !strings.EqualFold(gssUser, requestedUser) {
			return errors.Errorf("requested user is %s, but GSS auth is for %s", requestedUser, gssUser)
		}

		return nil
	}, nil
}

func gss_error(msg string, maj_stat, min_stat C.OM_uint32) error {
	var (
		gmsg            C.gss_buffer_desc
		lmin_s, msg_ctx C.OM_uint32
	)

	msg_ctx = 0
	C.gss_display_status(&lmin_s, maj_stat, C.GSS_C_GSS_CODE, C.GSS_C_NO_OID, &msg_ctx, &gmsg)
	msg_major := C.GoString((*C.char)(gmsg.value))
	C.gss_release_buffer(&lmin_s, &gmsg)

	msg_ctx = 0
	C.gss_display_status(&lmin_s, min_stat, C.GSS_C_MECH_CODE, C.GSS_C_NO_OID, &msg_ctx, &gmsg)
	msg_minor := C.GoString((*C.char)(gmsg.value))
	C.gss_release_buffer(&lmin_s, &gmsg)

	return errors.Errorf("%s: %s: %s", msg, msg_major, msg_minor)
}

func enabledIfEnterprise(cfg *sql.ExecutorConfig) error {
	return utilccl.CheckEnterpriseEnabled(cfg.Settings, cfg.ClusterID(), cfg.Organization(), "GSS authentication")
}

func init() {
	pgwire.RegisterAuthMethod("gss", authGSS, enabledIfEnterprise)
}
