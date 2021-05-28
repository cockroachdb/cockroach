// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package certmgr

import (
	"context"
	"crypto/tls"
)

//go:generate mockgen -package=certmgr -destination=mocks_generated.go -source=cert.go . Cert

// Cert is a generic presentation on managed certificate that can be reloaded.
// A managed certificate allows the certificates to be rotated by a cert manager.
// If a certificate isn't reloadable, picking a new cert version will require
// restart which isn't always feasible.
// The certification manager that manages these certs, will monitor for SIGHUP and
// call reload on each cert when the signal is received.
// A similar code exists for the cockroach executable but the code there is
// very specific for the set of certificates that cockroach db uses. This one
// is more generic and can be used as a building block in any application.
type Cert interface {
	// Reload will refresh the certificate. For certs loaded from files - this
	// means reloading from the files to pick up a new version. If there is an
	// error during the reload, Err() will return the generated error. The main
	// reason why Reload doesn't return the error itself is that the typical call
	// to reload will be done by the go routine that monitors for SIGHUP.
	// So without storing the error, and allowing for a deferred retrieval via
	// Err() - the code that uses the cert wouldn't know that an error occurred.
	//
	// A typical user pattern would be:
	// if tlsCert := fileCert.TLSCert(); fileCert.Err() != nil {
	//   .. try to fix or log the condition causing error and call
	//   .. fileCert.ClearErr() to allow reloads to resume
	// }
	Reload(ctx context.Context)
	// Err will return the last error that occurred during reload or nil if the
	// last reload was successful.
	Err() error
	// ClearErr will clear the last reported err and allow reloads to resume.
	ClearErr()
	// TLSCert will return the last loaded tls.Certificate
	TLSCert() *tls.Certificate
}
