// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"github.com/cockroachdb/cockroach/pkg/security/clientsecopts"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
)

// makeClientConnURL constructs a connection URL from the parsed options.
func (cliCtx *cliContext) makeClientConnURL() (*pgurl.URL, error) {
	copts := cliCtx.clientOpts
	copts.ClientSecurityOptions = clientsecopts.ClientSecurityOptions{
		Insecure: cliCtx.Config.Insecure,
		CertsDir: cliCtx.Config.SSLCertsDir,
	}
	return clientsecopts.MakeClientConnURL(copts)
}
