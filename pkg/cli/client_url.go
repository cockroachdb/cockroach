// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
