// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwire

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// serverIdentityMapSetting is the name of the cluster setting that
// holds the pg_ident configuration.
const serverIdentityMapSetting = "server.identity_map.configuration"

// ConnIdentityMapConf maps system-identities to database-usernames using the pg_ident.conf format.
var ConnIdentityMapConf = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	serverIdentityMapSetting,
	"system-identity to database-username mappings",
	"",
	settings.WithValidateString(func(values *settings.Values, s string) error {
		_, err := identmap.From(strings.NewReader(s))
		return err
	},
	),
	settings.WithPublic,
)

// loadLocalIdentityMapUponRemoteSettingChange initializes the local
// node's cache of the identity map configuration each time the cluster
// setting is updated.
func loadLocalIdentityMapUponRemoteSettingChange(
	ctx context.Context, server *Server, st *cluster.Settings,
) {
	val := ConnIdentityMapConf.Get(&st.SV)
	idMap, err := identmap.From(strings.NewReader(val))
	if err != nil {
		log.Ops.Warningf(ctx, "invalid %s: %v", serverIdentityMapSetting, err)
		idMap = identmap.Empty()
	}

	server.auth.Lock()
	defer server.auth.Unlock()
	server.auth.identityMap = idMap
}
