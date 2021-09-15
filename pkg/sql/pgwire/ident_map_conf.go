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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
)

// serverIdentityMapSetting is the name of the cluster setting that
// holds the pg_ident configuration.
const serverIdentityMapSetting = "server.identity_map.configuration"

var connIdentityMapConf = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		serverIdentityMapSetting,
		"system-identity to database-username mappings",
		"",
		func(values *settings.Values, s string) error {
			_, err := identmap.From(strings.NewReader(s))
			return err
		},
	)
	s.SetVisibility(settings.Public)
	return s
}()
