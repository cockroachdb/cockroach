// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// ShowZoneConfig only delegates if it selecting ALL configurations.
func (d *delegator) delegateShowZoneConfig(n *tree.ShowZoneConfig) (tree.Statement, error) {
	// Specifying a specific zone; fallback to non-delegation logic.
	if n.ZoneSpecifier != (tree.ZoneSpecifier{}) {
		return nil, nil
	}
	return d.parse(`SELECT target, raw_config_sql FROM crdb_internal.zones`)
}
