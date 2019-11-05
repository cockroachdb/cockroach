// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package delegate

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// ShowZoneConfig only delegates if it selecting ALL configurations.
func (d *delegator) delegateShowZoneConfig(n *tree.ShowZoneConfig) (tree.Statement, error) {
	// Specifying a specific zone; fallback to non-delegation logic.
	if n.ZoneSpecifier != (tree.ZoneSpecifier{}) {
		return nil, nil
	}
	return parse(`SELECT target, raw_config_sql FROM crdb_internal.zones`)
}
