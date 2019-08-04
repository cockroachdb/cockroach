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

func (d *delegator) delegateShowAllClusterSettings(
	stmt *tree.ShowAllClusterSettings,
) (tree.Statement, error) {
	if err := d.catalog.RequireAdminRole(d.ctx, "SHOW ALL CLUSTER SETTINGS"); err != nil {
		return nil, err
	}
	return parse(
		`SELECT variable, value, type AS setting_type, description
     FROM crdb_internal.cluster_settings`,
	)
}
