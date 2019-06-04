// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package delegate

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

func (d *delegator) delegateShowAllClusterSettings(
	stmt *tree.ShowAllClusterSettings,
) (tree.Statement, error) {
	if err := d.catalog.RequireSuperUser(d.ctx, "SHOW ALL CLUSTER SETTINGS"); err != nil {
		return nil, err
	}
	return parse(
		`SELECT variable, value, type AS setting_type, description
     FROM crdb_internal.cluster_settings`,
	)
}
