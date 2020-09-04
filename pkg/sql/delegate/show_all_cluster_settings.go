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

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (d *delegator) delegateShowClusterSettingList(
	stmt *tree.ShowClusterSettingList,
) (tree.Statement, error) {
	hasModify, err := d.catalog.HasRoleOption(d.ctx, roleoption.MODIFYCLUSTERSETTING)
	if err != nil {
		return nil, err
	}
	if !hasModify {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"only users with the %s privilege are allowed to SHOW CLUSTER SETTINGS",
			roleoption.MODIFYCLUSTERSETTING)
	}
	if stmt.All {
		return parse(
			`SELECT variable, value, type AS setting_type, public, description
       FROM   crdb_internal.cluster_settings`,
		)
	}
	return parse(
		`SELECT variable, value, type AS setting_type, description
     FROM   crdb_internal.cluster_settings
     WHERE  public IS TRUE`,
	)
}
