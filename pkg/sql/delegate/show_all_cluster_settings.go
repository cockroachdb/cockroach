// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
