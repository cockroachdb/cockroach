// Copyright 2017 The Cockroach Authors.
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

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ShowRoles returns all the roles.
// Privileges: SELECT on system.users.
func (p *planner) ShowRoles(ctx context.Context, n *tree.ShowRoles) (planNode, error) {
	return p.delegateQuery(ctx, "SHOW ROLES",
		`SELECT username FROM system.public.users WHERE "isRole" = true ORDER BY 1`, nil, nil)
}
