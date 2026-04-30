// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// AlterDomain implements ALTER DOMAIN.
func (p *planner) AlterDomain(
	ctx context.Context, n *tree.AlterDomain,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER DOMAIN",
	); err != nil {
		return nil, err
	}

	var cmdName string
	switch n.Cmd.(type) {
	case *tree.AlterDomainSetDefault:
		cmdName = "ALTER DOMAIN SET DEFAULT"
	case *tree.AlterDomainDropDefault:
		cmdName = "ALTER DOMAIN DROP DEFAULT"
	case *tree.AlterDomainSetNotNull:
		cmdName = "ALTER DOMAIN SET NOT NULL"
	case *tree.AlterDomainDropNotNull:
		cmdName = "ALTER DOMAIN DROP NOT NULL"
	case *tree.AlterDomainAddConstraint:
		cmdName = "ALTER DOMAIN ADD CONSTRAINT"
	case *tree.AlterDomainDropConstraint:
		cmdName = "ALTER DOMAIN DROP CONSTRAINT"
	case *tree.AlterDomainRenameConstraint:
		cmdName = "ALTER DOMAIN RENAME CONSTRAINT"
	case *tree.AlterDomainValidateConstraint:
		cmdName = "ALTER DOMAIN VALIDATE CONSTRAINT"
	case *tree.AlterDomainOwner:
		cmdName = "ALTER DOMAIN OWNER TO"
	case *tree.AlterDomainRename:
		cmdName = "ALTER DOMAIN RENAME TO"
	case *tree.AlterDomainSetSchema:
		cmdName = "ALTER DOMAIN SET SCHEMA"
	default:
		cmdName = fmt.Sprintf("ALTER DOMAIN %T", n.Cmd)
	}
	return nil, pgerror.Newf(pgcode.FeatureNotSupported,
		"unimplemented: %s", cmdName)
}
