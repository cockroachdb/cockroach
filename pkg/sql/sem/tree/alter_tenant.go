// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// AlterTenantSetClusterSetting represents an ALTER TENANT
// SET CLUSTER SETTING statement.
type AlterTenantSetClusterSetting struct {
	Name      string
	Value     Expr
	TenantID  roachpb.TenantID
	TenantAll bool
}

// Format implements the NodeFormatter interface.
func (node *AlterTenantSetClusterSetting) Format(ctx *FmtCtx) {
	if node.TenantAll {
		ctx.WriteString("ALTER TENANT ALL ")
	} else if node.TenantID.IsSet() {
		s := fmt.Sprintf("ALTER TENANT %d ", node.TenantID.ToUint64())
		ctx.WriteString(s)
	}
	ctx.WriteString("SET CLUSTER SETTING ")

	// Cluster setting names never contain PII and should be distinguished
	// for feature tracking purposes.
	ctx.WithFlags(ctx.flags & ^FmtAnonymize & ^FmtMarkRedactionNode, func() {
		ctx.FormatNameP(&node.Name)
	})

	ctx.WriteString(" = ")

	switch v := node.Value.(type) {
	case *DBool, *DInt:
		ctx.WithFlags(ctx.flags & ^FmtAnonymize & ^FmtMarkRedactionNode, func() {
			ctx.FormatNode(v)
		})
	default:
		ctx.FormatNode(v)
	}
}
