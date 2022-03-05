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

// AlterTenantSetClusterSetting represents an ALTER TENANT
// SET CLUSTER SETTING statement.
type AlterTenantSetClusterSetting struct {
	SetClusterSetting
	TenantID  Expr
	TenantAll bool
}

// Format implements the NodeFormatter interface.
func (n *AlterTenantSetClusterSetting) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER ")
	if n.TenantAll {
		ctx.WriteString("ALL TENANTS")
	} else {
		ctx.WriteString("TENANT ")
		ctx.FormatNode(n.TenantID)
	}
	ctx.WriteByte(' ')
	ctx.FormatNode(&n.SetClusterSetting)
}

// ShowTenantClusterSetting represents a SHOW CLUSTER SETTING ... FOR TENANT statement.
type ShowTenantClusterSetting struct {
	*ShowClusterSetting
	TenantID Expr
}

// Format implements the NodeFormatter interface.
func (node *ShowTenantClusterSetting) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.ShowClusterSetting)
	ctx.WriteString(" FOR TENANT ")
	ctx.FormatNode(node.TenantID)
}

// ShowTenantClusterSettingList represents a SHOW CLUSTER SETTINGS FOR TENANT statement.
type ShowTenantClusterSettingList struct {
	*ShowClusterSettingList
	TenantID Expr
}

// Format implements the NodeFormatter interface.
func (node *ShowTenantClusterSettingList) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.ShowClusterSettingList)
	ctx.WriteString(" FOR TENANT ")
	ctx.FormatNode(node.TenantID)
}
