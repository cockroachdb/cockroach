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

import "github.com/cockroachdb/errors"

// ReplicationCutoverTime represent the user-specified cutover time
type ReplicationCutoverTime struct {
	Timestamp Expr
	Latest    bool
}

// AlterTenantReplication represents an ALTER TENANT REPLICATION statement.
type AlterTenantReplication struct {
	TenantSpec *TenantSpec
	Command    JobCommand
	Cutover    *ReplicationCutoverTime
	Options    TenantReplicationOptions
}

var _ Statement = &AlterTenantReplication{}

// Format implements the NodeFormatter interface.
func (n *AlterTenantReplication) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER TENANT ")
	ctx.FormatNode(n.TenantSpec)
	ctx.WriteByte(' ')
	if n.Cutover != nil {
		ctx.WriteString("COMPLETE REPLICATION TO ")
		if n.Cutover.Latest {
			ctx.WriteString("LATEST")
		} else {
			ctx.WriteString("SYSTEM TIME ")
			ctx.FormatNode(n.Cutover.Timestamp)
		}
	} else if !n.Options.IsDefault() {
		ctx.WriteString("SET REPLICATION ")
		ctx.FormatNode(&n.Options)
	} else if n.Command == PauseJob || n.Command == ResumeJob {
		ctx.WriteString(JobCommandToStatement[n.Command])
		ctx.WriteString(" REPLICATION")
	}
}

// TenantCapability is a key-value parameter representing a tenant capability.
type TenantCapability struct {
	Name  string
	Value Expr
}

func (c *TenantCapability) GetBoolValue() (bool, error) {
	if c.Value == nil {
		return false, nil
	}
	dBool, ok := AsDBool(c.Value)
	if !ok {
		return false, errors.New("must be bool")
	}
	return bool(dBool), nil
}

// AlterTenantCapability represents an ALTER TENANT CAPABILITY statement.
type AlterTenantCapability struct {
	TenantSpec   *TenantSpec
	Capabilities []TenantCapability
	IsReset      bool
}

var _ Statement = &AlterTenantCapability{}

// Format implements the NodeFormatter interface.
func (n *AlterTenantCapability) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER TENANT ")
	ctx.FormatNode(n.TenantSpec)
	ctx.WriteByte(' ')
	if n.IsReset {
		ctx.WriteString("REVOKE CAPABILITY ")
		for i, capability := range n.Capabilities {
			if i > 0 {
				ctx.WriteString(", ")
			}
			ctx.WriteString(capability.Name)
		}
	} else {
		ctx.WriteString("GRANT CAPABILITY ")
		for i, capability := range n.Capabilities {
			if i > 0 {
				ctx.WriteString(", ")
			}
			ctx.WriteString(capability.Name)
			ctx.WriteString(" = ")
			ctx.FormatNode(capability.Value)
		}
	}
}

// TenantSpec designates a tenant for the ALTER TENANT statements.
type TenantSpec struct {
	Expr   Expr
	IsName bool
	All    bool
}

// Format implements the NodeFormatter interface.
func (n *TenantSpec) Format(ctx *FmtCtx) {
	if n.All {
		ctx.WriteString("ALL")
	} else if n.IsName {
		ctx.FormatNode(n.Expr)
	} else {
		ctx.WriteByte('[')
		ctx.FormatNode(n.Expr)
		ctx.WriteByte(']')
	}
}

// AlterTenantRename represents an ALTER TENANT RENAME statement.
type AlterTenantRename struct {
	TenantSpec *TenantSpec
	NewName    Expr
}

var _ Statement = &AlterTenantRename{}

// Format implements the NodeFormatter interface.
func (n *AlterTenantRename) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER TENANT ")
	ctx.FormatNode(n.TenantSpec)
	ctx.WriteString(" RENAME TO ")
	ctx.FormatNode(n.NewName)
}
