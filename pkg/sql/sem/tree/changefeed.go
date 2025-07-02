// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

type ChangefeedLevel int

const (
	// ChangefeedLevelTable is used for table-level changefeeds e.g.
	// CREATE CHANGEFEED FOR TABLE t;
	ChangefeedLevelTable ChangefeedLevel = iota
	// ChangefeedLevelDatabase is used for database-level changefeeds e.g.
	// CREATE CHANGEFEED FOR DATABASE d;
	ChangefeedLevelDatabase
)

// CreateChangefeed represents a CREATE CHANGEFEED statement.
type CreateChangefeed struct {
	TableTargets   ChangefeedTableTargets
	DatabaseTarget ChangefeedDatabaseTarget
	Level          ChangefeedLevel
	SinkURI        Expr
	Options        KVOptions
	Select         *SelectClause
}

var _ Statement = &CreateChangefeed{}

// Format implements the NodeFormatter interface.
func (node *CreateChangefeed) Format(ctx *FmtCtx) {
	if node.Select != nil {
		node.formatWithPredicates(ctx)
		return
	}
	if node.SinkURI != nil || node.Level == ChangefeedLevelDatabase {
		ctx.WriteString("CREATE ")
	} else {
		// Sinkless feeds don't really CREATE anything, so the syntax omits the
		// prefix. They're also still EXPERIMENTAL, so they get marked as such.
		ctx.WriteString("EXPERIMENTAL ")
	}

	ctx.WriteString("CHANGEFEED FOR ")
	if node.Level == ChangefeedLevelTable {
		ctx.FormatNode(&node.TableTargets)
	} else {
		ctx.FormatNode(&node.DatabaseTarget)
	}
	if node.SinkURI != nil {
		ctx.WriteString(" INTO ")
		ctx.FormatURI(node.SinkURI)
	}
	if node.Options != nil {
		ctx.WriteString(" WITH OPTIONS (")
		ctx.FormatNode(&node.Options)
		ctx.WriteString(")")
	}
}

// formatWithPredicates is a helper to format node when creating
// changefeed with predicates.
func (node *CreateChangefeed) formatWithPredicates(ctx *FmtCtx) {
	ctx.WriteString("CREATE CHANGEFEED")
	if node.SinkURI != nil {
		ctx.WriteString(" INTO ")
		ctx.FormatNode(node.SinkURI)
	}
	if node.Options != nil {
		ctx.WriteString(" WITH OPTIONS (")
		ctx.FormatNode(&node.Options)
		ctx.WriteString(")")
	}
	ctx.WriteString(" AS ")
	ctx.FormatNode(node.Select)
}

type ChangefeedDatabaseTarget Name

func (ct *ChangefeedDatabaseTarget) Format(ctx *FmtCtx) {
	ctx.WriteString("DATABASE ")
	ctx.FormatName(string(*ct))
}

// ChangefeedTarget represents a database object to be watched by a changefeed.
type ChangefeedTableTarget struct {
	TableName  TablePattern
	FamilyName Name
}

func (ct *ChangefeedTableTarget) GetName() string {
	return ct.TableName.String()
}

// Format implements the NodeFormatter interface.
func (ct *ChangefeedTableTarget) Format(ctx *FmtCtx) {
	ctx.WriteString("TABLE ")
	ctx.FormatNode(ct.TableName)
	if ct.FamilyName != "" {
		ctx.WriteString(" FAMILY ")
		ctx.FormatNode(&ct.FamilyName)
	}
}

// ChangefeedTargets represents a list of database objects to be watched by a changefeed.
type ChangefeedTableTargets []ChangefeedTableTarget

// Format implements the NodeFormatter interface.
func (cts *ChangefeedTableTargets) Format(ctx *FmtCtx) {
	for i := range *cts {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&(*cts)[i])
	}
}

// ChangefeedTargetFromTableExpr returns ChangefeedTarget for the
// specified table expression.
func ChangefeedTargetFromTableExpr(e TableExpr) (ChangefeedTableTarget, error) {
	switch t := e.(type) {
	case TablePattern:
		return ChangefeedTableTarget{TableName: t}, nil
	case *AliasedTableExpr:
		if tn, ok := t.Expr.(*TableName); ok {
			return ChangefeedTableTarget{TableName: tn}, nil
		}
	}
	return ChangefeedTableTarget{}, pgerror.Newf(
		pgcode.InvalidName, "unsupported changefeed target type")
}
