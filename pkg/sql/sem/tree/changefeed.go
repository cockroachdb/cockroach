// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// Changefeed option names whose values carry secrets. They are defined here
// rather than in ccl/changefeedccl/changefeedbase, which owns changefeed
// option parsing, because the Format methods must redact these values and
// cannot depend on that package. changefeedbase aliases these constants and
// builds its redaction map from ChangefeedSecretOptions, so there is no second
// list to keep in sync.
const (
	// ChangefeedOptSink is the ALTER CHANGEFEED option holding the sink URI.
	ChangefeedOptSink = `sink`
	// ChangefeedOptWebhookAuthHeader is the authorization header sent to
	// webhook sinks.
	ChangefeedOptWebhookAuthHeader = `webhook_auth_header`
	// ChangefeedOptExtraHeaders is the set of extra headers sent to Kafka and
	// webhook sinks.
	ChangefeedOptExtraHeaders = `extra_headers`
	// ChangefeedOptConfluentSchemaRegistry is the schema registry URI, which
	// can embed credentials.
	ChangefeedOptConfluentSchemaRegistry = `confluent_schema_registry`
)

// ChangefeedSecretOptions is the set of changefeed option names whose values
// carry secrets.
var ChangefeedSecretOptions = map[string]struct{}{
	ChangefeedOptSink:                    {},
	ChangefeedOptWebhookAuthHeader:       {},
	ChangefeedOptExtraHeaders:            {},
	ChangefeedOptConfluentSchemaRegistry: {},
}

// formatChangefeedOptionValue formats one changefeed option value, routing
// secret-bearing values through FormatURI so they redact under default format
// flags. It is shared by every statement that formats changefeed options
// (CREATE CHANGEFEED, CREATE SCHEDULE FOR CHANGEFEED, ALTER CHANGEFEED).
func formatChangefeedOptionValue(n *KVOption, ctx *FmtCtx) {
	if _, ok := ChangefeedSecretOptions[string(n.Key)]; ok {
		ctx.FormatURI(n.Value)
		return
	}
	ctx.FormatNode(n.Value)
}

// CreateChangefeed represents a CREATE CHANGEFEED statement.
type CreateChangefeed struct {
	Targets ChangefeedTargets
	SinkURI Expr
	Options KVOptions
	Select  *SelectClause
}

var _ Statement = &CreateChangefeed{}

// Format implements the NodeFormatter interface.
func (node *CreateChangefeed) Format(ctx *FmtCtx) {
	if node.Select != nil {
		node.formatWithPredicates(ctx)
		return
	}

	if node.SinkURI != nil {
		ctx.WriteString("CREATE ")
	} else {
		// Sinkless feeds don't really CREATE anything, so the syntax omits the
		// prefix. They're also still EXPERIMENTAL, so they get marked as such.
		ctx.WriteString("EXPERIMENTAL ")
	}

	ctx.WriteString("CHANGEFEED FOR ")
	ctx.FormatNode(&node.Targets)
	if node.SinkURI != nil {
		ctx.WriteString(" INTO ")
		ctx.FormatURI(node.SinkURI)
	}
	if node.Options != nil {
		ctx.WriteString(" WITH OPTIONS (")
		node.Options.formatEach(ctx, formatChangefeedOptionValue)
		ctx.WriteString(")")
	}
}

// formatWithPredicates is a helper to format node when creating
// changefeed with predicates.
func (node *CreateChangefeed) formatWithPredicates(ctx *FmtCtx) {
	ctx.WriteString("CREATE CHANGEFEED")
	if node.SinkURI != nil {
		ctx.WriteString(" INTO ")
		ctx.FormatURI(node.SinkURI)
	}
	if node.Options != nil {
		ctx.WriteString(" WITH OPTIONS (")
		node.Options.formatEach(ctx, formatChangefeedOptionValue)
		ctx.WriteString(")")
	}
	ctx.WriteString(" AS ")
	ctx.FormatNode(node.Select)
}

// ChangefeedTarget represents a database object to be watched by a changefeed.
type ChangefeedTarget struct {
	TableName  TablePattern
	FamilyName Name
}

// Format implements the NodeFormatter interface.
func (ct *ChangefeedTarget) Format(ctx *FmtCtx) {
	ctx.WriteString("TABLE ")
	ctx.FormatNode(ct.TableName)
	if ct.FamilyName != "" {
		ctx.WriteString(" FAMILY ")
		ctx.FormatNode(&ct.FamilyName)
	}
}

// ChangefeedTargets represents a list of database objects to be watched by a changefeed.
type ChangefeedTargets []ChangefeedTarget

// Format implements the NodeFormatter interface.
func (cts *ChangefeedTargets) Format(ctx *FmtCtx) {
	for i := range *cts {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&(*cts)[i])
	}
}

// ChangefeedTargetFromTableExpr returns ChangefeedTarget for the
// specified table expression.
func ChangefeedTargetFromTableExpr(e TableExpr) (ChangefeedTarget, error) {
	switch t := e.(type) {
	case TablePattern:
		return ChangefeedTarget{TableName: t}, nil
	case *AliasedTableExpr:
		if tn, ok := t.Expr.(*TableName); ok {
			return ChangefeedTarget{TableName: tn}, nil
		}
	}
	return ChangefeedTarget{}, pgerror.Newf(
		pgcode.InvalidName, "unsupported changefeed target type")
}
