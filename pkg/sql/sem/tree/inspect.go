// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// InspectType describes the INSPECT statement operation.
type InspectType int

const (
	// InspectTable describes the INSPECT operation INSPECT TABLE.
	InspectTable InspectType = iota
	// InspectDatabase describes the INSPECT operation INSPECT DATABASE.
	InspectDatabase
)

// Inspect represents an INSPECT statement.
type Inspect struct {
	Typ     InspectType
	Options InspectOptions
	// Table is only set during INSPECT TABLE statements.
	Table *UnresolvedObjectName
	// Database is only set during INSPECT DATABASE statements.
	Database *UnresolvedObjectName
	AsOf     AsOfClause
}

// Format implements the NodeFormatter interface.
func (n *Inspect) Format(ctx *FmtCtx) {
	ctx.WriteString("INSPECT ")
	switch n.Typ {
	case InspectTable:
		ctx.WriteString("TABLE ")
		ctx.FormatNode(n.Table)
	case InspectDatabase:
		ctx.WriteString("DATABASE ")
		ctx.FormatNode(n.Database)
	default:
		panic("Unhandled InspectType")
	}

	if n.AsOf.Expr != nil {
		ctx.WriteByte(' ')
		ctx.FormatNode(&n.AsOf)
	}

	if len(n.Options) > 0 {
		ctx.WriteString(" WITH OPTIONS ")
		ctx.FormatNode(&n.Options)
	}
}

// InspectOptions corresponds to a comma-delimited list of inspect options.
type InspectOptions []InspectOption

// namedIndexes flattens and copies the indexes named by option.
func (n *InspectOptions) namedIndexes() TableIndexNames {
	var names TableIndexNames
	for _, option := range *n {
		if opt, ok := option.(*InspectOptionIndex); ok {
			for _, n := range opt.IndexNames {
				name := *n
				names = append(names, &name)
			}
		}
	}

	return names
}

// SetNamedIndexesToTable returns a copy of the named indexes with each table
// set to the specified 3-part name. It errors if the existing table name can't
// be qualified to the new table name.
func (n *InspectOptions) WithNamedIndexesOnTable(tableName *TableName) (TableIndexNames, error) {
	tabledIndexes := n.namedIndexes()
	for _, index := range tabledIndexes {
		if !indexMatchesTable(index, *tableName) {
			return nil, pgerror.Newf(pgcode.InvalidName, "index %q is not on table %q", index.String(), tableName)
		}
		index.Table = *tableName
	}
	return tabledIndexes, nil
}

// WithNamedIndexesInDatabase returns a copy of the named indexes with each
// table name set to include database specified. It errors if the database was
// previously set and is not a match.
//
// In 2-part names, the schema field is used ambiguously for either the database
// or the schema. To prevent duplication of the database name, the catalog field
// isn't set if the database name is the same as the schema name.
func (n *InspectOptions) WithNamedIndexesInDatabase(databaseName string) (TableIndexNames, error) {
	databasedIndexes := n.namedIndexes()
	for _, index := range databasedIndexes {
		if index.Table.ExplicitCatalog && index.Table.CatalogName != Name(databaseName) {
			return nil, pgerror.Newf(pgcode.InvalidName, "index %q is not in database %q", index.String(), databaseName)
		}

		if index.Table.ExplicitSchema {
			if index.Table.SchemaName != Name(databaseName) {
				index.Table.CatalogName = Name(databaseName)
				index.Table.ExplicitCatalog = true
			}
		} else {
			index.Table.SchemaName = Name(databaseName)
			index.Table.ExplicitSchema = true
		}
	}

	return databasedIndexes, nil
}

// indexMatchesTable checks if a TableIndexName matches a 3-part table name.
func indexMatchesTable(index *TableIndexName, table TableName) bool {
	if index.Table.ObjectName != "" && table.ObjectName != index.Table.ObjectName {
		return false
	}
	if index.Table.ExplicitCatalog {
		return table.CatalogName == index.Table.CatalogName && table.SchemaName == index.Table.SchemaName
	}
	if index.Table.ExplicitSchema {
		// A 2-part name means the first segment may be the schema or the catalog.
		return table.CatalogName == index.Table.SchemaName || table.SchemaName == index.Table.SchemaName
	}

	return true
}

// IsDetached returns the value of the DETACHED option, if set.
// Returns false if DETACHED is not specified.
// Conflicts are handled by validate(), so this simply returns the first
// DETACHED value found.
func (n *InspectOptions) IsDetached() bool {
	for _, option := range *n {
		if opt, ok := option.(*InspectOptionDetached); ok {
			return bool(opt.Detached)
		}
	}
	return false
}

// HasIndexAll checks if the options include an INDEX ALL option.
func (n *InspectOptions) HasIndexAll() bool {
	for _, option := range *n {
		if _, ok := option.(*InspectOptionIndexAll); ok {
			return true
		}
	}
	return false
}

// HasIndexOption checks if the options include an INDEX option with
// specific index names (i.e., INDEX (name1, name2, ...)).
// This does NOT include INDEX ALL - use HasIndexAll() for that.
func (n *InspectOptions) HasIndexOption() bool {
	for _, option := range *n {
		if _, ok := option.(*InspectOptionIndex); ok {
			return true
		}
	}
	return false
}

// Validate checks for internal consistency of the INSPECT command.
func (n *Inspect) Validate() error {
	if err := n.Options.validate(); err != nil {
		return err
	}

	return nil
}

// validate checks for internal consistency of options on the INSPECT command.
func (n *InspectOptions) validate() error {
	var hasOptionIndex, hasOptionIndexAll bool
	var detachedSeen bool
	var detachedVal bool

	for _, option := range *n {
		switch opt := option.(type) {
		case *InspectOptionIndex:
			hasOptionIndex = true
		case *InspectOptionIndexAll:
			hasOptionIndexAll = true
		case *InspectOptionDetached:
			optVal := bool(opt.Detached)
			if detachedSeen && optVal != detachedVal {
				return pgerror.Newf(pgcode.Syntax,
					"conflicting INSPECT options: DETACHED specified with different values")
			}
			detachedSeen = true
			detachedVal = optVal
		default:
			return fmt.Errorf("unknown inspect option: %T", option)
		}
	}

	// INDEX and INDEX ALL are mutually exclusive.
	if hasOptionIndex && hasOptionIndexAll {
		return pgerror.Newf(pgcode.Syntax,
			"conflicting INSPECT options: INDEX and INDEX ALL cannot be used together")
	}

	return nil
}

// Format implements the NodeFormatter interface.
func (n *InspectOptions) Format(ctx *FmtCtx) {
	for i, option := range *n {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(option)
	}
}

func (n *InspectOptions) String() string { return AsString(n) }

// InspectOption represents an inspect option.
type InspectOption interface {
	fmt.Stringer
	NodeFormatter

	inspectOptionType()
}

// InspectOptionIndex implements the InspectOption interface
func (*InspectOptionIndex) inspectOptionType() {}
func (n *InspectOptionIndex) String() string   { return AsString(n) }

// InspectOptionIndex represents an INDEX inspect check.
type InspectOptionIndex struct {
	IndexNames TableIndexNames
}

// Format implements the NodeFormatter interface.
func (n *InspectOptionIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("INDEX (")
	ctx.FormatNode(&n.IndexNames)
	ctx.WriteByte(')')
}

// InspectOptionIndexAll implements the InspectOption interface
func (*InspectOptionIndexAll) inspectOptionType() {}
func (n *InspectOptionIndexAll) String() string   { return AsString(n) }

// InspectOptionIndexAll represents an `INDEX ALL` inspect option.
type InspectOptionIndexAll struct{}

// Format implements the NodeFormatter interface.
func (n *InspectOptionIndexAll) Format(ctx *FmtCtx) {
	ctx.WriteString("INDEX ALL")
}

// InspectOptionDetached keeps track of state for the DETACHED option.
type InspectOptionDetached struct {
	Detached DBool
}

// inspectOptionType implements InspectOption.
func (*InspectOptionDetached) inspectOptionType() {}

// Format implements the NodeFormatter interface.
func (n *InspectOptionDetached) Format(ctx *FmtCtx) {
	if bool(n.Detached) {
		ctx.WriteString("DETACHED")
		return
	}
	ctx.WriteString("DETACHED = false")
}

// String implements fmt.Stringer.
func (n *InspectOptionDetached) String() string { return AsString(n) }
