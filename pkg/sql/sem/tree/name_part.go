// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import "github.com/cockroachdb/cockroach/pkg/sql/lex"

// A Name is an SQL identifier.
//
// In general, a Name is the result of parsing a name nonterminal, which is used
// in the grammar where reserved keywords cannot be distinguished from
// identifiers. A Name that matches a reserved keyword must thus be quoted when
// formatted. (Names also need quoting for a variety of other reasons; see
// isBareIdentifier.)
//
// For historical reasons, some Names are instead the result of parsing
// `unrestricted_name` nonterminals. See UnrestrictedName for details.
type Name string

// Format implements the NodeFormatter interface.
func (n *Name) Format(ctx *FmtCtx) {
	f := ctx.flags
	if f.HasFlags(FmtAnonymize) && !isArityIndicatorString(string(*n)) {
		ctx.WriteByte('_')
	} else {
		lex.EncodeRestrictedSQLIdent(&ctx.Buffer, string(*n), f.EncodeFlags())
	}
}

// NameStringP escapes an identifier stored in a heap string to a SQL
// identifier, avoiding a heap allocation.
func NameStringP(s *string) string {
	return ((*Name)(s)).String()
}

// NameString escapes an identifier stored in a string to a SQL
// identifier.
func NameString(s string) string {
	return ((*Name)(&s)).String()
}

// ErrNameStringP escapes an identifier stored a string to a SQL
// identifier suitable for printing in error messages, avoiding a heap
// allocation.
func ErrNameStringP(s *string) string {
	return ErrString(((*Name)(s)))
}

// ErrNameString escapes an identifier stored a string to a SQL
// identifier suitable for printing in error messages.
func ErrNameString(s string) string {
	return ErrString(((*Name)(&s)))
}

// Normalize normalizes to lowercase and Unicode Normalization Form C
// (NFC).
func (n Name) Normalize() string {
	return lex.NormalizeName(string(n))
}

// An UnrestrictedName is a Name that does not need to be escaped when it
// matches a reserved keyword.
//
// In general, an UnrestrictedName is the result of parsing an unrestricted_name
// nonterminal, which is used in the grammar where reserved keywords can be
// unambiguously interpreted as identifiers. When formatted, an UnrestrictedName
// that matches a reserved keyword thus does not need to be quoted.
//
// For historical reasons, some unrestricted_name nonterminals are instead
// parsed as Names. The only user-visible impact of this is that we are too
// aggressive about quoting names in certain positions. New grammar rules should
// prefer to parse unrestricted_name nonterminals into UnrestrictedNames.
type UnrestrictedName string

// Format implements the NodeFormatter interface.
func (u *UnrestrictedName) Format(ctx *FmtCtx) {
	f := ctx.flags
	if f.HasFlags(FmtAnonymize) {
		ctx.WriteByte('_')
	} else {
		lex.EncodeUnrestrictedSQLIdent(&ctx.Buffer, string(*u), f.EncodeFlags())
	}
}

// ToStrings converts the name list to an array of regular strings.
func (l NameList) ToStrings() []string {
	if l == nil {
		return nil
	}
	names := make([]string, len(l))
	for i, n := range l {
		names[i] = string(n)
	}
	return names
}

// A NameList is a list of identifiers.
type NameList []Name

// Format implements the NodeFormatter interface.
func (l *NameList) Format(ctx *FmtCtx) {
	for i := range *l {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&(*l)[i])
	}
}

// ArraySubscript corresponds to the syntax `<name>[ ... ]`.
type ArraySubscript struct {
	Begin Expr
	End   Expr
	Slice bool
}

// Format implements the NodeFormatter interface.
func (a *ArraySubscript) Format(ctx *FmtCtx) {
	ctx.WriteByte('[')
	if a.Begin != nil {
		ctx.FormatNode(a.Begin)
	}
	if a.Slice {
		ctx.WriteByte(':')
		if a.End != nil {
			ctx.FormatNode(a.End)
		}
	}
	ctx.WriteByte(']')
}

// UnresolvedName corresponds to an unresolved qualified name.
type UnresolvedName struct {
	// NumParts indicates the number of name parts specified, including
	// the star. Always 1 or greater.
	NumParts int

	// Star indicates the name ends with a star.
	// In that case, Parts below is empty in the first position.
	Star bool

	// Parts are the name components, in reverse order.
	// There are at most 4: column, table, schema, catalog/db.
	//
	// Note: NameParts has a fixed size so that we avoid a heap
	// allocation for the slice every time we construct an
	// UnresolvedName. It does imply however that Parts does not have
	// a meaningful "length"; its actual length (the number of parts
	// specified) is populated in NumParts above.
	Parts NameParts
}

// NameParts is the array of strings that composes the path in an
// UnresolvedName.
type NameParts = [4]string

// Format implements the NodeFormatter interface.
func (u *UnresolvedName) Format(ctx *FmtCtx) {
	stopAt := 1
	if u.Star {
		stopAt = 2
	}
	for i := u.NumParts; i >= stopAt; i-- {
		// The first part to print is the last item in u.Parts.  It is also
		// a potentially restricted name to disambiguate from keywords in
		// the grammar, so print it out as a "Name". Every part after that is
		// necessarily an unrestricted name.
		if i == u.NumParts {
			ctx.FormatNode((*Name)(&u.Parts[i-1]))
		} else {
			ctx.FormatNode((*UnrestrictedName)(&u.Parts[i-1]))
		}
		if i > 1 {
			ctx.WriteByte('.')
		}
	}
	if u.Star {
		ctx.WriteByte('*')
	}
}
func (u *UnresolvedName) String() string { return AsString(u) }

// NewUnresolvedName constructs an UnresolvedName from some strings.
func NewUnresolvedName(args ...string) *UnresolvedName {
	n := MakeUnresolvedName(args...)
	return &n
}

// MakeUnresolvedName constructs an UnresolvedName from some strings.
func MakeUnresolvedName(args ...string) UnresolvedName {
	n := UnresolvedName{NumParts: len(args)}
	for i := 0; i < len(args); i++ {
		n.Parts[i] = args[len(args)-1-i]
	}
	return n
}

// UnresolvedObjectName is an unresolved qualified name for a database object
// (table, view, etc). It is like UnresolvedName but more restrictive.
// It should only be constructed via NewUnresolvedObjectName.
type UnresolvedObjectName struct {
	// NumParts indicates the number of name parts specified; always 1 or greater.
	NumParts int

	// Parts are the name components, in reverse order.
	// There are at most 3: object name, schema, catalog/db.
	//
	// Note: Parts has a fixed size so that we avoid a heap allocation for the
	// slice every time we construct an UnresolvedObjectName. It does imply
	// however that Parts does not have a meaningful "length"; its actual length
	// (the number of parts specified) is populated in NumParts above.
	Parts [3]string

	// UnresolvedObjectName cam be annotated with a *TableName.
	AnnotatedNode
}

// UnresolvedObjectName implements TableExpr.
func (*UnresolvedObjectName) tableExpr() {}

// NewUnresolvedObjectName creates an unresolved object name, verifying that it
// is well-formed.
func NewUnresolvedObjectName(
	numParts int, parts [3]string, annotationIdx AnnotationIdx,
) (*UnresolvedObjectName, error) {
	u := &UnresolvedObjectName{
		NumParts:      numParts,
		Parts:         parts,
		AnnotatedNode: AnnotatedNode{AnnIdx: annotationIdx},
	}
	if u.NumParts < 1 {
		return nil, newInvTableNameError(u)
	}

	// Check that all the parts specified are not empty.
	// It's OK if the catalog name is empty.
	// We allow this in e.g. `select * from "".crdb_internal.tables`.
	lastCheck := u.NumParts
	if lastCheck > 2 {
		lastCheck = 2
	}
	for i := 0; i < lastCheck; i++ {
		if len(u.Parts[i]) == 0 {
			return nil, newInvTableNameError(u)
		}
	}
	return u, nil
}

// Resolved returns the resolved name in the annotation for this node (or nil if
// there isn't one).
func (u *UnresolvedObjectName) Resolved(ann *Annotations) *TableName {
	r := u.GetAnnotation(ann)
	if r == nil {
		return nil
	}
	return r.(*TableName)
}

// Format implements the NodeFormatter interface.
func (u *UnresolvedObjectName) Format(ctx *FmtCtx) {
	// If we want to format the corresponding resolved name, look it up in the
	// annotation.
	if ctx.HasFlags(FmtAlwaysQualifyTableNames) || ctx.tableNameFormatter != nil {
		if ctx.tableNameFormatter != nil && ctx.ann == nil {
			// TODO(radu): this is a temporary hack while we transition to using
			// unresolved names everywhere. We will need to revisit and see if we need
			// to switch to (or add) an UnresolvedObjectName formatter.
			tn := u.ToTableName()
			tn.Format(ctx)
			return
		}

		if n := u.Resolved(ctx.ann); n != nil {
			n.Format(ctx)
			return
		}
	}

	for i := u.NumParts; i > 0; i-- {
		// The first part to print is the last item in u.Parts. It is also
		// a potentially restricted name to disambiguate from keywords in
		// the grammar, so print it out as a "Name". Every part after that is
		// necessarily an unrestricted name.
		if i == u.NumParts {
			ctx.FormatNode((*Name)(&u.Parts[i-1]))
		} else {
			ctx.WriteByte('.')
			ctx.FormatNode((*UnrestrictedName)(&u.Parts[i-1]))
		}
	}
}

func (u *UnresolvedObjectName) String() string { return AsString(u) }

// ToTableName converts the unresolved name to a table name.
//
// TODO(radu): the schema and catalog names might not be in the right places; we
// would only figure that out during name resolution. This method is temporary,
// while we change all the code paths to only use TableName after resolution.
func (u *UnresolvedObjectName) ToTableName() TableName {
	return TableName{tblName{
		TableName: Name(u.Parts[0]),
		TableNamePrefix: TableNamePrefix{
			SchemaName:      Name(u.Parts[1]),
			CatalogName:     Name(u.Parts[2]),
			ExplicitSchema:  u.NumParts >= 2,
			ExplicitCatalog: u.NumParts >= 3,
		},
	}}
}

// ToUnresolvedName converts the unresolved object name to the more general
// unresolved name.
func (u *UnresolvedObjectName) ToUnresolvedName() *UnresolvedName {
	return &UnresolvedName{
		NumParts: u.NumParts,
		Parts:    NameParts{u.Parts[0], u.Parts[1], u.Parts[2]},
	}
}
