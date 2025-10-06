// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package redact contains utilities to redact sensitive fields from
// descriptors.
package redact

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	plpgsqlparser "github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// Redact will redact the descriptor in place.
func Redact(descProto *descpb.Descriptor) []error {
	switch d := descProto.Union.(type) {
	case *descpb.Descriptor_Table:
		return redactTableDescriptor(d.Table)
	case *descpb.Descriptor_Type:
		redactTypeDescriptor(d.Type)
	case *descpb.Descriptor_Function:
		return redactFunctionDescriptor(d.Function)
	}
	return nil
}

func redactTableDescriptor(d *descpb.TableDescriptor) (errs []error) {
	handleErr := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	if d.ViewQuery != "" {
		handleErr(errors.Wrap(redactQuery(&d.ViewQuery), "view query"))
	}
	if d.CreateQuery != "" {
		handleErr(errors.Wrap(redactQuery(&d.CreateQuery), "create query"))
	}
	handleErr(redactIndex(&d.PrimaryIndex))
	for i := range d.Indexes {
		idx := &d.Indexes[i]
		handleErr(errors.Wrapf(redactIndex(idx), "index #%d", idx.ID))
	}
	for i := range d.Columns {
		col := &d.Columns[i]
		for _, err := range redactColumn(col) {
			handleErr(errors.Wrapf(err, "column #%d", col.ID))
		}
	}
	for i := range d.Checks {
		chk := d.Checks[i]
		handleErr(errors.Wrapf(redactCheckConstraints(chk), "constraint #%d", chk.ConstraintID))
	}
	for i := range d.UniqueWithoutIndexConstraints {
		uwi := &d.UniqueWithoutIndexConstraints[i]
		handleErr(errors.Wrapf(redactUniqueWithoutIndexConstraint(uwi), "constraint #%d", uwi.ConstraintID))
	}
	for _, m := range d.Mutations {
		if idx := m.GetIndex(); idx != nil {
			handleErr(errors.Wrapf(redactIndex(idx), "index #%d", idx.ID))
		} else if col := m.GetColumn(); col != nil {
			for _, err := range redactColumn(col) {
				handleErr(errors.Wrapf(err, "column #%d", col.ID))
			}
		} else if ctu := m.GetConstraint(); ctu != nil {
			switch ctu.ConstraintType {
			case descpb.ConstraintToUpdate_CHECK:
				chk := &ctu.Check
				handleErr(errors.Wrapf(redactCheckConstraints(chk), "constraint #%d", chk.ConstraintID))
			case descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX:
				uwi := &ctu.UniqueWithoutIndexConstraint
				handleErr(errors.Wrapf(redactUniqueWithoutIndexConstraint(uwi), "constraint #%d", uwi.ConstraintID))
			}
		}
	}
	if scs := d.DeclarativeSchemaChangerState; scs != nil {
		for i := range scs.RelevantStatements {
			stmt := &scs.RelevantStatements[i]
			stmt.Statement.Statement = stmt.Statement.RedactedStatement.StripMarkers()
		}
		for i := range scs.Targets {
			t := &scs.Targets[i]
			handleErr(errors.Wrapf(redactElement(t.Element()), "element #%d", i))
		}
	}
	return errs
}

func redactQuery(sql *string) error {
	q, err := parser.ParseOne(*sql)
	if err != nil {
		*sql = "_"
		return err
	}
	fmtCtx := tree.NewFmtCtx(tree.FmtHideConstants)
	q.AST.Format(fmtCtx)
	*sql = fmtCtx.String()
	return nil
}

func redactIndex(idx *descpb.IndexDescriptor) error {
	redactPartitioning(&idx.Partitioning)
	return errors.Wrap(redactExprStr(&idx.Predicate), "partial predicate")
}

func redactColumn(col *descpb.ColumnDescriptor) (errs []error) {
	handleErr := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	if ce := col.ComputeExpr; ce != nil {
		handleErr(errors.Wrap(redactExprStr(ce), "compute expr"))
	}
	if de := col.DefaultExpr; de != nil {
		handleErr(errors.Wrap(redactExprStr(de), "default expr"))
	}
	if ue := col.OnUpdateExpr; ue != nil {
		handleErr(errors.Wrap(redactExprStr(ue), "on-update expr"))
	}
	return errs
}

func redactCheckConstraints(chk *descpb.TableDescriptor_CheckConstraint) error {
	return redactExprStr(&chk.Expr)
}

func redactUniqueWithoutIndexConstraint(uwi *descpb.UniqueWithoutIndexConstraint) error {
	return redactExprStr(&uwi.Predicate)
}

func redactTypeDescriptor(d *descpb.TypeDescriptor) {
	for i := range d.EnumMembers {
		e := &d.EnumMembers[i]
		e.LogicalRepresentation = "_"
		e.PhysicalRepresentation = []byte("_")
	}
}

// redactElement redacts literals which may contain PII from elements.
func redactElement(element scpb.Element) error {
	switch e := element.(type) {
	case *scpb.EnumTypeValue:
		e.LogicalRepresentation = "_"
		e.PhysicalRepresentation = []byte("_")
	case *scpb.IndexPartitioning:
		redactPartitioning(&e.PartitioningDescriptor)
	case *scpb.SecondaryIndexPartial:
		return redactExpr(&e.Expression.Expr)
	case *scpb.CheckConstraint:
		return redactExpr(&e.Expression.Expr)
	case *scpb.ColumnDefaultExpression:
		return redactExpr(&e.Expression.Expr)
	case *scpb.ColumnOnUpdateExpression:
		return redactExpr(&e.Expression.Expr)
	case *scpb.ColumnType:
		if e.ComputeExpr != nil {
			return redactExpr(&e.ComputeExpr.Expr)
		}
	case *scpb.ColumnComputeExpression:
		return redactExpr(&e.Expression.Expr)
	case *scpb.FunctionBody:
		return redactFunctionBodyStr(e.Lang.Lang, &e.Body)
	}
	return nil
}

func redactPartitioning(p *catpb.PartitioningDescriptor) {
	for i := range p.List {
		l := &p.List[i]
		for j := range l.Values {
			l.Values[j] = []byte("_")
		}
		redactPartitioning(&l.Subpartitioning)
	}
	for i := range p.Range {
		r := &p.Range[i]
		r.FromInclusive = []byte("_")
		r.ToExclusive = []byte("_")
	}
}

func redactExpr(expr *catpb.Expression) error {
	str := string(*expr)
	err := redactExprStr(&str)
	*expr = catpb.Expression(str)
	return err
}

func redactExprStr(expr *string) error {
	if *expr == "" {
		return nil
	}
	parsedExpr, err := parser.ParseExpr(*expr)
	if err != nil {
		*expr = "_"
		return err
	}
	fmtCtx := tree.NewFmtCtx(tree.FmtHideConstants)
	parsedExpr.Format(fmtCtx)
	*expr = fmtCtx.String()
	return nil
}

func redactFunctionDescriptor(desc *descpb.FunctionDescriptor) (errs []error) {
	handleErr := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}

	if err := redactFunctionBodyStr(desc.Lang, &desc.FunctionBody); err != nil {
		return []error{err}
	}
	if scs := desc.DeclarativeSchemaChangerState; scs != nil {
		for i := range scs.RelevantStatements {
			stmt := &scs.RelevantStatements[i]
			stmt.Statement.Statement = stmt.Statement.RedactedStatement.StripMarkers()
		}
		for i := range scs.Targets {
			t := &scs.Targets[i]
			handleErr(errors.Wrapf(redactElement(t.Element()), "element #%d", i))
		}
	}
	return nil
}

func redactFunctionBodyStr(lang catpb.Function_Language, body *string) error {
	fmtCtx := tree.NewFmtCtx(tree.FmtHideConstants)
	switch lang {
	case catpb.Function_SQL:
		stmts, err := parser.Parse(*body)
		if err != nil {
			return err
		}
		for i, stmt := range stmts {
			if i > 0 {
				fmtCtx.WriteString(" ")
			}
			fmtCtx.FormatNode(stmt.AST)
			fmtCtx.WriteString(";")
		}
	case catpb.Function_PLPGSQL:
		stmt, err := plpgsqlparser.Parse(*body)
		if err != nil {
			return err
		}
		fmtCtx.FormatNode(stmt.AST)
	default:
		return errors.AssertionFailedf("unexpected function language %s", lang)
	}
	*body = fmtCtx.String()
	return nil
}
