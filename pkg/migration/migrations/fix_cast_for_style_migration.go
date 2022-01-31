package migrations

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func fixCastForStyleMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job,
) error {
	var visitor sql.FixCastForStyleVisitor
	var tables []descpb.ID
	var semaCtx tree.SemaContext
	semaCtx.IntervalStyleEnabled = true
	semaCtx.DateStyleEnabled = true

	if err := d.CollectionFactory.Txn(
		ctx,
		d.InternalExecutor,
		d.DB,
		func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			descs, err := descriptors.GetAllDescriptors(ctx, txn)
			if err != nil {
				return err
			}

			for _, desc := range descs.OrderedDescriptors() {
				if table, ok := desc.(catalog.TableDescriptor); ok {
					tables = append(tables, table.GetID())
				}
			}
			return nil
		}); err != nil {
		return err
	}

	for _, id := range tables {
		if err := d.CollectionFactory.Txn(
			ctx,
			d.InternalExecutor,
			d.DB,
			func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
				desc, err := descriptors.GetMutableDescriptorByID(ctx, txn, id)
				if err != nil {
					return err
				}

				if err = filterAndFixDesc(ctx, semaCtx, desc, visitor, descriptors, txn); err != nil {
					return err
				}

				return nil

			}); err != nil {
			return err
		}
	}

	return nil
}

func filterAndFixDesc(
	ctx context.Context,
	semaCtx tree.SemaContext,
	mutDesc catalog.MutableDescriptor,
	visitor sql.FixCastForStyleVisitor,
	descriptors *descs.Collection,
	txn *kv.Txn,
) error {

	desc := mutDesc.(*tabledesc.Mutable)
	columns := desc.Columns
	altered := false
	for idx, col := range columns {
		if col.IsComputed() {
			computeExpr := *col.ComputeExpr
			expr, err := parser.ParseExpr(computeExpr)
			if err != nil {
				return err
			}

			visitor = sql.MakeFixCastForStyleVisitor(ctx, &semaCtx, desc.TableDesc(), col.Type, err)
			newExpr, changed := tree.WalkExpr(&visitor, expr)
			if changed {
				altered = true
				s := tree.Serialize(newExpr)
				col.ComputeExpr = &s
				columns[idx] = col
			}
		}
	}

	tIdx := desc.Indexes
	if len(tIdx) > 0 {
		buildDesc := tabledesc.NewBuilder(desc.TableDesc())
		tDesc := buildDesc.BuildImmutableTable()

		for idx, index := range tIdx {
			if index.IsPartial() {
				var typedExpr tree.TypedExpr
				expr, err := parser.ParseExpr(index.Predicate)
				replacedExpr, err := schemaexpr.MakeDummyColForTypeCheck(ctx, tDesc, expr, tree.NewUnqualifiedTableName(tree.Name(tDesc.GetName())))
				//Are all/most partial expressions comparisonExpr?
				if cExpr, ok := replacedExpr.(*tree.ComparisonExpr); ok {
					typedExpr, err = cExpr.Left.TypeCheck(ctx, &semaCtx, types.Any)
				} else {
					typedExpr, err = replacedExpr.TypeCheck(ctx, &semaCtx, types.Any)
				}

				if err == nil {
					typ := typedExpr.ResolvedType()
					visitor = sql.MakeFixCastForStyleVisitor(ctx, &semaCtx, desc.TableDesc(), typ, err)
					newExpr, changed := tree.WalkExpr(&visitor, expr)
					if changed {
						altered = true
						s := tree.Serialize(newExpr)
						index.Predicate = s
						tIdx[idx] = index
						desc.Indexes = tIdx
					}
				}
			}
		}
	}

	if altered {
		batch := txn.NewBatch()
		if err := descriptors.WriteDescToBatch(
			ctx, false, desc, batch); err != nil {
			return err
		}
	}
	return nil
}
