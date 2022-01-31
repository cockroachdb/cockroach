package migrations

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func fixCastForStyleMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job,
) error {
	var semaCtx tree.SemaContext
	semaCtx.IntervalStyleEnabled = true
	semaCtx.DateStyleEnabled = true
	var visitor sql.FixCastForStyleVisitor

	query := `SELECT id, descriptor, crdb_internal_mvcc_timestamp FROM system.descriptor ORDER BY ID ASC`
	rows, err := d.InternalExecutor.QueryIterator(
		ctx, "fix-cast-for-style", nil /* txn */, query,
	)
	if err != nil {
		return err
	}

	alteredDesc, err := filterAndFixDesc(ctx, semaCtx, rows, visitor)
	if err != nil {
		return err
	}

	if len(alteredDesc) > 0 {
		writeAllDesc(ctx, d, alteredDesc)
	}

	return nil
}

func filterAndFixDesc(
	ctx context.Context,
	semaCtx tree.SemaContext,
	rows sqlutil.InternalRows,
	visitor sql.FixCastForStyleVisitor,
) ([]*descpb.TableDescriptor, error) {
	var changedDesc []*descpb.TableDescriptor
	ok, err := rows.Next(ctx)
	if err != nil {
		return nil, err
	}

	for ; ok; ok, err = rows.Next(ctx) {
		datums := rows.Cur()
		_, desc, _, err := unmarshalDescFromDescriptorRow(datums)
		if err != nil {
			return nil, err
		}

		tbDesc := desc.GetTable()
		if tbDesc != nil {

			columns := tbDesc.Columns
			altered := false
			for idx, col := range columns {
				if col.IsComputed() {
					computeExpr := *col.ComputeExpr
					expr, err := parser.ParseExpr(computeExpr)
					if err != nil {
						return nil, err
					}

					visitor = sql.MakeFixCastForStyleVisitor(ctx, &semaCtx, tbDesc, col.Type, err)
					newExpr, changed := tree.WalkExpr(&visitor, expr)
					if changed {
						altered = true
						s := tree.Serialize(newExpr)
						col.ComputeExpr = &s
						columns[idx] = col
					}
				}
			}

			tIdx := tbDesc.Indexes
			if len(tIdx) > 0 {
				desc := tabledesc.NewBuilder(tbDesc)
				tDesc := desc.BuildImmutableTable()

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
							visitor = sql.MakeFixCastForStyleVisitor(ctx, &semaCtx, tbDesc, typ, err)
							newExpr, changed := tree.WalkExpr(&visitor, expr)
							if changed {
								altered = true
								s := tree.Serialize(newExpr)
								index.Predicate = s
								tIdx[idx] = index
								tbDesc.Indexes = tIdx
							}
						}
					}
				}
			}

			if altered {
				changedDesc = append(changedDesc, tbDesc)
			}
		}
	}
	return changedDesc, nil
}

func writeAllDesc(
	ctx context.Context, d migration.TenantDeps, tDesc []*descpb.TableDescriptor,
) error {
	return d.CollectionFactory.Txn(ctx, d.InternalExecutor, d.DB, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
		batch := txn.NewBatch()

		for _, tDesc := range tDesc {
			desc, err := descriptors.GetMutableDescriptorByID(ctx, tDesc.GetID(), txn)
			if err != nil {
				return err
			}
			mutDesc := desc.(*tabledesc.Mutable)
			mutDesc.TableDescriptor = *tDesc
			err = descriptors.WriteDescToBatch(ctx, false, mutDesc, batch)
			if err != nil {
				return err
			}
		}
		return txn.Run(ctx, batch)
	})
}
