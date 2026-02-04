package databasestorageparam

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
)

type Setter struct {
	DatabaseDesc *dbdesc.Mutable
}

func NewSetter(databaseDesc *dbdesc.Mutable) *Setter {
	return &Setter{
		DatabaseDesc: databaseDesc,
	}
}

func (po *Setter) RunPostChecks() error {
	return nil
}

// IsNewObject implements the Setter interface.
func (po *Setter) IsNewObject() bool {
	return po.DatabaseDesc.GetState() == descpb.DescriptorState_ADD
}

type databaseParam struct {
	onSet   func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error
	onReset func(ctx context.Context, po *Setter, evalCtx *eval.Context, key string) error
}

var databaseParams = map[string]databaseParam{

	`max_row_size_log`: {
		onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
			if datum == tree.DNull {
				po.DatabaseDesc.MaxRowSizeLog = nil
				return nil
			}
			sizeStr, err := paramparse.DatumAsString(ctx, evalCtx, key, datum)
			if err != nil {
				return err
			}

			sizeBytes, err := humanizeutil.ParseBytes(sizeStr)
			if err != nil {
				return pgerror.Newf(
					pgcode.InvalidParameterValue,
					"invalid value for parameter %q, value should be a valid byte size string like '1KiB', '2MiB'", key)
			}

			err = rowinfra.IntInRange(rowinfra.MaxRowSizeFloor, rowinfra.MaxRowSizeCeil)(sizeBytes)
			if err != nil {
				return pgerror.Newf(
					pgcode.InvalidParameterValue,
					"parameter %q value is out of range, expected value in range [%s, %s]", key, humanizeutil.IBytes(rowinfra.MaxRowSizeFloor), humanizeutil.IBytes(rowinfra.MaxRowSizeCeil))
			}
			uint32Val := uint32(sizeBytes)
			po.DatabaseDesc.MaxRowSizeLog = &uint32Val

			return nil
		},
		onReset: func(ctx context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			po.DatabaseDesc.MaxRowSizeLog = nil
			return nil
		},
	},
	`max_row_size_err`: {
		onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
			if datum == tree.DNull {
				po.DatabaseDesc.MaxRowSizeErr = nil
				return nil
			}

			sizeStr, err := paramparse.DatumAsString(ctx, evalCtx, key, datum)
			if err != nil {
				return err
			}

			sizeBytes, err := humanizeutil.ParseBytes(sizeStr)
			if err != nil {
				return pgerror.Newf(
					pgcode.InvalidParameterValue,
					"invalid value for parameter %q, value should be a valid byte size string like '1KiB', '2MiB'", key)
			}

			err = rowinfra.IntInRange(rowinfra.MaxRowSizeFloor, rowinfra.MaxRowSizeCeil)(sizeBytes)
			if err != nil {
				return pgerror.Newf(
					pgcode.InvalidParameterValue,
					"parameter %q value is out of range, expected value in range [%s, %s]", key, humanizeutil.IBytes(rowinfra.MaxRowSizeFloor), humanizeutil.IBytes(rowinfra.MaxRowSizeCeil))
			}

			uint32Val := uint32(sizeBytes)
			po.DatabaseDesc.MaxRowSizeErr = &uint32Val

			return nil
		},
		onReset: func(ctx context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			po.DatabaseDesc.MaxRowSizeErr = nil
			return nil
		},
	},
}

// Set implements the Setter interface.
func (po *Setter) Set(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
	key string,
	datum tree.Datum,
) error {
	//TODO (KB): add this for Alter statements
	telemetry.Inc(sqltelemetry.SetDatabaseStorageParameter(key))

	if p, ok := databaseParams[key]; ok {
		return p.onSet(ctx, po, semaCtx, evalCtx, key, datum)
	}
	return pgerror.Newf(pgcode.InvalidParameterValue, "invalid storage parameter %q", key)
}

// Reset implements the Setter interface.
func (po *Setter) Reset(ctx context.Context, evalCtx *eval.Context, key string) error {
	telemetry.Inc(sqltelemetry.ResetDatabaseStorageParameter(key))

	if p, ok := databaseParams[key]; ok {
		return p.onReset(ctx, po, evalCtx, key)
	}
	return pgerror.Newf(pgcode.InvalidParameterValue, "invalid storage parameter %q", key)
}
