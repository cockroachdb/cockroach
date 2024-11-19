// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package row

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// ConvertBatchError attempts to map a key-value error generated during a
// key-value batch operating over the specified table to a user friendly SQL
// error.
func ConvertBatchError(ctx context.Context, tableDesc catalog.TableDescriptor, b *kv.Batch) error {
	origPErr := b.MustPErr()
	switch v := origPErr.GetDetail().(type) {
	case *kvpb.MinTimestampBoundUnsatisfiableError:
		return pgerror.WithCandidateCode(
			origPErr.GoError(),
			pgcode.UnsatisfiableBoundedStaleness,
		)

	case *kvpb.ConditionFailedError:
		if origPErr.Index == nil {
			break
		}
		j := origPErr.Index.Index
		_, kv, err := b.GetResult(int(j))
		if err != nil {
			return err
		}
		return NewUniquenessConstraintViolationError(ctx, tableDesc, kv.Key, v.ActualValue)

	case *kvpb.WriteIntentError:
		key := v.Locks[0].Key
		decodeKeyFn := func() (tableName string, indexName string, colNames []string, values []string, err error) {
			codec, index, err := decodeKeyCodecAndIndex(tableDesc, key)
			if err != nil {
				return "", "", nil, nil, err
			}
			var spec fetchpb.IndexFetchSpec
			if err := rowenc.InitIndexFetchSpec(&spec, codec, tableDesc, index, nil /* fetchColumnIDs */); err != nil {
				return "", "", nil, nil, err
			}

			colNames, values, err = decodeKeyValsUsingSpec(&spec, key)
			return spec.TableName, spec.IndexName, colNames, values, err
		}
		return newLockNotAvailableError(v.Reason, decodeKeyFn)
	}
	return origPErr.GoError()
}

// ConvertFetchError attempts to map a key-value error generated during a
// key-value fetch to a user friendly SQL error.
func ConvertFetchError(spec *fetchpb.IndexFetchSpec, err error) error {
	var errs struct {
		wi *kvpb.WriteIntentError
		bs *kvpb.MinTimestampBoundUnsatisfiableError
	}
	switch {
	case errors.As(err, &errs.wi):
		key := errs.wi.Locks[0].Key
		decodeKeyFn := func() (tableName string, indexName string, colNames []string, values []string, err error) {
			colNames, values, err = decodeKeyValsUsingSpec(spec, key)
			return spec.TableName, spec.IndexName, colNames, values, err
		}
		return newLockNotAvailableError(errs.wi.Reason, decodeKeyFn)

	case errors.As(err, &errs.bs):
		return pgerror.WithCandidateCode(
			err,
			pgcode.UnsatisfiableBoundedStaleness,
		)
	}
	return err
}

// NewUniquenessConstraintViolationError creates an error that represents a
// violation of a UNIQUE constraint.
func NewUniquenessConstraintViolationError(
	ctx context.Context, tableDesc catalog.TableDescriptor, key roachpb.Key, value *roachpb.Value,
) error {
	index, names, values, err := DecodeRowInfo(ctx, tableDesc, key, value, false)
	if err != nil {
		return pgerror.Wrap(err, pgcode.UniqueViolation,
			"duplicate key value got decoding error")
	}
	// Resolve the table index descriptor name.
	indexName, err := catalog.FindTargetIndexNameByID(tableDesc, index.GetID())
	if err != nil {
		log.Warningf(ctx,
			"unable to find index by ID for NewUniquenessConstraintViolationError: %d",
			index.GetID())
		indexName = index.GetName()
	}
	// Exclude implicit partitioning columns and hash sharded index columns from
	// the error message.
	skipCols := index.ExplicitColumnStartIdx()
	return errors.WithDetail(
		pgerror.WithConstraintName(pgerror.Newf(pgcode.UniqueViolation,
			"duplicate key value violates unique constraint %q", indexName,
		), indexName),
		fmt.Sprintf(
			"Key (%s)=(%s) already exists.",
			strings.Join(names[skipCols:], ", "),
			strings.Join(values[skipCols:], ", "),
		),
	)
}

// decodeKeyValsUsingSpec decodes an index key and returns the key column names
// and values.
func decodeKeyValsUsingSpec(
	spec *fetchpb.IndexFetchSpec, key roachpb.Key,
) (colNames []string, values []string, err error) {
	// We want the key columns without the suffix columns.
	keyCols := spec.KeyColumns()
	keyVals := make([]rowenc.EncDatum, len(keyCols))
	if len(key) < int(spec.KeyPrefixLength) {
		return nil, nil, errors.AssertionFailedf("invalid table key")
	}
	if _, _, err := rowenc.DecodeKeyValsUsingSpec(keyCols, key[spec.KeyPrefixLength:], keyVals); err != nil {
		return nil, nil, err
	}
	colNames = make([]string, len(keyCols))
	values = make([]string, len(keyCols))
	for i := range keyCols {
		colNames[i] = keyCols[i].Name
		values[i] = keyVals[i].String(keyCols[i].Type)
	}
	return colNames, values, nil
}

// newLockNotAvailableError creates an error that represents an inability to
// acquire a lock. It uses an IndexFetchSpec for the corresponding index (the
// fetch columns in the spec are not used).
func newLockNotAvailableError(
	reason kvpb.WriteIntentError_Reason,
	decodeKeyFn func() (tableName string, indexName string, colNames []string, values []string, err error),
) error {
	baseMsg := "could not obtain lock on row"
	if reason == kvpb.WriteIntentError_REASON_LOCK_TIMEOUT {
		baseMsg = "canceling statement due to lock timeout on row"
	}
	tableName, indexName, colNames, values, err := decodeKeyFn()
	if err != nil {
		return pgerror.Wrapf(err, pgcode.LockNotAvailable, "%s: got decoding error", baseMsg)
	}
	return pgerror.Newf(pgcode.LockNotAvailable,
		"%s (%s)=(%s) in %s@%s",
		baseMsg,
		strings.Join(colNames, ","),
		strings.Join(values, ","),
		tableName,
		indexName,
	)
}

// decodeKeyCodecAndIndex extracts the codec and index from a key (for a
// particular table).
func decodeKeyCodecAndIndex(
	tableDesc catalog.TableDescriptor, key roachpb.Key,
) (keys.SQLCodec, catalog.Index, error) {
	_, tenantID, err := keys.DecodeTenantPrefix(key)
	if err != nil {
		return keys.SQLCodec{}, nil, err
	}
	codec := keys.MakeSQLCodec(tenantID)
	indexID, _, err := rowenc.DecodeIndexKeyPrefix(codec, tableDesc.GetID(), key)
	if err != nil {
		return keys.SQLCodec{}, nil, err
	}
	index, err := catalog.MustFindIndexByID(tableDesc, indexID)
	if err != nil {
		return keys.SQLCodec{}, nil, err
	}

	return keys.MakeSQLCodec(tenantID), index, nil
}

// DecodeRowInfo takes a table descriptor, a key, and an optional value and
// returns information about the corresponding SQL row. If successful, the index
// and corresponding column names and values to the provided KV are returned.
func DecodeRowInfo(
	ctx context.Context,
	tableDesc catalog.TableDescriptor,
	key roachpb.Key,
	value *roachpb.Value,
	allColumns bool,
) (_ catalog.Index, columnNames []string, columnValues []string, _ error) {
	codec, index, err := decodeKeyCodecAndIndex(tableDesc, key)
	if err != nil {
		return nil, nil, nil, err
	}
	var rf Fetcher

	var colIDs []descpb.ColumnID
	if !allColumns {
		colIDs = make([]descpb.ColumnID, index.NumKeyColumns())
		for i := 0; i < index.NumKeyColumns(); i++ {
			colIDs[i] = index.GetKeyColumnID(i)
		}
	} else if index.Primary() {
		publicColumns := tableDesc.PublicColumns()
		colIDs = make([]descpb.ColumnID, len(publicColumns))
		for i, col := range publicColumns {
			colIDs[i] = col.GetID()
		}
	} else {
		maxNumIDs := index.NumKeyColumns() + index.NumKeySuffixColumns() + index.NumSecondaryStoredColumns()
		colIDs = make([]descpb.ColumnID, 0, maxNumIDs)
		for i := 0; i < index.NumKeyColumns(); i++ {
			colIDs = append(colIDs, index.GetKeyColumnID(i))
		}
		for i := 0; i < index.NumKeySuffixColumns(); i++ {
			colIDs = append(colIDs, index.GetKeySuffixColumnID(i))
		}
		for i := 0; i < index.NumSecondaryStoredColumns(); i++ {
			colIDs = append(colIDs, index.GetStoredColumnID(i))
		}
	}
	cols := make([]catalog.Column, len(colIDs))
	for i, colID := range colIDs {
		col, err := catalog.MustFindColumnByID(tableDesc, colID)
		if err != nil {
			return nil, nil, nil, err
		}
		cols[i] = col
	}
	var spec fetchpb.IndexFetchSpec
	if err := rowenc.InitIndexFetchSpec(&spec, codec, tableDesc, index, colIDs); err != nil {
		return nil, nil, nil, err
	}
	rf.IgnoreUnexpectedNulls = true
	if err := rf.Init(
		ctx,
		FetcherInitArgs{
			WillUseKVProvider: true,
			Alloc:             &tree.DatumAlloc{},
			Spec:              &spec,
		},
	); err != nil {
		return nil, nil, nil, err
	}
	f := KVProvider{KVs: []roachpb.KeyValue{{Key: key}}}
	if value != nil {
		f.KVs[0].Value = *value
	}
	// Use the Fetcher to decode the single kv pair above by passing in
	// this KVProvider implementation, which doesn't actually hit KV.
	if err := rf.ConsumeKVProvider(ctx, &f); err != nil {
		return nil, nil, nil, err
	}
	datums, err := rf.NextRowDecoded(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	names := make([]string, len(cols))
	values := make([]string, len(cols))
	for i := range cols {
		if cols[i].IsExpressionIndexColumn() {
			names[i] = cols[i].GetComputeExpr()
		} else {
			names[i] = cols[i].GetName()
		}
		if datums[i] == tree.DNull {
			continue
		}
		values[i] = datums[i].String()
	}
	return index, names, values, nil
}

// CheckFailed returns error message when a check constraint is violated.
func CheckFailed(
	ctx context.Context,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	sessionData *sessiondata.SessionData,
	tabDesc catalog.TableDescriptor,
	check catalog.CheckConstraint,
) error {
	// Failed to satisfy CHECK constraint, so unwrap the serialized
	// check expression to display to the user.
	expr, err := schemaexpr.FormatExprForDisplay(
		ctx, tabDesc, check.GetExpr(), evalCtx, semaCtx, sessionData, tree.FmtParsable,
	)
	if err != nil {
		// If we ran into an error trying to read the check constraint, wrap it
		// and return.
		return pgerror.WithConstraintName(errors.Wrapf(err, "failed to satisfy CHECK constraint (%s)", check.GetExpr()), check.GetName())
	}
	return pgerror.WithConstraintName(pgerror.Newf(
		pgcode.CheckViolation, "failed to satisfy CHECK constraint (%s)", expr,
	), check.GetName())
}
