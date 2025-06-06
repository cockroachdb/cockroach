package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// unexpectedKeyCheckOperation implements the checkOperation interface. It is a
// scrub check for the KV's integrity. This operation will detect:
//  1. Extraneous keys that fall between or outside index spans.
//  2. Extraneous column families (unexpected family IDs).
type unexpectedKeyCheckOperation struct {
	tableName *tree.TableName
	tableDesc catalog.TableDescriptor
	asOf      hlc.Timestamp
	run       unexpectedKeyCheckRun
}

// unexpectedKeyCheckRun contains the run-time state for unexpectedKeyCheckOperation.
type unexpectedKeyCheckRun struct {
	started  bool
	rowIndex int

	// Collected during Start()
	errorRows []tree.Datum
	keys      []roachpb.Key
	familyIDs []uint32
}

func newUnexpectedKeyCheckOperation(
	tableName *tree.TableName,
	tableDesc catalog.TableDescriptor,
	asOf hlc.Timestamp,
) *unexpectedKeyCheckOperation {
	return &unexpectedKeyCheckOperation{
		tableName: tableName,
		tableDesc: tableDesc,
		asOf:      asOf,
	}
}

func (o *unexpectedKeyCheckOperation) Start(params runParams) error {
	ctx := params.ctx
	codec := params.ExecCfg().Codec
	db := params.ExecCfg().DB

	// Build expected column family IDs
	expectedFamilies := make(map[uint32]struct{})
	for _, fam := range o.tableDesc.GetFamilies() {
		expectedFamilies[uint32(fam.ID)] = struct{}{}
	}

	indexSpans := o.tableDesc.AllIndexSpans(codec)
	fullTableSpan := o.tableDesc.TableSpan(codec)

	txn := db.NewTxn(ctx, "unexpected-key-check")

	kvs, err := txn.Scan(ctx, fullTableSpan.Key, fullTableSpan.EndKey, 0)
	if err != nil {
		return err
	}

	var errorType tree.Datum
	var errorFound bool

	for _, kv := range kvs {
		familyID, err := keys.DecodeFamilyKey(kv.Key)
		if err != nil {
			return err
		}
		errorFound = false

		if !isFamilyExpected(familyID, expectedFamilies) {
			errorType = tree.NewDString(scrub.UnexpectedColumnFamilyError)
			errorFound = true
		}
		if !keyInIndexSpans(kv.Key, indexSpans) {
			errorType = tree.NewDString(scrub.UnexpectedKeyOutsideIndexSpanError)
			errorFound = true
		}
		if errorFound {
			o.run.errorRows = append(o.run.errorRows, errorType)
			o.run.keys = append(o.run.keys, kv.Key)
			o.run.familyIDs = append(o.run.familyIDs, familyID)
		}
	}

	o.run.started = true
	return nil
}

// isFamilyExpected returns true if the given familyID
// is present in the expected set of family IDs.
// The expected map acts as a set of valid family IDs.
func isFamilyExpected(familyID uint32, expected map[uint32]struct{}) bool {
	_, ok := expected[familyID]
	return ok
}

// keyInIndexSpans checks whether the provided key
// falls within any of the given spans. It returns true
// if the key is contained by at least one span,
// and false otherwise.
func keyInIndexSpans(key roachpb.Key, spans []roachpb.Span) bool {
	for _, span := range spans {
		if span.ContainsKey(key) {
			return true
		}
	}
	return false
}

func (o *unexpectedKeyCheckOperation) Next(params runParams) (tree.Datums, error) {
	if o.run.rowIndex >= len(o.run.errorRows) {
		return nil, nil
	}
	idx := o.run.rowIndex
	o.run.rowIndex++

	errorType := o.run.errorRows[idx]
	key := o.run.keys[idx]
	familyID := o.run.familyIDs[idx]

	timestamp, err := tree.MakeDTimestamp(params.extendedEvalCtx.GetStmtTimestamp(), time.Nanosecond)
	if err != nil {
		return nil, err
	}

	details := make(map[string]interface{})
	rowDetails := make(map[string]interface{})

	details["unexpected_key"] = key.String()
	details["family_id"] = int64(familyID)

	rowDetails["key"] = key.String()
	details["row_data"] = rowDetails

	detailsJSON, err := tree.MakeDJSON(details)
	if err != nil {
		return nil, err
	}

	return tree.Datums{
		tree.DNull, /* job_uuid */
		errorType,
		tree.NewDString(o.tableName.Catalog()),
		tree.NewDString(o.tableName.Table()),
		tree.NewDString(key.String()),
		timestamp,
		tree.DBoolFalse,
		detailsJSON,
	}, nil
}

func (o *unexpectedKeyCheckOperation) Started() bool {
	return o.run.started
}

func (o *unexpectedKeyCheckOperation) Done(_ context.Context) bool {
	return o.run.rowIndex >= len(o.run.errorRows)
}

func (o *unexpectedKeyCheckOperation) Close(_ context.Context) {
	o.run.errorRows = nil
}
