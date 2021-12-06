package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/errors"
)

func (sc *SchemaChanger) Merge(
	ctx context.Context,
	codec keys.SQLCodec,
	table catalog.TableDescriptor,
	source catalog.Index,
	destination catalog.Index,
) error {
	// TODO(rhu): Do we need to get the timestamp and use a fix timestamp?
	mergeTimestamp := sc.clock.Now()

	sourceSpan := table.IndexSpan(codec, source.GetID())
	destSpan := table.IndexSpan(codec, destination.GetID())

	const pageSize = 1000

	return sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetFixedTimestamp(ctx, mergeTimestamp); err != nil {
			return err
		}

		return txn.Iterate(ctx, sourceSpan.Key, sourceSpan.EndKey, pageSize, func(kvs []kv.KeyValue) error {
			destKeys := make([]roachpb.Key, len(kvs))

			prefixLen := len(sourceSpan.Key)

			// For now just grab all of the destination KVs and merge the corresponding entries.
			for i := range kvs {
				sourceKV := &kvs[i]

				if len(sourceKV.Key) < prefixLen {
					return errors.Errorf("Key for index entry %v does not start with prefix %v", sourceKV, sourceSpan.Key)
				}

				destKey := make([]byte, len(destSpan.Key))
				copy(destKey, destSpan.Key)
				destKey = append(destKey, sourceKV.Key[prefixLen:]...)
				destKeys[i] = destKey
			}

			wb := txn.NewBatch()
			for i := range kvs {
				mergedEntry, deleted, err := mergeEntry(&kvs[i], destKeys[i])
				if err != nil {
					return err
				}

				if deleted {
					wb.Del(mergedEntry.Key)
				} else {
					wb.Put(mergedEntry.Key, mergedEntry.Value)
				}
			}

			return txn.Run(ctx, wb)
		})
	})
}

func mergeEntry(sourceKV *kv.KeyValue, destKey roachpb.Key) (*kv.KeyValue, bool, error) {
	var destTagAndData []byte
	var deleted bool

	tempWrapper, err := rowenc.DecodeWrapper(sourceKV.Value)
	if err != nil {
		return nil, false, err
	}

	if tempWrapper.Deleted {
		deleted = true
	} else {
		destTagAndData = tempWrapper.Value
	}

	value := &roachpb.Value{}
	value.SetTagAndData(destTagAndData)

	return &kv.KeyValue{
		Key:   destKey,
		Value: value,
	}, deleted, nil
}
