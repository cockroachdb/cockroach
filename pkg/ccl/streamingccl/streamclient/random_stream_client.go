// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamclient

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"math/rand"
	"time"
)

// client is a mock stream client.
type randomStreamClient struct{
	r *rand.Rand
	baseDesc *tabledesc.Mutable
}

var _ Client = &randomStreamClient{}

func newRandomStreamClient() randomStreamClient {
	fooTable, err := sql.CreateTestTableDescriptor(
		context.Background(),
		50 /* defaultdb */,
		52 /* first table ID */,
		"CREATE TABLE foo (a INT PRIMARY KEY, b INT)",
		systemschema.JobsTable.Privileges,
	)
	if err != nil {
		panic(err)
	}
	return randomStreamClient{
		r: rand.New(rand.NewSource(42)),
		baseDesc: fooTable,
	}
}

// GetTopology implements the Client interface.
func (m *randomStreamClient) GetTopology(address streamingccl.StreamAddress) (streamingccl.Topology, error) {
	return streamingccl.Topology{
		Partitions: []streamingccl.PartitionAddress{streamingccl.PartitionAddress(address)},
	}, nil
}

// ConsumePartition implements the Client interface.
func (m *randomStreamClient) ConsumePartition(
	address streamingccl.PartitionAddress, startTime time.Time,
) (chan streamingccl.Event, error) {
	eventCh := make(chan streamingccl.Event)
	lastResolvedTime := timeutil.Now()
	tableKey := func(i uint32) roachpb.Key {
		return keys.SystemSQLCodec.TablePrefix(i)
	}
	allSpan := roachpb.Span{Key: tableKey(52), EndKey: tableKey(52).PrefixEnd()}

	go func() {
		kvInterval := time.Second
		kvTimer := timeutil.NewTimer()
		kvTimer.Reset(kvInterval)
		defer kvTimer.Stop()

		resolvedInterval := time.Second * 10
		resolvedTimer := timeutil.NewTimer()
		resolvedTimer.Reset(resolvedInterval)
		defer resolvedTimer.Stop()

		for {
			select {
			case <-kvTimer.C:
				eventCh <- streamingccl.MakeKVEvent(m.makeRandomKey(lastResolvedTime))
				kvTimer.Reset(kvInterval)
			case <-resolvedTimer.C:
				resolvedTime := timeutil.Now()
				hlcResolvedTime := hlc.Timestamp{WallTime: resolvedTime.UnixNano()}
				lastResolvedTime = resolvedTime
				eventCh <- streamingccl.MakeCheckpointEvent(allSpan, hlcResolvedTime)
				resolvedTimer.Reset(resolvedInterval)
			}
		}
	}()

	return eventCh, nil
}

func (m *randomStreamClient) makeRandomKey(minTs time.Time) roachpb.KeyValue {
	// Generate a timestamp between minTs and now().
	newTimestamp := rand.Intn(int(timeutil.Now().UnixNano()) - int(minTs.UnixNano())) + int(minTs.UnixNano())

	// Generate a random a value.
	// Generate a random b value.
	aVal := m.r.Intn(100)
	bVal := m.r.Intn(100)
	datums := tree.Datums{tree.NewDInt(tree.DInt(aVal)), tree.NewDInt(tree.DInt(bVal))}

	indexDesc := m.baseDesc.PrimaryIndex
	tableDesc := m.baseDesc

	indexColIDs := indexDesc.ColumnIDs
	if indexDesc.ID != tableDesc.GetPrimaryIndexID() && !indexDesc.Unique {
		indexColIDs = append(indexColIDs, indexDesc.ExtraColumnIDs...)
	}
	// Create a column id to row index map. In this case, each column ID
	// just maps to the i'th ordinal.
	var colMap catalog.TableColMap
	for i, id := range indexColIDs {
		colMap.Set(id, i)
	}
	// Finally, encode the index key using the provided datums.
	keyPrefix := rowenc.MakeIndexKeyPrefix(keys.SystemSQLCodec, tableDesc, indexDesc.ID)
	key, _, err := rowenc.EncodePartialIndexKey(tableDesc, &indexDesc, len(datums), colMap, datums, keyPrefix)
	if err != nil {
		panic(err)
	}

	// Generate a k/v that has a different value that violates the
	// constraint.
	values := []tree.Datum{tree.NewDInt(10)}
	// Encode the column value.
	valueBuf, err := rowenc.EncodeTableValue(
		[]byte(nil), tableDesc.Columns[1].ID, values[1], []byte(nil))
	if err != nil {
		panic(err)
	}
	// Construct the tuple for the family value.
	var value roachpb.Value
	value.SetTuple(valueBuf)
	value.Timestamp = hlc.Timestamp{WallTime: int64(newTimestamp)}

	return roachpb.KeyValue{
		Key:   key,
		Value: value,
	}
}
