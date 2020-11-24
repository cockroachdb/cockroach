// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalogkv

import (
	"context"
	"encoding/hex"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestUnwrapValidation uses testdata to find issues validating descriptors.
// The test is driven by serialized testdata. The expected testdata directories
// will hold a file "descriptors.csv" which is a csv of id,descriptor where
// descriptor is hex encoded.
func TestUnwrapValidation(t *testing.T) {
	testdata := testutils.TestDataPath("testdata", "unwrap_validation")
	const descriptorsCSVFilename = "descriptors.csv"
	dirs, err := ioutil.ReadDir(testdata)
	require.NoError(t, err)
	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}
		dp := filepath.Join(testdata, dir.Name(), descriptorsCSVFilename)
		if _, err := os.Stat(dp); errors.Is(err, os.ErrNotExist) {
			continue
		}
		t.Run(dir.Name(), func(t *testing.T) {
			unwrapValidationTest(t, dp)
		})
	}
}

func unwrapValidationTest(t *testing.T, descriptorCSVPath string) {
	m := decodeDescriptorDSV(t, descriptorCSVPath)
	for id, data := range m {
		var desc descpb.Descriptor
		require.NoError(t, protoutil.Unmarshal(data, &desc))
		ts := descpb.GetDescriptorModificationTime(&desc)
		if ts == (hlc.Timestamp{}) {
			ts = hlc.Timestamp{WallTime: 1}
		}
		_, err := unwrapDescriptor(context.Background(), m, ts, &desc, true)
		require.NoErrorf(t, err, "id: %d", id)
	}
}

// oneLevelMapDescGetter exists to mirror the behavior of the
// oneLevelTxnDescGetter but instead of reading from the key-value store, it
// reads from the map.
type oneLevelMapDescGetter map[descpb.ID][]byte

var _ catalog.DescGetter = (oneLevelMapDescGetter)(nil)

func (o oneLevelMapDescGetter) GetDesc(
	ctx context.Context, id descpb.ID,
) (catalog.Descriptor, error) {
	var desc descpb.Descriptor
	if err := protoutil.Unmarshal(o[id], &desc); err != nil {
		return nil, err
	}
	mt := descpb.GetDescriptorModificationTime(&desc)
	if mt == (hlc.Timestamp{}) {
		mt = hlc.Timestamp{WallTime: 1}
	}
	return unwrapDescriptorMutable(ctx, nil, mt, &desc)
}

func (o oneLevelMapDescGetter) GetDescs(
	ctx context.Context, reqs []descpb.ID,
) ([]catalog.Descriptor, error) {
	resps := make([]catalog.Descriptor, len(reqs))
	for i, r := range reqs {
		var err error
		resps[i], err = o.GetDesc(ctx, r)
		if err != nil {
			return nil, err
		}
	}
	return resps, nil
}

func decodeDescriptorDSV(t *testing.T, descriptorCSVPath string) oneLevelMapDescGetter {
	f, err := os.Open(descriptorCSVPath)
	require.NoError(t, err)
	defer f.Close()
	r := csv.NewReader(f)
	records, err := r.ReadAll()
	require.NoError(t, err)
	require.Equal(t, records[0], []string{"id", "descriptor"})
	records = records[1:]
	m := decodeCSVRecordsToDescGetter(t, records)
	return m
}

func decodeCSVRecordsToDescGetter(t *testing.T, records [][]string) oneLevelMapDescGetter {
	m := oneLevelMapDescGetter{}
	for _, rec := range records {
		id, err := strconv.Atoi(rec[0])
		require.NoError(t, err)
		decoded, err := hex.DecodeString(rec[1])
		require.NoError(t, err)
		m[descpb.ID(id)] = decoded
	}
	return m
}
