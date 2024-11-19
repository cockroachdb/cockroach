// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package slstorage

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestGetEncoder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type codecType int
	const (
		isNil codecType = iota
		isRbr
	)

	checkCodec := func(t *testing.T, typ codecType, codec keyCodec) {
		t.Helper()
		switch typ {
		case isNil:
			require.Nil(t, codec)
		case isRbr:
			require.NotNil(t, codec)
			_, ok := codec.(*rbrEncoder)
			require.True(t, ok, "expected %v to be an rbr encoder", codec)
		}
	}

	type testCase struct {
		name      string
		version   clusterversion.Key
		readCodec codecType
		dualCodec codecType
	}
	tests := []testCase{
		{
			name:      "current",
			version:   clusterversion.Latest,
			readCodec: isRbr,
			dualCodec: isNil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			storage := NewTestingStorage(
				log.AmbientContext{}, nil, nil, nil, keys.SystemSQLCodec, nil, nil, systemschema.SqllivenessTable(), nil, true /*withSyntheticClock*/)

			checkCodec(t, tc.readCodec, storage.keyCodec)
		})
	}
}
