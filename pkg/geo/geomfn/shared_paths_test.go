// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geomfn

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/stretchr/testify/require"
)

func TestSharedPaths(t *testing.T) {
	type args struct {
		a *geo.Geometry
		b *geo.Geometry
	}
	tests := []struct {
		name    string
		args    args
		want    *geo.Geometry
		wantErr bool
	}{
		{
			name: "shared path between a multilinestring and linestring",
			args: args{
				a: geo.MustParseGeometry("MULTILINESTRING((26 125,26 200,126 200,126 125,26 125)," +
					"(51 150,101 150,76 175,51 150))"),
				b: geo.MustParseGeometry("LINESTRING(151 100,126 156.25,126 125,90 161, 76 175)"),
			},
			want: geo.MustParseGeometry("GEOMETRYCOLLECTION(MULTILINESTRING((126 156.25,126 125)," +
				"(101 150,90 161),(90 161,76 175)),MULTILINESTRING EMPTY)"),
		},
		{
			name: "shared path between a linestring and multilinestring",
			args: args{
				a: geo.MustParseGeometry("LINESTRING(76 175,90 161,126 125,126 156.25,151 100)"),
				b: geo.MustParseGeometry("MULTILINESTRING((26 125,26 200,126 200,126 125,26 125), " +
					"(51 150,101 150,76 175,51 150))"),
			},
			want: geo.MustParseGeometry("GEOMETRYCOLLECTION(MULTILINESTRING EMPTY," +
				"MULTILINESTRING((76 175,90 161),(90 161,101 150),(126 125,126 156.25)))"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SharedPaths(tt.args.a, tt.args.b)
			if (err != nil) != tt.wantErr {
				t.Errorf("SharedPaths() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			require.Equal(t, tt.want, got)
		})
	}
}
