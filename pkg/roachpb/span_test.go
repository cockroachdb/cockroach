// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachpb_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

func TestSpanZeroLength(t *testing.T) {
	// create two separate references here.
	shouldBeEmpty := roachpb.Span{
		Key:    keys.SystemSQLCodec.TablePrefix(1),
		EndKey: keys.SystemSQLCodec.TablePrefix(1),
	}
	if !shouldBeEmpty.ZeroLength() {
		t.Fatalf("expected span %s to be empty.", shouldBeEmpty)
	}

	shouldNotBeEmpty := roachpb.Span{
		Key:    keys.SystemSQLCodec.TablePrefix(1),
		EndKey: keys.SystemSQLCodec.TablePrefix(1).Next(),
	}
	if shouldNotBeEmpty.ZeroLength() {
		t.Fatalf("expected span %s to not be empty.", shouldNotBeEmpty)
	}
}

func TestSpanClamp(t *testing.T) {
	tp := keys.SystemSQLCodec.TablePrefix
	tests := []struct {
		name   string
		span   roachpb.Span
		bounds roachpb.Span
		want   roachpb.Span
	}{
		{
			name:   "within bounds",
			span:   roachpb.Span{tp(5), tp(10)},
			bounds: roachpb.Span{tp(0), tp(15)},
			want:   roachpb.Span{tp(5), tp(10)},
		},
		{
			name:   "clamp lower bound",
			span:   roachpb.Span{tp(0), tp(10)},
			bounds: roachpb.Span{tp(5), tp(15)},
			want:   roachpb.Span{tp(5), tp(10)},
		},
		{
			name:   "clamp upper bound",
			span:   roachpb.Span{tp(5), tp(20)},
			bounds: roachpb.Span{tp(0), tp(15)},
			want:   roachpb.Span{tp(5), tp(15)},
		},
		{
			name:   "clamp both bounds",
			span:   roachpb.Span{tp(0), tp(20)},
			bounds: roachpb.Span{tp(5), tp(15)},
			want:   roachpb.Span{tp(5), tp(15)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.span.Clamp(tt.bounds); !got.Equal(tt.want) {
				t.Errorf("Clamp() = %v, want %v", got, tt.want)
			}
		})
	}
}
