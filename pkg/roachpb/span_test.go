// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachpb_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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
		error  string
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
		{
			name:   "clamp start error",
			span:   roachpb.Span{},
			bounds: roachpb.Span{tp(2), tp(1)},
			want:   roachpb.Span{},
			error:  "cannot clamp when min '/Table/2' is larger than max '/Table/1'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			span, err := tt.span.Clamp(tt.bounds)
			if !testutils.IsError(err, tt.error) {
				t.Fatalf("expected error to be '%s', got '%s'", tt.error, err)
			}
			if !span.Equal(tt.want) {
				t.Errorf("Clamp() = %v, want %v", span, tt.want)
			}
		})
	}
}
