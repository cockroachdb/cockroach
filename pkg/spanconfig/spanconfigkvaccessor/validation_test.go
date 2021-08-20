package spanconfigkvaccessor

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestValidation(t *testing.T) {
	for _, tc := range []struct {
		toDelete []roachpb.Span
		toUpsert []roachpb.SpanConfigEntry
		expErr   string
	}{
		{
			toUpsert: nil, toDelete: nil,
			expErr: "",
		},
		{
			toDelete: []roachpb.Span{
				{Key: roachpb.Key("a")}, // empty end key in delete list
			},
			expErr: "invalid span: a",
		},
		{
			toUpsert: []roachpb.SpanConfigEntry{
				{
					Span: roachpb.Span{Key: roachpb.Key("a")}, // empty end key in update list
				},
			},
			expErr: "invalid span: a",
		},
		{
			toUpsert: []roachpb.SpanConfigEntry{
				{
					Span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("a")}, // invalid span; end < start
				},
			},
			expErr: "invalid span: {b-a}",
		},
		{
			toDelete: []roachpb.Span{
				{Key: roachpb.Key("b"), EndKey: roachpb.Key("a")}, // invalid span; end < start
			},
			expErr: "invalid span: {b-a}",
		},
		{
			toDelete: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}, // overlapping spans in the same list
				{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
			},
			expErr: "overlapping spans {a-c} and {b-c} in same list",
		},
		{
			toUpsert: []roachpb.SpanConfigEntry{ // overlapping spans in the same list
				{
					Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
				},
				{
					Span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
				},
			},
			expErr: "overlapping spans {a-c} and {b-c} in same list",
		},
		{
			toDelete: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
			},
			toUpsert: []roachpb.SpanConfigEntry{ // overlapping spans in different lists
				{
					Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
				},
				{
					Span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
				},
			},
			expErr: "",
		},
	} {
		require.True(t, testutils.IsError(validateUpdateArgs(tc.toDelete, tc.toUpsert), tc.expErr))
	}
}
