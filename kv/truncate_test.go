package kv

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/gogo/protobuf/proto"
)

func TestTruncate(t *testing.T) {
	defer leaktest.AfterTest(t)
	loc := func(s string) string {
		return string(keys.RangeDescriptorKey(roachpb.Key(s)))
	}
	testCases := []struct {
		keys     [][2]string
		expKeys  [][2]string
		from, to string
		desc     [2]string // optional, defaults to {from,to}
	}{
		{
			// Keys inside of active range.
			keys:    [][2]string{{"a", "q"}, {"c"}, {"b, e"}, {"q"}},
			expKeys: [][2]string{{"a", "q"}, {"c"}, {"b, e"}, {"q"}},
			from:    "a", to: "q\x00",
		},
		{
			// Keys outside of active range.
			keys:    [][2]string{{"a"}, {"a", "b"}, {"q"}, {"q", "z"}},
			expKeys: [][2]string{{}, {}, {}, {}},
			from:    "b", to: "q",
		},
		{
			// Range-local Keys outside of active range.
			keys:    [][2]string{{loc("e")}, {loc("a"), loc("b")}, {loc("e"), loc("z")}},
			expKeys: [][2]string{{}, {}, {}},
			from:    "b", to: "e",
		},
		{
			// Range-local Keys overlapping active range in various ways.
			// TODO(tschottdorf): those aren't handled nicely but I'll address
			// it in #2198. Right now local ranges can wind up going all over
			// the place.
			keys:    [][2]string{{loc("b")}, {loc("a"), loc("b\x00")}, {loc("c"), loc("f")}, {loc("a"), loc("z")}},
			expKeys: [][2]string{{loc("b")}, {"b", loc("b\x00")}, {loc("c"), "e"}, {"b", "e"}},
			from:    "b", to: "e",
		},
		{
			// Key range touching and intersecting active range.
			keys:    [][2]string{{"a", "b"}, {"a", "c"}, {"p", "q"}, {"p", "r"}, {"a", "z"}},
			expKeys: [][2]string{{}, {"b", "c"}, {"p", "q"}, {"p", "q"}, {"b", "q"}},
			from:    "b", to: "q",
		},
		// Active key range is intersection of descriptor and [from,to).
		{
			keys:    [][2]string{{"c", "q"}},
			expKeys: [][2]string{{"d", "p"}},
			from:    "a", to: "z",
			desc: [2]string{"d", "p"},
		},
		{
			keys:    [][2]string{{"c", "q"}},
			expKeys: [][2]string{{"d", "p"}},
			from:    "d", to: "p",
			desc: [2]string{"a", "z"},
		},
	}

	for i, test := range testCases {
		ba := &roachpb.BatchRequest{}
		for _, ks := range test.keys {
			if len(ks[1]) > 0 {
				ba.Add(&roachpb.ScanRequest{
					Span: roachpb.Span{Start: roachpb.Key(ks[0]), End: roachpb.Key(ks[1])},
				})
			} else {
				ba.Add(&roachpb.GetRequest{
					Span: roachpb.Span{Start: roachpb.Key(ks[0])},
				})
			}
		}
		original := proto.Clone(ba).(*roachpb.BatchRequest)

		desc := &roachpb.RangeDescriptor{
			StartKey: roachpb.Key(test.desc[0]), EndKey: roachpb.Key(test.desc[1]),
		}
		if len(desc.StartKey) == 0 {
			desc.StartKey = roachpb.Key(test.from)
		}
		if len(desc.EndKey) == 0 {
			desc.EndKey = roachpb.Key(test.to)
		}
		undo, num, err := truncate(ba, desc, roachpb.Key(test.from), roachpb.Key(test.to))
		if err != nil {
			t.Errorf("%d: %s", i, err)
		}
		var reqs int
		for j, arg := range ba.Requests {
			req := arg.GetInner()
			if h := req.Header(); !bytes.Equal(h.Start, roachpb.Key(test.expKeys[j][0])) || !bytes.Equal(h.End, roachpb.Key(test.expKeys[j][1])) {
				t.Errorf("%d.%d: range mismatch: actual [%q,%q), wanted [%q,%q)", i, j,
					h.Start, h.End, test.expKeys[j][0], test.expKeys[j][1])
			} else if _, ok := req.(*roachpb.NoopRequest); ok != (len(h.Start) == 0) {
				t.Errorf("%d.%d: expected NoopRequest, got %T", i, j, req)
			} else if len(h.Start) != 0 {
				reqs++
			}
		}
		if reqs != num {
			t.Errorf("%d: counted %d requests, but truncation indicated %d", i, reqs, num)
		}
		undo()
		if !reflect.DeepEqual(ba, original) {
			t.Errorf("%d: undoing truncation failed:\nexpected: %s\nactual: %s",
				i, original, ba)
		}
	}
}
