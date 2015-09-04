package kv

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/batch"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/leaktest"

	gogoproto "github.com/gogo/protobuf/proto"
)

func TestTruncate(t *testing.T) {
	defer leaktest.AfterTest(t)
	loc := func(s string) string {
		return string(keys.RangeDescriptorKey(proto.Key(s)))
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
		ba := &proto.BatchRequest{}
		for _, ks := range test.keys {
			if len(ks[1]) > 0 {
				ba.Add(&proto.ScanRequest{
					RequestHeader: proto.RequestHeader{Key: proto.Key(ks[0]), EndKey: proto.Key(ks[1])},
				})
			} else {
				ba.Add(&proto.GetRequest{
					RequestHeader: proto.RequestHeader{Key: proto.Key(ks[0])},
				})
			}
		}
		original := gogoproto.Clone(ba).(*proto.BatchRequest)

		desc := &proto.RangeDescriptor{
			StartKey: proto.Key(test.desc[0]), EndKey: proto.Key(test.desc[1]),
		}
		if len(desc.StartKey) == 0 {
			desc.StartKey = proto.Key(test.from)
		}
		if len(desc.EndKey) == 0 {
			desc.EndKey = proto.Key(test.to)
		}
		undo, num, err := truncate(ba, desc, proto.Key(test.from), proto.Key(test.to))
		if err != nil {
			t.Errorf("%d: %s", i, err)
		}
		var reqs int
		for j, arg := range ba.Requests {
			req := arg.GetValue().(proto.Request)
			if h := req.Header(); !bytes.Equal(h.Key, proto.Key(test.expKeys[j][0])) || !bytes.Equal(h.EndKey, proto.Key(test.expKeys[j][1])) {
				t.Errorf("%d.%d: range mismatch: actual [%q,%q), wanted [%q,%q)", i, j,
					h.Key, h.EndKey, test.expKeys[j][0], test.expKeys[j][1])
			} else if _, ok := req.(*proto.NoopRequest); ok != (len(h.Key) == 0) {
				t.Errorf("%d.%d: expected NoopRequest, got %T", i, j, req)
			} else if len(h.Key) != 0 {
				reqs++
			}
		}
		if reqs != num {
			t.Errorf("%d: counted %d requests, but truncation indicated %d", i, reqs, num)
		}
		undo()
		if !reflect.DeepEqual(ba, original) {
			t.Errorf("%d: undoing truncation failed:\nexpected: %s\nactual: %s",
				i, batch.Short(original), batch.Short(ba))
		}
	}
}
