package kv

import (
	"testing"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
)

func TestTruncate(t *testing.T) {
	testCases := []struct {
		keys     [][2]proto.Key
		from, to proto.Key
		desc     [2]proto.Key
		expNoop  []bool
	}{
		{
			// TODO(tschottdorf): add more test cases.
			keys: [][2]proto.Key{{keys.RangeDescriptorKey(proto.Key("e")), nil}},
			from: proto.Key("b"), to: proto.Key("g\x00\x00"),
			desc:    [2]proto.Key{proto.Key("b"), proto.Key("e")},
			expNoop: []bool{true},
		},
	}

	for i, test := range testCases {
		ba := &proto.BatchRequest{}
		for _, ks := range test.keys {
			ba.Add(&proto.PutRequest{
				RequestHeader: proto.RequestHeader{Key: ks[0], EndKey: ks[1]},
			})
		}
		undo, err := truncate(ba, &proto.RangeDescriptor{
			StartKey: test.desc[0], EndKey: test.desc[1],
		}, test.from, test.to)
		if err != nil {
			t.Errorf("%d: %s", i, err)
		}
		for j, arg := range ba.Requests {
			if _, ok := arg.GetValue().(*proto.NoopRequest); ok != test.expNoop[i] {
				t.Errorf("%d: %d: is_noop=%t, exp_noop=%t", i, j, !ok, test.expNoop[i])
			}
		}
		undo()
	}
}
