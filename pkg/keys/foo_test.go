package keys_test

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestFoo(t *testing.T) {
	for idx := 5125; idx <= 5205; idx++ {
		k := storage.MVCCKey{
			Key: keys.RaftLogKey(37, uint64(idx)),
		}
		b := storage.EncodeKey(k)
		fmt.Printf("~/cockroach-pebble debug pebble find --value '%%x' . hex:%x | grep -A 1 -F '.log' | grep -Eo 'SET .*' | awk '{print $2}' | tee idx%d.log\n",
			b, idx)
	}
	t.Error("x")
}

func TestBar(t *testing.T) {
	for idx := 5171; idx <= 5171; idx++ {
		t.Run(fmt.Sprintf("idx=%d", idx), func(t *testing.T) {
			for n := 1; n <= 2; n++ {
				t.Run(fmt.Sprintf("n%d", n), func(t *testing.T) {
					hexBytes, err := ioutil.ReadFile(fmt.Sprintf("/home/tobias/Downloads/hotspotsplits-repro-61990/run_585/%d.idx%d.log", n, idx))
					b, err := hex.DecodeString(strings.TrimSpace(string(hexBytes)))
					require.NoError(t, err)
					var meta enginepb.MVCCMetadata
					require.NoError(t, meta.Unmarshal(b))
					v := roachpb.Value{RawBytes: meta.RawBytes}
					ent := &raftpb.Entry{}
					require.NoError(t, v.GetProto(ent))
					t.Logf("ent.Index=%d", ent.Index)
					t.Logf("ent.Term=%d", ent.Term)
					idKey, data := kvserver.DecodeRaftCommand(ent.Data)
					t.Logf("idKey=%x", idKey)
					var cmd kvserverpb.RaftCommand
					require.NoError(t, cmd.Unmarshal(data))
					t.Logf("cmd.MLAI=%d", cmd.MaxLeaseIndex)
				})
			}
		})
	}
}
