package keys_test

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"sort"
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
	fs, err := filepath.Glob(
		"../../idx/*.idx*.log",
	)
	require.NoError(t, err)
	re := regexp.MustCompile(`(\d+)\.idx(\d+)\.log`)

	type res struct {
		node              string
		entIndex, entTerm uint64
		idKey             []byte
		mlai              uint64
		leaseSeq          int64
		raftCmd           kvserverpb.RaftCommand
	}

	var rs []res

	for _, f := range fs {
		base := filepath.Base(f)
		sl := re.FindStringSubmatch(base)
		require.NotZero(t, sl, "%s", base)
		idx := sl[2] // raft entry index
		n := sl[1]   // source node

		hexBytes, err := ioutil.ReadFile(f)
		require.GreaterOrEqual(t, len(hexBytes), 4)

		// TODO(tbg): this doesn't handle n3 which is the only node that has
		// seen multiple entries for some log entries. We exclude n3 right now.
		if n == "3" {
			continue
		}
		hexBytes = hexBytes[4:] // cut off 'SET '
		b, err := hex.DecodeString(strings.TrimSpace(string(hexBytes)))
		require.NoError(t, err)
		var meta enginepb.MVCCMetadata
		require.NoError(t, meta.Unmarshal(b), "%s", base)
		v := roachpb.Value{RawBytes: meta.RawBytes}
		ent := &raftpb.Entry{}
		require.NoError(t, v.GetProto(ent))
		require.Equal(t, idx, fmt.Sprint(ent.Index)) // sanity check

		r := res{
			entIndex: ent.Index,
			entTerm:  ent.Term,
		}
		if len(ent.Data) != 0 {
			idKey, data := kvserver.DecodeRaftCommand(ent.Data)
			r.idKey = []byte(idKey)
			var cmd kvserverpb.RaftCommand
			require.NoError(t, cmd.Unmarshal(data))
			r.mlai = cmd.MaxLeaseIndex
			r.leaseSeq = int64(cmd.ProposerLeaseSequence)
			cmd.WriteBatch = nil
			r.raftCmd = cmd
		}
		rs = append(rs, r)
	}

	// Sort by index first, then by node, then by term. The term dimension
	// matters since n3 has seen some entries at more than one term.
	sort.Slice(rs, func(i, j int) bool {
		if a, b := rs[i].entIndex, rs[j].entIndex; a < b {
			return true
		} else if a > b {
			return false
		}
		if a, b := rs[i].node, rs[j].node; a < b {
			return true
		} else if a > b {
			return false
		}
		return rs[i].entTerm < rs[j].entTerm
	})

	n2mlaiToRes := map[uint64]res{}
	var i = 0
	for i < len(rs)-1 {
		n1 := rs[i]
		n2 := rs[i+1]                                        // n2's entry at the same index
		require.Equal(t, int(n1.entIndex), int(n2.entIndex)) // sanity check
		n2mlaiToRes[n2.mlai] = n2
		fmt.Printf("n1(idx=%d):", n1.entIndex)
		a, b := n1.entIndex, n2mlaiToRes[n1.mlai].entIndex
		if n1.mlai != n2.mlai { // divergent log entry
			fmt.Printf(" n2(idx=%d) delta=%d", b, a-b)
		}
		if st := n2.raftCmd.ReplicatedEvalResult.State; st != nil {
			if st.TruncatedState != nil {
				fmt.Printf(" trunc(%+v)", st.TruncatedState)
			}
		}
		if n2.raftCmd.MaxLeaseIndex == 0 {
			fmt.Printf(" term-change(%d)", n2.entTerm)
		}
		fmt.Println()
		for i < len(rs)-1 && rs[i].entIndex == n1.entIndex {
			i++
		}
	}
	return

	/*
		for idx := range m {
			t.Run(fmt.Sprintf("idx=%s", idx), func(t *testing.T) {
				for n := range m[idx] {

					switch n {
					case "1":
						rs.n1 = r
					case "2":
						rs.n2 = r
					case "3":
						rs.n3 = r
					default:
						t.Fatal(n)
					}
					t.Run(n, func(t *testing.T) {
						t.Logf("%+v", r)
					})
				}
				require.NotZero(t, rs.n2)
				t.Logf("%+v", rs.n2)
				if st := rs.n2.raftCmd.ReplicatedEvalResult.State; st != nil {
					if st.TruncatedState != nil {
						t.Run(fmt.Sprintf("log truncation %+v", st.TruncatedState), func(t *testing.T) {
							t.Errorf("log truncation: %+v", st.TruncatedState)
						})
					}
				}
				if rs.n2.raftCmd.MaxLeaseIndex == 0 {
					t.Run(fmt.Sprintf("term-change to %d", rs.n2.entTerm), func(t *testing.T) {
						t.Errorf("empty command")
					})
				}
				t.Run("n2 and n1 differ", func(t *testing.T) {
					require.Equal(t, rs.n2, rs.n1, "expected like n2, but n1 is different")
				})
				if rs.n3.entIndex != 0 {
					require.Equal(t, rs.n2, rs.n3)
				}
				// Map the "true" MLAI of the entry at the current index
				// (i.e. that on n2 or n3, not the one on n1) to the resSet.
				mlaiToIdx[rs.n2.mlai] = rs
			})
		}
		for mlai, rs := range mlaiToIdx {
			if mlai != rs.n1.mlai {
				t.Errorf("on n1, idx %d holds actual index %d", rs.n1.entIndex, mlaiToIdx[rs.n1.mlai].n2.entIndex)
			}
		}

	*/
}
