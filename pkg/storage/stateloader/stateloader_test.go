package stateloader

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/datadriven"
	"go.etcd.io/etcd/raft"
)

var testMakeConfState = `
# Not in a joint config, just two voters.
cs reps=(1,2)
----
Voters:[1 2] VotersOutgoing:[] Learners:[] LearnersNext:[] AutoLeave:false

# A voter and an outgoing voter. We'll see AutoLeave=true because this is a
# joint state (so are all others that follow).
cs reps=(1) joint=(2)
----
Voters:[1] VotersOutgoing:[2] Learners:[] LearnersNext:[] AutoLeave:true

# Test that a learner in the incoming config gets read into LearnersNext if it
# overlaps an outgoing voter.
cs reps=(1,2LEARNER) joint=(2)
----
Voters:[1] VotersOutgoing:[2] Learners:[] LearnersNext:[2] AutoLeave:true

# Add another learner that does not overlap to check that it goes into Learners.
cs reps=(1,2LEARNER,4LEARNER) joint=(2,3)
----
Voters:[1] VotersOutgoing:[2 3] Learners:[4] LearnersNext:[2] AutoLeave:true

# Add some learners to joint where they should not make a difference (the
# incoming descriptor has all the relevant info on learners already).
cs reps=(1,2LEARNER,4LEARNER) joint=(2,3,4LEARNER,1LEARNER)
----
Voters:[1] VotersOutgoing:[2 3] Learners:[4] LearnersNext:[2] AutoLeave:true
`

func TestMakeConfState(t *testing.T) {
	all := make([]roachpb.ReplicaDescriptor, 10)
	for i := 0; i < len(all)/2; i++ {
		all[i] = roachpb.ReplicaDescriptor{
			NodeID:    roachpb.NodeID(i + 1),
			StoreID:   roachpb.StoreID(i + 1),
			ReplicaID: roachpb.ReplicaID(i + 1),
		}
		all[i+5] = roachpb.ReplicaDescriptor{
			NodeID:    roachpb.NodeID(i + 6),
			StoreID:   roachpb.StoreID(i + 6),
			ReplicaID: roachpb.ReplicaID(i + 6),
			Type:      roachpb.ReplicaTypeLearner(),
		}
	}

	datadriven.RunTestFromString(t, testMakeConfState, func(d *datadriven.TestData) string {
		var reps, joint []roachpb.ReplicaDescriptor
		for _, arg := range d.CmdArgs {
			for i := 0; i < len(arg.Vals); i++ {
				var rep roachpb.ReplicaDescriptor
				if strings.HasSuffix(arg.Vals[i], "LEARNER") {
					arg.Vals[i] = arg.Vals[i][:len(arg.Vals[i])-7]
					rep.Type = roachpb.ReplicaTypeLearner()
				}
				var id int
				arg.Scan(t, i, &id)
				rep.ReplicaID, rep.StoreID, rep.NodeID = roachpb.ReplicaID(id), roachpb.StoreID(id), roachpb.NodeID(id)
				switch arg.Key {
				case "reps":
					reps = append(reps, rep)
				case "joint":
					joint = append(joint, rep)
				}
			}
		}
		return raft.DescribeConfState(makeConfState(reps, joint...)) + "\n"
	})
}
