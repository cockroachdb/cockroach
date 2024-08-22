package tests

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
)

var InterestingCases = []struct {
	description         string
	constraint          string
	expectedSuccess     bool
	expectedErrorMsgStr string
}{
	{
		description:         "straightforward valid configuration",
		constraint:          "num_replicas=2 num_voters=1",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "straightforward valid configuration",
		constraint: "num_replicas=5 num_voters=5 " +
			"constraints={'+region=a':3,'+region=b':1,'+region=c':1} " +
			"voter_constraints={'+region=a':3,'+region=b':1,'+region=c':1}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "promotion to satisfy region voter constraint",
		constraint: "num_replicas=2 num_voters=2 " +
			"constraints={'+zone=b1':2} voter_constraints={'+region=b':2}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description:         "promotion to satisfy cluster constraint",
		constraint:          "num_replicas=2 num_voters=2 constraints={'+zone=b1':2}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "promoting some nonvoters to voters",
		constraint: "num_replicas=6 num_voters=3 constraints={'+zone=a3':3} " +
			"voter_constraints={'+region=a':3,'+zone=a2':2}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "promoting some nonvoters + add voters + add nonvoters",
		constraint: "num_replicas=15 num_voters=6 " +
			"constraints={'+zone=a4':10,'+region=c':3,'+region=a':11} " +
			"voter_constraints={'+region=a':3,'+zone=a3':1,'+zone=b1':1}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description:         "satisfying zone constraint can help satisfy region constraint",
		constraint:          "num_replicas=2 constraints={'+zone=b1':2,'+region=b':2}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "cluster is fully assigned by region constraints",
		constraint: "num_replicas=28 num_voters=28 " +
			"constraints={'+region=a':16,'+region=b':2,'+region=c':10}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "cluster is fully assigned by region and zone constraints",
		constraint: "num_replicas=28 num_voters=28 " +
			"constraints={'+region=a':16,'+region=b':2,'+region=c':10," +
			"'+zone=a1':1,'+zone=a2':2,'+zone=a3':3,'+zone=a4':10,'+zone=b1':2," +
			"'+zone=c1':3,'+zone=c2':3,'+zone=c3':4} " +
			"voter_constraints={'+region=a':16,'+region=b':2,'+region=c':10," +
			"'+zone=a1':1,'+zone=a2':2,'+zone=a3':3,'+zone=a4':10,'+zone=b1':2," +
			"'+zone=c1':3,'+zone=c2':3,'+zone=c3':4}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "having unconstrained replicas + unconstrained voters",
		constraint: "num_replicas=28 num_voters=25 " +
			"constraints={'+region=a':2} voter_constraints={'+region=a':2}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description:         "having unconstrained replicas + fully constrained voters",
		constraint:          "num_replicas=27 num_voters=16 voter_constraints={'+region=a':16}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "having fully constrained replicas + unconstrained voters",
		constraint: "num_replicas=16 num_voters=3 " +
			"constraints={'+region=a':16,'+zone=a1':1,'+zone=a2':2} " +
			"voter_constraints={'+zone=a4':3}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "can promote any replicas to voters at cluster level",
		constraint: "num_replicas=28 num_voters=3 " +
			"constraints={'+region=a':16,'+region=b':2,'+region=c':10} " +
			"voter_constraints={'+region=c':2}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "configuration for issue #106559",
		constraint: "num_replicas=6 num_voters=5 " +
			"constraints={'+zone=b1':1,'+zone=c1':1,'+zone=a2':2,'+zone=a3':2} " +
			"voter_constraints={'+zone=b1':1,'+zone=c1':1,'+zone=a2':2,'+zone=a3':1}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "configuration for issue #106559",
		constraint: "num_replicas=6 num_voters=5 " +
			"constraints={'+zone=b1':1,'+zone=c1':1,'+zone=a2':1,'+zone=a3':1} " +
			"voter_constraints={'+zone=b1':2,'+zone=a2':2}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "configuration for issue #122292",
		constraint: "num_replicas=4 num_voters=3 " +
			"constraints={'+region=a':1,'+zone=a1':1,'+zone=a2':1} " +
			"voter_constraints={'+zone=a2':2}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description:         "no voters or replicas needed to add for constraints",
		constraint:          "num_replicas=0 constraints={'+zone=a1':0}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "insufficient replicas for region constraint",
		constraint: "num_replicas=28 num_voters=28 " +
			"constraints={'+region=a':17,'+region=b':2,'+region=c':10}",
		expectedSuccess:     false,
		expectedErrorMsgStr: "failed to satisfy constraints for region a",
	},
	{
		description: "insufficient replicas for cluster constraints",
		constraint: "num_replicas=16 num_voters=3 " +
			"constraints={'+region=a':16} voter_constraints={'+region=c':2}",
		expectedSuccess:     false,
		expectedErrorMsgStr: "failed to satisfy constraints for cluster",
	},
	{
		description:         "more voters than replicas",
		constraint:          "num_replicas=1 num_voters=2",
		expectedSuccess:     false,
		expectedErrorMsgStr: "failed to satisfy constraints for cluster",
	},
	{
		description:         "too many replicas for cluster constraint",
		constraint:          "num_replicas=6 num_voters=2 constraints={'+region=a':16}",
		expectedSuccess:     false,
		expectedErrorMsgStr: "failed to satisfy constraints for cluster",
	},
	{
		description:         "too many voters for cluster constraint",
		constraint:          "num_replicas=20 num_voters=2 voter_constraints={'+region=a':16}",
		expectedSuccess:     false,
		expectedErrorMsgStr: "failed to satisfy constraints for cluster",
	},
	{
		description: "zero NumReplicas should use total num_replicas, num_voters for constraints",
		constraint: "num_replicas=5 num_voters=3 " +
			"constraints={'+region=a'} voter_constraints={'+region=b'}",
		expectedSuccess:     false,
		expectedErrorMsgStr: "failed to satisfy constraints for region b",
	},
	{
		description:         "unsupported constraint key",
		constraint:          "num_replicas=5 constraints={'+az=a'}",
		expectedSuccess:     false,
		expectedErrorMsgStr: "only zone and region constraint keys are supported",
	},
	{
		description:         "unsupported constraint value",
		constraint:          "num_replicas=5 num_voters=1 voter_constraints={'+region=e':1}",
		expectedSuccess:     false,
		expectedErrorMsgStr: "region constraint value e is not found in the cluster set up",
	},
	{
		description:         "unsupported constraint value",
		constraint:          "num_replicas=5 constraints={'+zone=CA':1}",
		expectedSuccess:     false,
		expectedErrorMsgStr: "zone constraint value CA is not found in the cluster set up",
	},
	{
		description:         "unsupported constraint type",
		constraint:          "num_replicas=5 constraints={'-region=b':1}",
		expectedSuccess:     false,
		expectedErrorMsgStr: "constraints marked as Constraint_PROHIBITED are unsupported",
	},
}

func GetInterestingSpanConfigs() []zonepb.ZoneConfig {
	spanConfigs := make([]zonepb.ZoneConfig, 0)
	for _, c := range InterestingCases {
		spanConfigs = append(spanConfigs, spanconfigtestutils.ParseZoneConfig(&testing.T{}, c.constraint))
	}
	return spanConfigs
}

type constraint struct {
	replicas int
	voters   int
}

func FormatConstraint(
	numReplicas int, numVoters int, regions map[string]constraint, zones map[string]constraint,
) string {
	// Format the constraint string
	buf := &strings.Builder{}
	if numReplicas > 0 {
		buf.WriteString(fmt.Sprintf("num_replicas=%d ", numReplicas))
	}
	if numVoters > 0 {
		buf.WriteString(fmt.Sprintf("num_voters=%d ", numVoters))
	}
	return buf.String()
}

// for every zone, num_replicas = [0, total number of nodes in the zone];
// num_voters <= num_replicas
// for every region, num_replicas = no constraints [total of the zones
// above, total]; num_voters <= num_replicas
// for every cluster, num_replicas = total of the regions above , num_voters <=
// num_replicas
// future optimization: determine when a constraint is impossible to satisfy
// given the prev state is unsatisfiable

// regions -> num_replicas and num_voters
// zones -> num_replicas and num_voters
// cluster -> num_replicas and num_voters
// constraints [tierValue:constraint]

func total(r state.Region) (sum int) {
	for _, z := range r.Zones {
		sum += z.NodeCount
	}
	return sum
}

func push(m []map[string]constraint) {

}

// For every region,
// For every zones, make a decision on numReplicas and then on numVoters
func backtrack(regions []state.Region, m []map[string]constraint) {
	for _, region := range regions {
		for _, zone := range region.Zones {
			totalReplicas := 0
			// make a decision here on num_replicas for the zone [0, total)
			// make a decision internally again for num_voters [0, num_replicas)
			for numReplicas := 0; numReplicas <= zone.NodeCount; numReplicas++ {
				totalReplicas += numReplicas
				for numVoters := 0; numVoters <= numReplicas; numVoters++ {
					// Make a deicision on replicas
					m[zone.Name] = constraint{
						replicas: numReplicas,
						voters:   numVoters,
					}
				}
				totalReplicas -= numReplicas
			}
		}

		for numReplicas := 0; numReplicas < total(region); numReplicas++ {
			for numVoters := 0; numVoters <= numReplicas; numVoters++ {
				m[region.Name] = constraint{
					replicas: numReplicas,
					voters:   numVoters,
				}
			}
		}
	}
}
