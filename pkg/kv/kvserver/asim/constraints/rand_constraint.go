package constraints

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
)

type zoneConstraint struct {
	tierValue string
	replicas  int
	voters    int
}

type zoneConstraints struct {
	c   []zoneConstraint
	sum int
}

type clusterConstraints []clusterConstraint

type clusterConstraint struct {
	replicas int
	voters   int
	c        []regionConstraint
}

type regionConstraints struct {
	rc  []regionConstraint
	sum int
}

type regionConstraint struct {
	tierValue string
	replicas  int
	voters    int
	c         []zoneConstraint // zones
}

func FormatConstraint(c clusterConstraints) (res []string) {
	// Format the constraint string
	buf := &strings.Builder{}

	strHelper := func(buf *strings.Builder, tierKey string, tierValue string, num int) {
		if buf.Len() > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf("'+%s=%s':%d", tierKey, tierValue, num))
	}
	for _, cluster := range c {
		if numReplicas := cluster.replicas; numReplicas >= 0 {
			buf.WriteString(fmt.Sprintf("num_replicas=%d ", numReplicas))
		}
		if numVoters := cluster.voters; numVoters >= 0 {
			buf.WriteString(fmt.Sprintf("num_voters=%d ", numVoters))
		}

		constraintBuf := &strings.Builder{}
		voterBuf := &strings.Builder{}
		for _, region := range cluster.c {
			if region.replicas >= 0 {
				strHelper(constraintBuf, "region", region.tierValue, region.replicas)
			}
			if region.voters >= 0 {
				strHelper(voterBuf, "region", region.tierValue, region.voters)
			}
			for _, zone := range region.c {
				if zone.replicas >= 0 {
					strHelper(constraintBuf, "zone", zone.tierValue, zone.replicas)
				}
				if zone.voters >= 0 {
					strHelper(voterBuf, "zone", zone.tierValue, zone.voters)
				}
			}
		}
		if constraintBuf.Len() > 0 {
			buf.WriteString(fmt.Sprintf("constraints={%s} ", constraintBuf.String()))
		}
		if voterBuf.Len() > 0 {
			buf.WriteString(fmt.Sprintf("voter_constraints={%s} ", voterBuf.String()))
		}
		res = append(res, buf.String())
		buf = &strings.Builder{}
	}
	return res
}

// for every zone, num_replicas = [0, total number of nodes in the zone];
// num_voters <= num_replicas
// for every region, num_replicas = no zoneConstraints [total of the zones
// above, total]; num_voters <= num_replicas
// for every cluster, num_replicas = total of the regions above , num_voters <=
// num_replicas
// future optimization: determine when a constraint is impossible to satisfy
// given the prev state is unsatisfiable

// regions -> num_replicas and num_voters
// zones -> num_replicas and num_voters
// cluster -> num_replicas and num_voters
// zoneConstraints [tierValue:constraint]

func total(r state.Region) (sum int) {
	for _, z := range r.Zones {
		sum += z.NodeCount
	}
	return sum
}

// zones -> zoneConstraints
func backtrackZone(
	zones []state.Zone, begin int, sum int, path *[]zoneConstraint, res *[]zoneConstraints,
) {
	// for every zone, a slice of possible zoneConstraints
	if begin == len(zones) {
		copySlice := make([]zoneConstraint, len(*path))
		copy(copySlice, *path)
		*res = append(*res, zoneConstraints{
			c:   copySlice,
			sum: sum,
		})
		return
	}
	for i := begin; i < len(zones); i++ {
		zone := zones[i]
		for numReplicas := -1; numReplicas <= zone.NodeCount; numReplicas++ {
			cnumReplicas := numReplicas
			if numReplicas == -1 {
				cnumReplicas = zone.NodeCount
			}
			// -1 means no constraint
			for numVoters := -1; numVoters <= cnumReplicas; numVoters++ {
				*path = append(*path, zoneConstraint{
					tierValue: zone.Name,
					replicas:  numReplicas,
					voters:    numVoters,
				})
				if numReplicas != -1 {
					backtrackZone(zones, i+1, sum+numReplicas, path, res)
				} else {
					backtrackZone(zones, i+1, sum, path, res)
				}
				*path = (*path)[:len(*path)-1]
			}
		}
	}
}

func backtrackRegion(
	regions []state.Region,
	begin int,
	sum int,
	path *[]regionConstraint,
	res *[]regionConstraints,
	clusters map[string]int,
) {
	if begin == len(regions) {
		copySlice := make([]regionConstraint, len(*path))
		copy(copySlice, *path)
		*res = append(*res, regionConstraints{
			rc:  copySlice,
			sum: sum,
		})
		return
	}

	for i := begin; i < len(regions); i++ {
		region := regions[i]
		zoneConstraints := make([]zoneConstraints, 0)
		backtrackZone(region.Zones, 0, 0, &[]zoneConstraint{}, &zoneConstraints)
		for _, zone := range zoneConstraints {
			for numReplicas := -1; numReplicas <= clusters[region.Name]; numReplicas++ {
				if numReplicas == 0 {
					numReplicas = zone.sum
				}
				cnumReplicas := numReplicas
				if numReplicas == -1 {
					cnumReplicas = clusters[region.Name]
				}
				for numVoters := -1; numVoters <= cnumReplicas; numVoters++ {
					*path = append(*path, regionConstraint{
						tierValue: region.Name,
						replicas:  numReplicas,
						voters:    numVoters,
						c:         zone.c,
					})
					if numReplicas == -1 {
						backtrackRegion(regions, i+1, sum, path, res, clusters)
					} else {
						backtrackRegion(regions, i+1, sum+numReplicas, path, res, clusters)
					}
					*path = (*path)[:len(*path)-1]
				}
			}
		}
	}
}

func processTotalNums(r []state.Region) (map[string]int, int) {
	clusters := make(map[string]int)
	total := 0
	for _, region := range r {
		for _, zone := range region.Zones {
			clusters[region.Name] += zone.NodeCount
			total += zone.NodeCount
		}
	}
	return clusters, total
}

func backtrackCluster(regions []state.Region, clustersConstraint *clusterConstraints) {
	clusterInfo, total := processTotalNums(regions)
	regionConstraints := make([]regionConstraints, 0)
	backtrackRegion(regions, 0, 0, &[]regionConstraint{}, &regionConstraints, clusterInfo)

	// backtrackZone(region.Zones, 0, 0, &[]zoneConstraint{}, &zoneConstraints)
	for _, region := range regionConstraints {
		for numReplicas := -1; numReplicas <= total; numReplicas++ {
			if numReplicas == 0 {
				numReplicas = region.sum
			}
			cnumReplicas := numReplicas
			if numReplicas == -1 {
				cnumReplicas = total
			}
			for numVoters := -1; numVoters <= cnumReplicas; numVoters++ {
				*clustersConstraint = append(*clustersConstraint, clusterConstraint{
					replicas: numReplicas,
					voters:   numVoters,
					c:        region.rc,
				})
			}
		}
	}
}

func backtrack(regions []state.Region) clusterConstraints {
	res := make(clusterConstraints, 0)
	backtrackCluster(regions, &res)
	return res
}

func allConstraints(cluster []state.Region) []string {
	res := FormatConstraint(backtrack(cluster))
	sort.Strings(res)
	for i := 1; i < len(res); i++ {
		if res[i] == res[i-1] {
			fmt.Println("duplicate")
		}
	}
	fmt.Println(len(res))
	return res
}
