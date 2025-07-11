// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gen

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
)

// SettingsGen provides a method to generate simulations settings given a seed.
type SettingsGen interface {
	// Generate returns a new simulation settings that is parameterized
	// randomly by the seed provided.
	Generate(seed int64) config.SimulationSettings
}

// LoadGen provides a method to generate a workload generator given a seed and
// simulation settings.
type LoadGen interface {
	// Generate returns a workload generator that is parameterized randomly by
	// the seed and simulation settings provided.
	Generate(seed int64, settings *config.SimulationSettings) []workload.Generator
	String() string
}

// ClusterGen provides a method to generate the initial cluster state,  given a
// seed and simulation settings. The initial cluster state includes: nodes
// (including locality) and stores.
type ClusterGen interface {
	// Generate returns a new State that is parameterized randomly by the seed
	// and simulation settings provided.
	Generate(seed int64, settings *config.SimulationSettings) state.State
	String() string
	Regions() []state.Region
}

// RangeGen provides a method to generate the initial range splits, range
// replica and lease placement within a cluster.
type RangeGen interface {
	// Generate returns an updated state, given the initial state, seed and
	// simulation settings provided. In the updated state, ranges will have been
	// created, replicas and leases assigned to stores in the cluster.
	Generate(seed int64, settings *config.SimulationSettings, s state.State) state.State
	String() string
}

// GenerateSimulation is a utility function that creates a new allocation
// simulation using the provided state, workload, settings generators and seed.
func GenerateSimulation(
	duration time.Duration,
	clusterGen ClusterGen,
	rangeGen RangeGen,
	loadGen LoadGen,
	settingsGen SettingsGen,
	eventGen EventGen,
	seed int64,
) *asim.Simulator {
	settings := settingsGen.Generate(seed)
	s := clusterGen.Generate(seed, &settings)
	s = rangeGen.Generate(seed, &settings, s)
	eventExecutor := eventGen.Generate(seed, &settings)
	return asim.NewSimulator(
		duration,
		loadGen.Generate(seed, &settings),
		s,
		&settings,
		metrics.NewTracker(settings.MetricsInterval),
		eventExecutor,
	)
}

// StaticSettings implements the SettingsGen interface.
type StaticSettings struct {
	Settings *config.SimulationSettings
}

// Generate returns a new simulation settings where the settings are identical
// to the original settings but have the seed updated to be equal to the
// provided seed.
func (ss StaticSettings) Generate(seed int64) config.SimulationSettings {
	ret := *ss.Settings
	ret.Seed = seed
	return ret
}

type MultiLoad []BasicLoad

// MultiLoad implements the LoadGen interface. It is a collection of
// BasicLoad.
var _ LoadGen = MultiLoad{}

func (ml MultiLoad) String() string {
	var str string
	for _, load := range ml {
		str += fmt.Sprintf("%s\n", load.String())
	}
	return str
}

func (ml MultiLoad) Generate(seed int64, settings *config.SimulationSettings) []workload.Generator {
	var generators []workload.Generator
	for _, load := range ml {
		generators = append(generators, load.Generate(seed, settings)...)
	}
	return generators
}

// BasicLoad implements the LoadGen interface.
type BasicLoad struct {
	RWRatio             float64
	Rate                float64
	SkewedAccess        bool
	MinBlockSize        int
	MaxBlockSize        int
	MinKey, MaxKey      int64
	RequestCPUPerAccess int64
	RaftCPUPerWrite     int64
}

var _ LoadGen = BasicLoad{}

func (bl BasicLoad) String() string {
	return fmt.Sprintf(
		"basic load with rw_ratio=%0.2f, rate=%0.2f, skewed_access=%t, min_block_size=%d, max_block_size=%d, "+
			"min_key=%d, max_key=%d, request_cpu_per_access=%d, raft_cpu_per_write=%d",
		bl.RWRatio, bl.Rate, bl.SkewedAccess, bl.MinBlockSize, bl.MaxBlockSize,
		bl.MinKey, bl.MaxKey, bl.RequestCPUPerAccess, bl.RaftCPUPerWrite)
}

// Generate returns a new list of workload generators where the generator
// parameters are populated with the parameters from the generator and either a
// uniform or zipfian key generator is created depending on whether
// SkewedAccess is true. The returned workload generators are seeded with the
// provided seed.
func (bl BasicLoad) Generate(seed int64, settings *config.SimulationSettings) []workload.Generator {
	if bl.Rate == 0 {
		return []workload.Generator{}
	}

	var keyGen workload.KeyGenerator
	rand := rand.New(rand.NewSource(seed))
	if bl.SkewedAccess {
		keyGen = workload.NewZipfianKeyGen(bl.MinKey, bl.MaxKey, 1.1, 1, rand)
	} else {
		keyGen = workload.NewUniformKeyGen(bl.MinKey, bl.MaxKey, rand)
	}

	return []workload.Generator{
		workload.NewRandomGenerator(
			settings.StartTime,
			seed,
			keyGen,
			bl.Rate,
			bl.RWRatio,
			bl.MaxBlockSize,
			bl.MinBlockSize,
			bl.RequestCPUPerAccess,
			bl.RaftCPUPerWrite,
		),
	}
}

// LoadedCluster implements the ClusterGen interface.
type LoadedCluster struct {
	Info state.ClusterInfo
}

// Generate returns a new simulator state, where the cluster is loaded based on
// the cluster info the loaded cluster generator is created with. There is no
// randomness in this cluster generation.
func (lc LoadedCluster) Generate(seed int64, settings *config.SimulationSettings) state.State {
	return state.LoadClusterInfo(lc.Info, settings)
}

func (lc LoadedCluster) String() string {
	return fmt.Sprintf("loaded cluster with\n %v", lc.Info)
}

func (lc LoadedCluster) Regions() []state.Region {
	return lc.Info.Regions
}

// BasicCluster implements the ClusterGen interace.
type BasicCluster struct {
	Nodes               int
	StoresPerNode       int
	StoreByteCapacity   int64
	Region              []string
	NodesPerRegion      []int
	NodeCPURateCapacity int64
}

func (bc BasicCluster) String() string {
	var b strings.Builder
	_, _ = fmt.Fprintf(&b,
		"basic cluster with nodes=%d, stores_per_node=%d, store_byte_capacity=%d, node_cpu_rate_capacity=%d",
		bc.Nodes, bc.StoresPerNode, bc.StoreByteCapacity, bc.NodeCPURateCapacity)
	if len(bc.Region) != 0 {
		_, _ = fmt.Fprintf(&b, ", region=%v, nodes_per_region=%v", bc.Region, bc.NodesPerRegion)
	}
	return b.String()
}

// Generate returns a new simulator state, where the cluster is created with all
// nodes having the same locality and with the specified number of stores/nodes
// created. The cluster is created based on the stores and stores-per-node
// values the basic cluster generator is created with.
func (bc BasicCluster) Generate(seed int64, settings *config.SimulationSettings) state.State {
	info := bc.info()
	info.StoreDiskCapacityBytes = bc.StoreByteCapacity
	info.NodeCPURateCapacityNanos = bc.NodeCPURateCapacity
	return state.LoadClusterInfo(info, settings)
}

func (bc BasicCluster) Regions() []state.Region {
	return bc.info().Regions
}

func (bc BasicCluster) info() state.ClusterInfo {
	if len(bc.Region) == 0 {
		return state.ClusterInfoWithStoreCount(bc.Nodes, bc.StoresPerNode)
	}

	// TODO(wenyihu6): we have the number of nodes and their localities already.
	// We could construct ClusterInfo without a ratio calculation. We are doing
	// this for now just to reuse ClusterInfoWithDistribution. But there may be
	// rounding errors.
	regionNodeWeights := make([]float64, len(bc.NodesPerRegion))
	totalNodes := 0
	for i, nodes := range bc.NodesPerRegion {
		regionNodeWeights[i] = float64(nodes) / float64(bc.Nodes)
		totalNodes += nodes
	}
	if totalNodes != bc.Nodes {
		panic(fmt.Sprintf("total nodes %d does not match expected nodes %d", totalNodes, bc.Nodes))
	}
	return state.ClusterInfoWithDistribution(bc.Nodes, bc.StoresPerNode, bc.Region, regionNodeWeights)
}

// LoadedRanges implements the RangeGen interface.
type LoadedRanges struct {
	Info state.RangesInfo
}

func (lr LoadedRanges) String() string {
	return fmt.Sprintf("loaded ranges with ranges=%d", len(lr.Info))
}

// Generate returns an updated simulator state, where the cluster is loaded
// with the range info that the generator was created with. There is no
// randomness in this cluster generation.
func (lr LoadedRanges) Generate(
	seed int64, settings *config.SimulationSettings, s state.State,
) state.State {
	state.LoadRangeInfo(s, lr.Info...)
	return s
}

// PlacementType represents a type of placement distribution.
type PlacementType int

const (
	Even PlacementType = iota
	Skewed
	Random
	WeightedRandom
	ReplicaPlacement
)

func (p PlacementType) String() string {
	switch p {
	case Even:
		return "even"
	case Skewed:
		return "skewed"
	case Random:
		return "random"
	case WeightedRandom:
		return "weighted_rand"
	case ReplicaPlacement:
		return "replica_placement"
	default:
		panic("unknown placement type")
	}
}

func GetRangePlacementType(s string) PlacementType {
	switch s {
	case "even":
		return Even
	case "skewed":
		return Skewed
	case "random":
		return Random
	case "weighted_rand":
		return WeightedRandom
	case "replica_placement":
		return ReplicaPlacement
	default:
		panic(fmt.Sprintf("unknown placement type %s", s))
	}
}

// BaseRanges provide fundamental range functionality and are embedded in
// specialized range structs. These structs implement the RangeGen interface
// which is then utilized to generate allocator simulation. Key structs that
// embed BaseRanges are: BasicRanges, RandomizedBasicRanges, and
// WeightedRandomizedBasicRanges.
type BaseRanges struct {
	Ranges            int
	MinKey, MaxKey    int64
	ReplicationFactor int
	Bytes             int64
	ReplicaPlacement  state.ReplicaPlacement
}

func (b BaseRanges) String() string {
	return fmt.Sprintf("ranges=%d, min_key=%d, max_key=%d, replication_factor=%d, bytes=%d", b.Ranges, b.MinKey, b.MaxKey, b.ReplicationFactor, b.Bytes)
}

// GetRangesInfo generates and distributes ranges across stores based on
// PlacementType while using other BaseRanges fields for range configuration.
func (b BaseRanges) GetRangesInfo(
	pType PlacementType, numOfStores int, randSource *rand.Rand, weightedRandom []float64,
) state.RangesInfo {
	switch pType {
	case Even:
		return state.RangesInfoEvenDistribution(numOfStores, b.Ranges, b.MinKey, b.MaxKey, b.ReplicationFactor, b.Bytes)
	case Skewed:
		return state.RangesInfoSkewedDistribution(numOfStores, b.Ranges, b.MinKey, b.MaxKey, b.ReplicationFactor, b.Bytes)
	case Random:
		return state.RangesInfoRandDistribution(randSource, numOfStores, b.Ranges, b.MinKey, b.MaxKey, b.ReplicationFactor, b.Bytes)
	case WeightedRandom:
		return state.RangesInfoWeightedRandDistribution(
			randSource, weightedRandom, b.Ranges, b.MinKey, b.MaxKey, b.ReplicationFactor, b.Bytes)
	case ReplicaPlacement:
		return state.RangesInfoWithReplicaPlacement(
			b.ReplicaPlacement,
			b.Ranges,
			state.DefaultSpanConfigWithRF(b.ReplicationFactor),
			b.MinKey, b.MaxKey, b.Bytes,
		)
	default:
		panic(fmt.Sprintf("unexpected range placement type %v", pType))
	}
}

// LoadRangeInfo loads the given state with the specified rangesInfo.
func (b BaseRanges) LoadRangeInfo(s state.State, rangesInfo state.RangesInfo) {
	state.LoadRangeInfo(s, rangesInfo...)
}

// BasicRanges implements the RangeGen interface, supporting basic range info
// distribution, including even and skewed distributions.
type BasicRanges struct {
	BaseRanges
	PlacementType PlacementType
}

func (br BasicRanges) String() string {
	return fmt.Sprintf("basic ranges with placement_type=%v, %v", br.PlacementType, br.BaseRanges)
}

// Generate returns an updated simulator state, where the cluster is loaded with
// ranges generated based on the parameters specified in the fields of
// BasicRanges.
func (br BasicRanges) Generate(
	seed int64, settings *config.SimulationSettings, s state.State,
) state.State {
	if br.PlacementType == Random || br.PlacementType == WeightedRandom {
		panic("BasicRanges generate only uniform or skewed distributions")
	}
	rangesInfo := br.GetRangesInfo(br.PlacementType, len(s.Stores()), nil, []float64{})
	br.LoadRangeInfo(s, rangesInfo)
	return s
}

// MultiRanges implements the RangeGen interface, supporting multiple
// BasicRanges generation.
type MultiRanges []BasicRanges

var _ RangeGen = MultiRanges{}

func (mr MultiRanges) String() string {
	var str string
	for _, ranges := range mr {
		str += fmt.Sprintf("%s\n", ranges.String())
	}
	return str
}

func (mr MultiRanges) Generate(
	seed int64, settings *config.SimulationSettings, s state.State,
) state.State {
	var rangeInfos []state.RangeInfo
	for _, ranges := range mr {
		rangeInfos = append(rangeInfos,
			ranges.GetRangesInfo(ranges.PlacementType, len(s.Stores()), nil, []float64{})...)
	}
	state.LoadRangeInfo(s, rangeInfos...)
	return s
}
