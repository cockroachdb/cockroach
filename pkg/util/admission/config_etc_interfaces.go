// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"iter"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
)

// Code comments here are a mix of implementation notes and usage notes, to
// illustrate the thought process -- should not be interpreted as real comments.

// Config and ConfigSnapshot are implemented outside the core admission package.

type Config interface {
	// Snapshot is called every 1s when recomputing token bucket sizes for the
	// next 1s.
	//
	// It is called by what is currently called the cpuTimeTokenAllocator -- it no
	// longer knows about cluster settings etc. This is the one source of
	// configuration.
	Snapshot() ConfigSnapshot
}

const MatchAllTenantID = 0
const MatchAllGroupID = 0

// ElasticGroupID is for future use.
const ElasticGroupID = 1

// DefaultGroupID is used when no resource groups are configured.
const DefaultGroupID = 2

// We should consider reserving a prefix of GroupIDs, say [1, 5) for internal
// purposes.

// ResourceGroup serves two purposes:
// - as the key for the config.
// - as a parameter when seeking admission.
//
// In the latter case both fields must be non-zero.
//
// In the former case, the (0, 0) and (0, <non-zero>) pair are permitted. The
// (<non-zero>, 0) pair is not permitted, since there is no difficulty in
// enumerating group-ids (unlike tenant-ids).
//
// When seeking admission, policy matching tries to exact match both fields. If
// that fails, it tries to match (0, 1), i.e., the admission request specifies
// (tenant-id, 1), i.e., it is elastic work for some tenant. If that fails, it
// matches (0, 0).
//
// In the serverless case, the config will specify three groups:
// (1, <default-group-id>): for system tenant regular work
// (0, 0): for all application tenant's regular work
// (0, 1): (eventually) for all tenant's elastic work.
type ResourceGroup struct {
	TenantID roachpb.TenantID
	GroupID  uint64
}

// GroupConfig is the config for a group.
//
// For serverless, the system tenant (1, <default-group-id>) has
// Weight=math.MaxUint32, BurstQualification=1.0, and other tenants (0, 0) have
// Weight=1, BurstQualification=0.2.
//
// In a real resource group world, the weight is the WEIGHT_CPU, and the
// BurstQualification is the normalized fraction of the weight.
type GroupConfig struct {
	Weight uint32
	// Fraction is in the interval [0, 1].
	BurstQualificationFraction float64
	// TODO(future): add fields for the case this group represents elastic work.
}

// ConfigSnapshot is immutable and very cheap to query, since no locking, and
// internal data-structures are pre-computed for speed (since configs change
// rarely and are queried often).
type ConfigSnapshot interface {
	Configs() iter.Seq2[ResourceGroup, GroupConfig]
	// MaxFraction is the goal fraction of CPU used when there is always burstable
	// work. Likely set to 1.0, but could be lower. Will be derived from a cluster setting.
	MaxFraction() float64
	// MaxNonBurstableFraction is the goal fraction of CPU used when there is no
	// burstable work. For serverless, this may be set to 0.8. In the resource
	// manager docs, we have mentioned 0.75. Anyway, this will be derived from a
	// cluster setting.
	MaxNonBurstableFraction() float64
	// BurstableTokenBucketFullnessFraction is debatable. It corresponds to an
	// internal implementation detail, which for completeness, could be
	// configurable. For example, when a group has BurstQualificationFraction=0.2,
	// we use that to determine a burst token bucket rate and capacity. But we
	// also need to decide what fraction of that token bucket needs to be full to
	// allow for bursting. Currently we hard-code that to 0.9.
	BurstableTokenBucketFullnessFraction() float64

	ResourceGroupTransformer
}

// ResourceGroupTransformer allows the core admission package to be agnostic of
// the context it is functioning in. Transform is called as the first step in
// admission.WorkQueue (outside the mutex). Some scenarios:
//
//   - Running in serverless mode: values of the form (t, *, p) are transformed
//     into (t, 2, p) since we want all the work for a tenant to share the same
//     resource group.
//
//   - Running in resource group mode where the system tenant has defined resource
//     groups for all tenants to share: values of the form (t, g, p) where g > 0
//     are transformed into (1, g, p) since we want work for all tenants for group
//     g to share the same burstable budget. Values of the form (t, 0, p)
//     represent the caller does not know the resource group, and would be
//     transformed to (t, 2, p) since 2 is the default resource group.
//
//   - Running in a mode where < normal pri is one resource group (g2) and the
//     rest is another (g1). Value (t, g, p) is transformed into (1, g2, 0) if p <
//     normal pri else (1, g1, 0).
type ResourceGroupTransformer interface {
	Transform(ResourceGroup, admissionpb.WorkPriority) (ResourceGroup, admissionpb.WorkPriority)
}

// workQueueForBurstBuckets is implemented by WorkQueue.
//
// burstAllocationTick is called every 1ms and burstReset every 1s. After
// burstReset returns, the WorkQueue guarantees it will never call an older
// filler. This allows the caller to reuse burstBucketFillers (the caller only
// needs a pair of them).
type workQueueForBurstBuckets interface {
	burstAllocationTick()
	burstReset(filler *burstBucketFiller)
}

// burstBucketFiller is initialized every 1s by the cpuTimeTokenAllocator, using
// its computed tokens and the current ConfigSnapshot, and then handed to the
// WorkQueue by calling burstReset. The internals are opaque to the WorkQueue,
// which must only call the methods: getCapacityAndWeight, getTokensToAdd, and
// the ResourceGroupTransformer method.
type burstBucketFiller struct {
	// burstConfigs contain the fully specified groups.
	burstConfigs map[ResourceGroup]bucketConfig
	// matchAllConfig is for the (0, 0) group.
	matchAllConfig bucketConfig
	ResourceGroupTransformer
	// TODO: measure if we need to optimize doing a series of map lookups every
	// 1ms.
	//
	// TODO: the above doesn't handle the (0, 1) case since unification of elastic
	// work is in the future.
}

type bucketConfig struct {
	tokensToAddPerTick int64
	tokenCapacity      int64
	weight             uint32
}

// On every burstReset, the WorkQueue iterates over its current groups map and
// calls this method to get the new capacity, to adjust the capacity value, and
// the new weight. It also calls this when constructing a new groupInfo in
// response to a new Admit request.
func (bbf *burstBucketFiller) getCapacityAndWeight(g ResourceGroup) (cap int64, w uint32) {
	bc := bbf.getBucketConfigInternal(g)
	return bc.tokenCapacity, bc.weight
}

// On every burstAllocationTick, the WorkQueue iterates over its current groups
// map and calls this method to get the tokens to add.
func (bbf *burstBucketFiller) getTokensToAdd(g ResourceGroup) int64 {
	return bbf.getBucketConfigInternal(g).tokensToAddPerTick
}

func (bbf *burstBucketFiller) getBucketConfigInternal(g ResourceGroup) bucketConfig {
	bc, ok := bbf.burstConfigs[g]
	if ok {
		return bc
	}
	return bbf.matchAllConfig
}
