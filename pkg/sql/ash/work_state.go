// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"context"
	"encoding/hex"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/petermattis/goid"
)

// WorkState represents the current work state of a goroutine.
// Goroutines explicitly register their state when performing work
// that should be captured by ASH sampling.
type WorkState struct {
	// WorkloadID identifies the workload (e.g., statement fingerprint).
	WorkloadID uint64
	// WorkEventType categorizes the type of work.
	WorkEventType WorkEventType
	// TODO(alyshan): Make an int enum for this.
	// WorkEvent is a more specific identifier for the work event.
	WorkEvent string
	// AppNameID is the uint64 identifier for the app_name of the SQL session
	// that executed the query. Only set for work that comes from SQL sessions.
	// Zero means no app_name is associated with this work.
	AppNameID uint64
	// GatewayNodeID is the ID of the gateway node where the request originated.
	// This is used to fetch app name mappings when they're not available locally.
	GatewayNodeID roachpb.NodeID
	// prev is the previous work state for the goroutine if one exists.
	prev *WorkState
}

// workStatePool pools WorkState structs to reduce allocations.
var workStatePool = sync.Pool{
	New: func() any { return &WorkState{} },
}

// activeWorkStates maps goroutine IDs to their work state.
// This is a global map that the sampler reads from periodically.
var activeWorkStates sync.Map // int64 (goroutine ID) -> *WorkState

// retiredWorkStates holds WorkState objects that have been removed from
// activeWorkStates but cannot yet be returned to the pool. This is because
// the sampler may still hold a pointer obtained from sync.Map.Range.
// clearWorkState appends here; the sampler drains after RangeWorkStates.
var retiredWorkStates struct {
	syncutil.Mutex
	states []*WorkState
}

// appNameMap stores the mapping from app name ID (uint64) to app name string.
// This map is node-local and is populated when app names are first seen.
// TODO(alyshan): A sharded map might outperform sync.Map (write heavy).
var appNameMap sync.Map // uint64 -> string

// SetWorkState registers the current goroutine's work state.
// It returns a cleanup function that should be deferred to clear the state.
// If ASH sampling is disabled, this is a no-op and returns a no-op cleanup function.
//
// Example usage:
//
//	cleanup := ash.SetWorkState(stmtFingerprintID, ash.WORK_LOCK, "LockWait")
//	defer cleanup()
//	// ... perform work ...
func SetWorkState(workloadID uint64, eventType WorkEventType, event string) func() {
	return SetWorkStateWithAppName(workloadID, eventType, event, 0, 0)
}

// SetWorkStateWithAppName registers the current goroutine's work state with
// app name and gateway node information. It returns a cleanup function that
// should be deferred to clear the state. If ASH sampling is disabled, this is
// a no-op and returns a no-op cleanup function.
//
// Example usage:
//
//	cleanup := ash.SetWorkStateWithAppName(stmtFingerprintID, ash.WORK_LOCK, "LockWait", appNameID, gatewayNodeID)
//	defer cleanup()
//	// ... perform work ...
func SetWorkStateWithAppName(
	workloadID uint64,
	eventType WorkEventType,
	event string,
	appNameID uint64,
	gatewayNodeID roachpb.NodeID,
) func() {
	if !enabled.Load() {
		return func() {}
	}
	gid := goid.Get()
	state := workStatePool.Get().(*WorkState)
	state.WorkloadID = workloadID
	state.WorkEventType = eventType
	state.WorkEvent = event
	state.AppNameID = appNameID
	state.GatewayNodeID = gatewayNodeID
	prev, ok := getWorkState(gid)
	if ok {
		state.prev = prev
	} else {
		state.prev = nil
	}
	activeWorkStates.Store(gid, state)
	return func() {
		clearWorkState(gid)
	}
}

// EncodeUint64ToBytes returns the []byte representation of an uint64 value.
func EncodeUint64ToBytes(id uint64) []byte {
	result := make([]byte, 0, 8)
	return encoding.EncodeUint64Ascending(result, id)
}

// EncodeStmtFingerprintIDToString returns the hex string representation of a statement fingerprint ID.
func EncodeStmtFingerprintIDToString(id uint64) string {
	return hex.EncodeToString(EncodeUint64ToBytes(id))
}

// HashAppName computes a uint64 hash of the app_name string.
// This identifier is used to represent app names in WorkState without
// passing the full string in every request.
func HashAppName(appName string) uint64 {
	if appName == "" {
		return 0
	}
	fnv := util.MakeFNV64()
	for _, b := range []byte(appName) {
		fnv.Add(uint64(b))
	}
	return fnv.Sum()
}

// GetOrStoreAppNameID hashes the app_name string to an ID and stores it in
// the local map if it doesn't already exist. Returns the ID.
func GetOrStoreAppNameID(appName string) uint64 {
	if appName == "" {
		return 0
	}
	id := HashAppName(appName)
	// Use Load first to avoid the allocation from boxing appName into
	// interface{} on every call to LoadOrStore. The common case (app name
	// already seen) hits Load and returns without allocating.
	if _, ok := appNameMap.Load(id); ok {
		return id
	}
	appNameMap.LoadOrStore(id, appName)
	return id
}

// StoreAppNameMapping stores a single app name ID to string mapping.
// This is used when the ID is already known (e.g., received in a
// SetupFlowRequest) and we only need to populate the local map
// without re-hashing the app name.
func StoreAppNameMapping(id uint64, appName string) {
	if id != 0 && appName != "" {
		appNameMap.Store(id, appName)
	}
}

// GetAppName retrieves the app name string for the given ID from the local map.
// Returns the app name and true if found, empty string and false otherwise.
func GetAppName(id uint64) (string, bool) {
	if id == 0 {
		return "", false
	}
	val, ok := appNameMap.Load(id)
	if !ok {
		return "", false
	}
	return val.(string), true
}

// GetAllAppNameMappings returns all app name ID to string mappings in the local map.
// This is used when a node requests mappings from another node.
func GetAllAppNameMappings() map[uint64]string {
	result := make(map[uint64]string)
	appNameMap.Range(func(key, value any) bool {
		result[key.(uint64)] = value.(string)
		return true
	})
	return result
}

// StoreAppNameMappings stores multiple app name ID to string mappings in the local map.
// This is used when fetching mappings from another node.
func StoreAppNameMappings(mappings map[uint64]string) {
	for id, appName := range mappings {
		if id != 0 && appName != "" {
			appNameMap.Store(id, appName)
		}
	}
}

// AppNameMappingsRequest requests all app name ID to string mappings from a node.
type AppNameMappingsRequest struct {
	// NodeID is the ID of the node to request mappings from.
	NodeID roachpb.NodeID
}

// AppNameMappingsResponse returns all app name ID to string mappings.
type AppNameMappingsResponse struct {
	// Mappings is a map from app name ID (uint64) to app name string.
	Mappings map[uint64]string
}

// ResolveAppNameID is a function type that can resolve app name IDs to app names.
// This is used by the sampler to resolve app name IDs when they're not available locally.
type ResolveAppNameID func(context.Context, *AppNameMappingsRequest) (*AppNameMappingsResponse, error)

// clearWorkState removes the work state for the given goroutine.
// The removed WorkState is placed on the retired list rather than
// immediately returned to the pool, because the sampler may still
// hold a pointer to it from a concurrent sync.Map.Range call.
// The sampler calls reclaimRetiredWorkStates after each Range to
// safely return retired states to the pool.
func clearWorkState(gid int64) {
	v, ok := activeWorkStates.Load(gid)
	if !ok {
		return
	}
	state := v.(*WorkState)
	if state.prev != nil {
		activeWorkStates.Store(gid, state.prev)
	} else {
		activeWorkStates.Delete(gid)
	}
	// Do NOT zero fields or return to pool here. The sampler may
	// still be reading this WorkState via a pointer obtained from
	// sync.Map.Range. Defer reclamation to the sampler.
	retireWorkState(state)
}

// retireWorkState adds a WorkState to the retired list for deferred pool return.
func retireWorkState(state *WorkState) {
	retiredWorkStates.Lock()
	defer retiredWorkStates.Unlock()
	retiredWorkStates.states = append(retiredWorkStates.states, state)
}

// reclaimRetiredWorkStates returns all retired WorkState objects to the pool.
// This must be called after RangeWorkStates completes, at which point the
// sampler no longer holds any pointers from the previous iteration.
func reclaimRetiredWorkStates() {
	states := drainRetiredWorkStates()
	for _, s := range states {
		*s = WorkState{}
		workStatePool.Put(s)
	}
}

// drainRetiredWorkStates atomically swaps out the retired list and returns it.
func drainRetiredWorkStates() []*WorkState {
	retiredWorkStates.Lock()
	defer retiredWorkStates.Unlock()
	states := retiredWorkStates.states
	retiredWorkStates.states = retiredWorkStates.states[:0]
	return states
}

func getWorkState(gid int64) (*WorkState, bool) {
	v, ok := activeWorkStates.Load(gid)
	if !ok {
		return nil, false
	}
	return v.(*WorkState), true
}

// RangeWorkStates iterates over all registered work states.
// The callback receives a copy of the WorkState to avoid data races during iteration.
func RangeWorkStates(fn func(gid int64, state WorkState) bool) {
	activeWorkStates.Range(func(key, value any) bool {
		return fn(key.(int64), *value.(*WorkState))
	})
}
