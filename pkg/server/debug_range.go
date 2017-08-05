// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package server

import (
	"bytes"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

const (
	debugRangeHeaderStore                = "Store"
	debugRangeHeaderNode                 = "Node"
	debugRangeHeaderKeyRange             = "Key Range"
	debugRangeHeaderRaftState            = "Raft State"
	debugRangeHeaderLeaseHolder          = "Lease Holder"
	debugRangeHeaderLeaseType            = "Lease Type"
	debugRangeHeaderLeaseEpoch           = "Lease Epoch"
	debugRangeHeaderLeaseStart           = "Lease Start"
	debugRangeHeaderLeaseExpiration      = "Lease Expiration"
	debugRangeHeaderLeaseAppliedIndex    = "Lease Applied Index"
	debugRangeHeaderRaftLeader           = "Raft Leader"
	debugRangeHeaderVote                 = "Vote"
	debugRangeHeaderTerm                 = "Term"
	debugRangeHeaderApplied              = "Applied"
	debugRangeHeaderCommit               = "Commit"
	debugRangeHeaderLastIndex            = "Last Index"
	debugRangeHeaderLogSize              = "Log Size"
	debugRangeHeaderLeaseholderQPS       = "Leaseholder QPS"
	debugRangeHeaderKeysWrittenPerSecond = "Keys Written Per Second"
	debugRangeHeaderApproxProposalQuota  = "Approx Proposal Quota"
	debugRangeHeaderPendingCommands      = "Pending Commands"
	debugRangeHeaderDroppedCommands      = "Dropped Commands"
	debugRangeHeaderTruncatedIndex       = "Truncated Index"
	debugRangeHeaderTruncatedTerm        = "Truncated Term"
	debugRangeHeaderMVCCLastUpdate       = "MVCC Last Update"
	debugRangeHeaderMVCCIntentAge        = "MVCC Intent Age"
	debugRangeHeaderMVCCGCBytesAge       = "MVCC GC Bytes Age"
	debugRangeHeaderMVCCLive             = "MVCC Live Bytes/Count"
	debugRangeHeaderMVCCKey              = "MVCC Key Bytes/Count"
	debugRangeHeaderMVCCVal              = "MVCC Value Bytes/Count"
	debugRangeHeaderMVCCIntent           = "MVCC Intent Bytes/Count"
	debugRangeHeaderMVCCSys              = "MVCC System Bytes/Count"

	debugRangeClassWarning       = "warning"
	debugRangeClassMatch         = "match"
	debugRangeClassMissing       = "missing"
	debugRangeClassLeaseHolder   = "lease-holder"
	debugRangeClassLeaseFollower = "lease-follower"
	debugRangeClassRaftLeader    = "raftstate-leader"
	debugRangeClassRaftFollower  = "raftstate-follower"
	debugRangeClassRaftDormant   = "raftstate-dormant"
	debugRangeClassOtherRange    = "other-range"

	debugRangeValueEmpty = "-"

	debugRangeValueLeaseNone       = "Unknown"
	debugRangeValueLeaseExpiration = "Expiration"
	debugRangeValueLeaseEpoch      = "Epoch"
)

var debugRangeHeaderWarnings = []string{
	debugRangeHeaderLeaseHolder,
	debugRangeHeaderLeaseType,
	debugRangeHeaderLeaseEpoch,
	debugRangeHeaderLeaseStart,
	debugRangeHeaderLeaseExpiration,
	debugRangeHeaderLeaseAppliedIndex,
	debugRangeHeaderRaftLeader,
	debugRangeHeaderTerm,
	debugRangeHeaderApplied,
	debugRangeHeaderCommit,
	debugRangeHeaderLastIndex,
	debugRangeHeaderTruncatedIndex,
	debugRangeHeaderTruncatedTerm,
	debugRangeHeaderMVCCLastUpdate,
	debugRangeHeaderMVCCIntentAge,
	debugRangeHeaderMVCCGCBytesAge,
	debugRangeHeaderMVCCLive,
	debugRangeHeaderMVCCKey,
	debugRangeHeaderMVCCVal,
	debugRangeHeaderMVCCIntent,
	debugRangeHeaderMVCCSys,
}

// Returns an HTML page displaying information about all node's view of a
// specific range.
func (s *statusServer) handleDebugRange(w http.ResponseWriter, r *http.Request) {
	ctx := s.AnnotateCtx(r.Context())
	w.Header().Add("Content-type", "text/html")
	rangeIDString := r.URL.Query().Get("id")
	if len(rangeIDString) == 0 {
		http.Error(
			w,
			"no range ID provided, please specify one: debug/range?id=[range_id]",
			http.StatusNoContent,
		)
	}

	rangeID, err := parseInt64WithDefault(rangeIDString, 1)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data := debugRangeData{
		RangeID:  rangeID,
		replicas: make(map[roachpb.ReplicaID][]roachpb.ReplicaDescriptor),
	}

	type nodeResponse struct {
		nodeID roachpb.NodeID
		resp   *serverpb.RangesResponse
		err    error
	}

	isLiveMap := s.nodeLiveness.GetIsLiveMap()
	aliveNodes := len(isLiveMap)
	responses := make(chan nodeResponse)
	nodeCtx, cancel := context.WithTimeout(ctx, base.NetworkTimeout)
	defer cancel()
	for nodeID, alive := range isLiveMap {
		if !alive {
			data.Failures = append(data.Failures, serverpb.RangeInfo{
				SourceNodeID: nodeID,
				ErrorMessage: "node liveness reports that the node is not alive",
			})
			aliveNodes--
			continue
		}
		nodeID := nodeID
		if err := s.stopper.RunAsyncTask(
			nodeCtx, "server.statusServer: requesting remote ranges",
			func(ctx context.Context) {
				status, err := s.dialNode(nodeID)
				var rangesResponse *serverpb.RangesResponse
				if err == nil {
					req := &serverpb.RangesRequest{
						RangeIDs: []roachpb.RangeID{roachpb.RangeID(rangeID)},
					}
					rangesResponse, err = status.Ranges(ctx, req)
				}
				response := nodeResponse{
					nodeID: nodeID,
					resp:   rangesResponse,
					err:    err,
				}

				select {
				case responses <- response:
					// Response processed.
				case <-ctx.Done():
					// Context completed, response no longer needed.
				}
			}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	for remainingResponses := aliveNodes; remainingResponses > 0; remainingResponses-- {
		select {
		case resp := <-responses:
			if resp.err != nil {
				data.Failures = append(data.Failures, serverpb.RangeInfo{
					SourceNodeID: resp.nodeID,
					ErrorMessage: resp.err.Error(),
				})
				continue
			}
			for _, info := range resp.resp.Ranges {
				if len(info.ErrorMessage) != 0 {
					data.Failures = append(data.Failures, info)
				} else {
					data.rangeInfos = append(data.rangeInfos, info)
					for _, desc := range info.State.Desc.Replicas {
						data.replicas[desc.ReplicaID] = append(data.replicas[desc.ReplicaID], desc)
					}
				}
			}
		case <-ctx.Done():
			http.Error(w, ctx.Err().Error(), http.StatusRequestTimeout)
			return
		}
	}

	// Fetch the range history.
	rangeLogReq := &serverpb.RangeLogRequest{RangeId: rangeID}
	if data.rangeLogResp, err = s.admin.RangeLog(nodeCtx, rangeLogReq); err != nil {
		data.Failures = append(data.Failures, serverpb.RangeInfo{
			SourceNodeID: s.gossip.NodeID.Get(),
			ErrorMessage: err.Error(),
		})
	}

	data.postProcessing()
	t, err := template.New("webpage").Parse(debugRangeTemplate)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := t.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// replicaIDSlice implements sort.Interface.
type replicaIDSlice []roachpb.ReplicaID

var _ sort.Interface = replicaIDSlice(nil)

func (r replicaIDSlice) Len() int           { return len(r) }
func (r replicaIDSlice) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r replicaIDSlice) Less(i, j int) bool { return r[i] < r[j] }

// rangeInfoSlice implements sort.Interface.
type rangeInfoSlice []serverpb.RangeInfo

var _ sort.Interface = rangeInfoSlice(nil)

func (r rangeInfoSlice) Len() int      { return len(r) }
func (r rangeInfoSlice) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r rangeInfoSlice) Less(i, j int) bool {
	if r[i].SourceNodeID != r[j].SourceNodeID {
		return r[i].SourceNodeID < r[j].SourceNodeID
	}
	return r[i].SourceStoreID < r[j].SourceStoreID
}

type debugOutput struct {
	Class string
	Title string
	Value string
}

type debugLeaseDetail struct {
	Replica         debugOutput
	Epoch           debugOutput
	ProposedTS      debugOutput
	ProposedTSDelta debugOutput
	Expiration      debugOutput
	Start           debugOutput
	StartDelta      debugOutput
}

type debugRangeLogEvent struct {
	RowClass     string
	Timestamp    debugOutput
	StoreID      debugOutput
	EventType    debugOutput
	RangeID      debugOutput
	OtherRangeID debugOutput
	Info         debugOutput
}

type debugRangeData struct {
	RangeID      int64
	Failures     rangeInfoSlice
	rangeInfos   rangeInfoSlice
	replicas     map[roachpb.ReplicaID][]roachpb.ReplicaDescriptor
	rangeLogResp *serverpb.RangeLogResponse

	// The following are populated in post-processing.
	ReplicaIDs        replicaIDSlice
	StoreIDs          roachpb.StoreIDSlice
	HeaderKeys        []string
	Results           map[string]map[roachpb.StoreID]*debugOutput
	HeaderFakeStoreID roachpb.StoreID
	LeaseHistory      []debugLeaseDetail
	LeaseEpoch        bool // true if epoch based, false if expiration based
	RangeLog          []debugRangeLogEvent
}

func (d *debugRangeData) postProcessing() {
	d.HeaderFakeStoreID = roachpb.StoreID(0)

	// Populate ReplicaIDs
	d.ReplicaIDs = make(replicaIDSlice, 0, len(d.replicas))
	for repID := range d.replicas {
		d.ReplicaIDs = append(d.ReplicaIDs, repID)
	}

	addHeader := func(header string) {
		d.HeaderKeys = append(d.HeaderKeys, header)
		d.Results[header] = make(map[roachpb.StoreID]*debugOutput)
		d.Results[header][d.HeaderFakeStoreID] = &debugOutput{
			Title: header,
			Value: header,
		}
	}

	// convertTimestamp returns a human readable version of the timestamp.
	convertTimestamp := func(timestamp hlc.Timestamp) string {
		return fmt.Sprintf("%s, %d", timestamp.GoTime(), timestamp.Logical)
	}

	replicaHeader := func(repID roachpb.ReplicaID) string {
		return fmt.Sprintf("Replica %d", repID)
	}

	convertBytes := func(bytes, count int64) *debugOutput {
		return &debugOutput{
			Title: fmt.Sprintf("%d bytes / %d", bytes, count),
			Value: fmt.Sprintf("%d bytes / %d", bytes, count),
		}
	}

	outputSameTitleValue := func(value string) *debugOutput {
		return &debugOutput{Title: value, Value: value}
	}

	outputSameTitleValueWithClass := func(value, class string) *debugOutput {
		return &debugOutput{Title: value, Value: value, Class: class}
	}

	// Prepare the replica output.
	d.Results = make(map[string]map[roachpb.StoreID]*debugOutput)

	// Add all headers except for the replicas, this is the order they will be
	// displayed in.
	addHeader(debugRangeHeaderStore)
	addHeader(debugRangeHeaderNode)
	addHeader(debugRangeHeaderKeyRange)
	addHeader(debugRangeHeaderRaftState)
	addHeader(debugRangeHeaderLeaseHolder)
	addHeader(debugRangeHeaderLeaseType)
	addHeader(debugRangeHeaderLeaseEpoch)
	addHeader(debugRangeHeaderLeaseStart)
	addHeader(debugRangeHeaderLeaseExpiration)
	addHeader(debugRangeHeaderLeaseAppliedIndex)
	addHeader(debugRangeHeaderRaftLeader)
	addHeader(debugRangeHeaderVote)
	addHeader(debugRangeHeaderTerm)
	addHeader(debugRangeHeaderApplied)
	addHeader(debugRangeHeaderCommit)
	addHeader(debugRangeHeaderLastIndex)
	addHeader(debugRangeHeaderLogSize)
	addHeader(debugRangeHeaderLeaseholderQPS)
	addHeader(debugRangeHeaderKeysWrittenPerSecond)
	addHeader(debugRangeHeaderApproxProposalQuota)
	addHeader(debugRangeHeaderPendingCommands)
	addHeader(debugRangeHeaderDroppedCommands)
	addHeader(debugRangeHeaderTruncatedIndex)
	addHeader(debugRangeHeaderTruncatedTerm)
	addHeader(debugRangeHeaderMVCCLastUpdate)
	addHeader(debugRangeHeaderMVCCIntentAge)
	addHeader(debugRangeHeaderMVCCGCBytesAge)
	addHeader(debugRangeHeaderMVCCLive)
	addHeader(debugRangeHeaderMVCCKey)
	addHeader(debugRangeHeaderMVCCVal)
	addHeader(debugRangeHeaderMVCCIntent)
	addHeader(debugRangeHeaderMVCCSys)

	// Add the replica headers.
	sort.Sort(d.ReplicaIDs)
	for _, repID := range d.ReplicaIDs {
		repHeader := replicaHeader(repID)
		d.HeaderKeys = append(d.HeaderKeys, repHeader)
		d.Results[repHeader] = make(map[roachpb.StoreID]*debugOutput)
		d.Results[repHeader][d.HeaderFakeStoreID] = &debugOutput{
			Title: repHeader,
			Value: repHeader,
		}

		// Fill all stores with an empty output for all replicas.
		for _, info := range d.rangeInfos {
			d.Results[repHeader][info.SourceStoreID] = &debugOutput{
				Value: debugRangeValueEmpty,
			}
		}
	}

	// leaderStoreInfo keeps tack of the raftLeader with the most recent term.
	var leaderStoreInfo serverpb.RangeInfo
	// latestTermInfo is used when there is no leader but we want to
	// display some details about one of the replicas.
	var latestTermInfo serverpb.RangeInfo

	// Convert each rangeInfo into debugRangeOutputs.
	for _, info := range d.rangeInfos {
		d.StoreIDs = append(d.StoreIDs, info.SourceStoreID)

		// Add the replica first in order to get the sourceReplicaID.
		var sourceReplicaID roachpb.ReplicaID
		var sourceReplicaHeader string
		for _, desc := range info.State.Desc.Replicas {
			var replicaHeaderClass string
			repHeader := replicaHeader(desc.ReplicaID)
			if desc.StoreID == info.SourceStoreID {
				sourceReplicaID = desc.ReplicaID
				replicaHeaderClass = debugRangeClassMatch
				sourceReplicaHeader = repHeader
			}
			d.Results[repHeader][info.SourceStoreID] = outputSameTitleValueWithClass(
				fmt.Sprintf("n%d s%d", desc.NodeID, desc.StoreID),
				replicaHeaderClass,
			)
		}

		raftLeader := sourceReplicaID != 0 && info.RaftState.Lead == uint64(sourceReplicaID)
		if raftLeader && info.RaftState.HardState.Term > leaderStoreInfo.RaftState.HardState.Term {
			leaderStoreInfo = info
		}
		if info.RaftState.HardState.Term > latestTermInfo.RaftState.HardState.Term {
			latestTermInfo = info
		}

		d.Results[debugRangeHeaderStore][info.SourceStoreID] =
			outputSameTitleValue(fmt.Sprintf("s%d", info.SourceStoreID))
		d.Results[debugRangeHeaderNode][info.SourceStoreID] =
			outputSameTitleValue(fmt.Sprintf("n%d", info.SourceNodeID))
		d.Results[debugRangeHeaderKeyRange][info.SourceStoreID] =
			outputSameTitleValue(fmt.Sprintf("%s %s", info.Span.StartKey, info.Span.EndKey))
		raftState := strings.ToLower(strings.TrimPrefix(info.RaftState.State, "State"))
		d.Results[debugRangeHeaderRaftState][info.SourceStoreID] = outputSameTitleValueWithClass(
			raftState,
			fmt.Sprintf("raftstate-%s", raftState),
		)
		var leaseClass string
		if info.State.Lease.Replica.ReplicaID == sourceReplicaID {
			leaseClass = debugRangeClassLeaseHolder
		} else {
			leaseClass = debugRangeClassLeaseFollower
		}
		d.Results[debugRangeHeaderLeaseHolder][info.SourceStoreID] = outputSameTitleValueWithClass(
			info.State.Lease.Replica.ReplicaID.String(),
			leaseClass,
		)
		var leaseTypeValue string
		switch info.State.Lease.Type() {
		case roachpb.LeaseNone:
			leaseTypeValue = debugRangeValueLeaseNone
		case roachpb.LeaseEpoch:
			leaseTypeValue = debugRangeValueLeaseEpoch
		case roachpb.LeaseExpiration:
			leaseTypeValue = debugRangeValueLeaseExpiration
		}
		d.Results[debugRangeHeaderLeaseType][info.SourceStoreID] = outputSameTitleValue(leaseTypeValue)
		var epoch string
		if info.State.Lease.Epoch != nil {
			epoch = strconv.FormatInt(*info.State.Lease.Epoch, 10)
		} else {
			epoch = debugRangeValueEmpty
		}
		d.Results[debugRangeHeaderLeaseEpoch][info.SourceStoreID] = outputSameTitleValue(epoch)
		start := convertTimestamp(info.State.Lease.Start)
		d.Results[debugRangeHeaderLeaseStart][info.SourceStoreID] = &debugOutput{
			Title: fmt.Sprintf("%s\n%s", start, info.State.Lease.Start),
			Value: start,
		}
		var expiration string
		if info.State.Lease.Expiration.WallTime == 0 {
			expiration = debugRangeValueEmpty
		} else {
			expiration = convertTimestamp(info.State.Lease.Expiration)
		}
		d.Results[debugRangeHeaderLeaseExpiration][info.SourceStoreID] = &debugOutput{
			Title: fmt.Sprintf("%s\n%s", expiration, info.State.Lease.Expiration),
			Value: expiration,
		}
		d.Results[debugRangeHeaderLeaseAppliedIndex][info.SourceStoreID] =
			outputSameTitleValue(strconv.FormatUint(info.State.LeaseAppliedIndex, 10))
		var raftleaderClass string
		if raftLeader {
			raftleaderClass = debugRangeClassRaftLeader
		} else {
			raftleaderClass = debugRangeClassRaftFollower
		}
		d.Results[debugRangeHeaderRaftLeader][info.SourceStoreID] = outputSameTitleValueWithClass(
			strconv.FormatUint(info.RaftState.Lead, 10),
			raftleaderClass,
		)
		var voteClass string
		if info.RaftState.HardState.Vote == uint64(sourceReplicaID) {
			voteClass = debugRangeClassRaftLeader
		} else {
			voteClass = debugRangeClassRaftFollower
		}
		d.Results[debugRangeHeaderVote][info.SourceStoreID] = outputSameTitleValueWithClass(
			strconv.FormatUint(info.RaftState.HardState.Vote, 10),
			voteClass,
		)
		d.Results[debugRangeHeaderTerm][info.SourceStoreID] =
			outputSameTitleValue(strconv.FormatUint(info.RaftState.HardState.Term, 10))
		d.Results[debugRangeHeaderApplied][info.SourceStoreID] =
			outputSameTitleValue(strconv.FormatUint(info.RaftState.Applied, 10))
		d.Results[debugRangeHeaderCommit][info.SourceStoreID] =
			outputSameTitleValue(strconv.FormatUint(info.RaftState.HardState.Commit, 10))
		d.Results[debugRangeHeaderLastIndex][info.SourceStoreID] =
			outputSameTitleValue(strconv.FormatUint(info.State.LastIndex, 10))
		d.Results[debugRangeHeaderLogSize][info.SourceStoreID] =
			outputSameTitleValue(strconv.FormatInt(info.State.RaftLogSize, 10))
		if leaseClass == debugRangeClassLeaseHolder {
			d.Results[debugRangeHeaderLeaseholderQPS][info.SourceStoreID] =
				outputSameTitleValue(strconv.FormatFloat(info.Stats.QueriesPerSecond, 'g', -1, 64))
		} else {
			d.Results[debugRangeHeaderLeaseholderQPS][info.SourceStoreID] =
				outputSameTitleValue(debugRangeValueEmpty)
		}
		d.Results[debugRangeHeaderKeysWrittenPerSecond][info.SourceStoreID] =
			outputSameTitleValue(strconv.FormatFloat(info.Stats.WritesPerSecond, 'g', -1, 64))
		if raftLeader {
			d.Results[debugRangeHeaderApproxProposalQuota][info.SourceStoreID] =
				outputSameTitleValue(strconv.FormatInt(info.State.ApproximateProposalQuota, 10))
		} else {
			d.Results[debugRangeHeaderApproxProposalQuota][info.SourceStoreID] =
				outputSameTitleValue(debugRangeValueEmpty)
		}
		var pendingCommandsClass string
		if !raftLeader && info.State.NumPending > 0 {
			pendingCommandsClass = debugRangeClassWarning
		}
		d.Results[debugRangeHeaderPendingCommands][info.SourceStoreID] = outputSameTitleValueWithClass(
			strconv.FormatUint(info.State.NumPending, 10),
			pendingCommandsClass,
		)
		var droppedCommandsClass string
		if !raftLeader && info.State.NumDropped > 0 {
			droppedCommandsClass = debugRangeClassWarning
		}
		d.Results[debugRangeHeaderDroppedCommands][info.SourceStoreID] = outputSameTitleValueWithClass(
			strconv.FormatUint(info.State.NumDropped, 10),
			droppedCommandsClass,
		)
		d.Results[debugRangeHeaderTruncatedIndex][info.SourceStoreID] =
			outputSameTitleValue(strconv.FormatUint(info.State.TruncatedState.Index, 10))
		d.Results[debugRangeHeaderTruncatedTerm][info.SourceStoreID] =
			outputSameTitleValue(strconv.FormatUint(info.State.TruncatedState.Term, 10))

		// MVCC stats.
		stats := info.State.ReplicaState.Stats
		if stats.LastUpdateNanos > 0 {
			d.Results[debugRangeHeaderMVCCLastUpdate][info.SourceStoreID] = &debugOutput{
				Title: fmt.Sprintf("%d\n%s", stats.LastUpdateNanos, time.Unix(0, stats.LastUpdateNanos)),
				Value: time.Unix(0, stats.LastUpdateNanos).String(),
			}
		} else {
			d.Results[debugRangeHeaderMVCCLastUpdate][info.SourceStoreID] = &debugOutput{
				Title: debugRangeValueEmpty,
				Value: debugRangeValueEmpty,
			}
		}
		intentAge := time.Duration(stats.IntentAge)
		d.Results[debugRangeHeaderMVCCIntentAge][info.SourceStoreID] = &debugOutput{
			Title: fmt.Sprintf("%dns", stats.IntentAge),
			Value: intentAge.String(),
		}
		gcBytesAge := time.Duration(stats.GCBytesAge)
		d.Results[debugRangeHeaderMVCCGCBytesAge][info.SourceStoreID] = &debugOutput{
			Title: fmt.Sprintf("%dns", stats.GCBytesAge),
			Value: gcBytesAge.String(),
		}
		d.Results[debugRangeHeaderMVCCLive][info.SourceStoreID] =
			convertBytes(stats.LiveBytes, stats.LiveCount)
		d.Results[debugRangeHeaderMVCCKey][info.SourceStoreID] =
			convertBytes(stats.KeyBytes, stats.KeyCount)
		d.Results[debugRangeHeaderMVCCVal][info.SourceStoreID] =
			convertBytes(stats.ValBytes, stats.ValCount)
		d.Results[debugRangeHeaderMVCCIntent][info.SourceStoreID] =
			convertBytes(stats.IntentBytes, stats.IntentCount)
		d.Results[debugRangeHeaderMVCCSys][info.SourceStoreID] =
			convertBytes(stats.SysBytes, stats.SysCount)

		// If the replica is dormant, set all classes in the store to dormant.
		if info.RaftState.State == raftStateDormant {
			for _, header := range d.HeaderKeys {
				// Don't overwrite it if it's the source replica.
				if header != sourceReplicaHeader {
					if output, ok := d.Results[header][info.SourceStoreID]; ok {
						output.Class = debugRangeClassRaftDormant
					}
				}
			}
		}
	}

	// If we have a leader use that as our most up to date info, otherwise use the
	// replica with the latest term.
	if leaderStoreInfo.SourceStoreID > 0 {
		latestTermInfo = leaderStoreInfo
	}

	// Add warnings to select headers and cells when the values don't match
	// those of the leader. This only affects non-dormant replicas.
	if leaderStoreInfo.SourceStoreID > 0 {
		leaderReplicaMap := make(map[roachpb.ReplicaID]roachpb.ReplicaDescriptor)
		for _, desc := range leaderStoreInfo.State.Desc.Replicas {
			leaderReplicaMap[desc.ReplicaID] = desc
		}
		for _, info := range d.rangeInfos {
			if info.SourceStoreID == leaderStoreInfo.SourceStoreID ||
				info.RaftState.State == raftStateDormant {
				continue
			}

			if leaderStoreInfo.Span.Equal(info.Span) {
				d.Results[debugRangeHeaderKeyRange][d.HeaderFakeStoreID].Class = debugRangeClassWarning
				d.Results[debugRangeHeaderKeyRange][info.SourceStoreID].Class = debugRangeClassWarning
			}

			for _, header := range debugRangeHeaderWarnings {
				leaderOutput := d.Results[header][leaderStoreInfo.SourceStoreID]
				currentOutput := d.Results[header][info.SourceStoreID]
				if leaderOutput.Value != currentOutput.Value &&
					leaderOutput.Title != currentOutput.Title {
					d.Results[header][d.HeaderFakeStoreID].Class = debugRangeClassWarning
					currentOutput.Class = debugRangeClassWarning
				}
			}

			// Find all replicas that the leader doesn't know about and any
			// replicas that differ from the leader's.
			foundReplicaIDs := make(map[roachpb.ReplicaID]struct{})
			for _, desc := range info.State.Desc.Replicas {
				foundReplicaIDs[desc.ReplicaID] = struct{}{}
				if leaderDesc, ok := leaderReplicaMap[desc.ReplicaID]; !ok {
					// The leader doesn't know about this replica.
					repHeader := replicaHeader(desc.ReplicaID)
					d.Results[repHeader][d.HeaderFakeStoreID].Class = debugRangeClassWarning
					d.Results[repHeader][info.SourceStoreID].Class = debugRangeClassMissing
				} else if !leaderDesc.Equal(desc) {
					// The leader's version of this replica is different.
					repHeader := replicaHeader(desc.ReplicaID)
					d.Results[repHeader][d.HeaderFakeStoreID].Class = debugRangeClassWarning
					d.Results[repHeader][info.SourceStoreID].Class = debugRangeClassWarning
				}
			}

			// Find all replicas that this store doesn't know about that it
			// should.
			for repID := range leaderReplicaMap {
				if _, ok := foundReplicaIDs[repID]; !ok {
					repHeader := replicaHeader(repID)
					d.Results[repHeader][d.HeaderFakeStoreID].Class = debugRangeClassWarning
					d.Results[repHeader][info.SourceStoreID].Class = debugRangeClassWarning
				}
			}
		}
	}

	floorMilliseconds := func(d time.Duration) time.Duration {
		return time.Duration(d.Nanoseconds() - (d.Nanoseconds() % time.Millisecond.Nanoseconds()))
	}

	// Reverse order for display purposes.
	for i := len(latestTermInfo.LeaseHistory) - 1; i >= 0; i-- {
		lease := latestTermInfo.LeaseHistory[i]
		if lease.ProposedTS == nil {
			failedInfo := latestTermInfo
			failedInfo.ErrorMessage = fmt.Sprintf("Lease has a nil proposedTS: %+v", lease)
			d.Failures = append(d.Failures, failedInfo)
			continue
		}
		if i == len(latestTermInfo.LeaseHistory)-1 {
			if lease.Epoch != nil {
				d.LeaseEpoch = true
			}
		}
		if d.LeaseEpoch && lease.Epoch == nil {
			failedInfo := latestTermInfo
			failedInfo.ErrorMessage = fmt.Sprintf("Lease has a nil epoch: %+v", lease)
			d.Failures = append(d.Failures, failedInfo)
			continue
		}
		var detail debugLeaseDetail
		detail.Replica.Value = fmt.Sprintf("n%d s%d r%d/%d",
			lease.Replica.NodeID, lease.Replica.StoreID, d.RangeID, lease.Replica.ReplicaID,
		)
		detail.Replica.Title = detail.Replica.Value

		if d.LeaseEpoch {
			detail.Epoch.Value = fmt.Sprintf("n%d, %d", lease.Replica.NodeID, *lease.Epoch)
			detail.Epoch.Title = detail.Epoch.Value
		} else {
			detail.Expiration.Title = fmt.Sprintf("%s\n%s",
				convertTimestamp(lease.Expiration), lease.Expiration,
			)
			detail.Expiration.Value =
				floorMilliseconds(lease.Expiration.GoTime().Sub(lease.ProposedTS.GoTime())).String()
		}

		if lease.Start.WallTime != 0 {
			start := convertTimestamp(lease.Start)
			detail.Start.Title = fmt.Sprintf("%s\n%s", start, lease.Start)
			detail.Start.Value = start
		} else {
			detail.Start.Title = debugRangeValueEmpty
			detail.Start.Value = debugRangeValueEmpty
		}

		if lease.ProposedTS.WallTime != 0 {
			proposed := convertTimestamp(*lease.ProposedTS)
			detail.ProposedTS.Title = fmt.Sprintf("%s\n%s", proposed, lease.ProposedTS)
			detail.ProposedTS.Value = proposed
		} else {
			detail.ProposedTS.Title = debugRangeValueEmpty
			detail.ProposedTS.Value = debugRangeValueEmpty
		}

		if i > 0 {
			prevLease := latestTermInfo.LeaseHistory[i-1]
			if prevLease.ProposedTS != nil && prevLease.ProposedTS.WallTime != 0 {
				detail.ProposedTSDelta.Title =
					floorMilliseconds(lease.ProposedTS.GoTime().Sub(prevLease.ProposedTS.GoTime())).String()
			} else {
				detail.ProposedTSDelta.Title = debugRangeValueEmpty
			}
			detail.ProposedTSDelta.Value = detail.ProposedTSDelta.Title

			if prevLease.Start.WallTime != 0 {
				detail.StartDelta.Title =
					floorMilliseconds(lease.Start.GoTime().Sub(prevLease.Start.GoTime())).String()
			} else {
				detail.StartDelta.Title = debugRangeValueEmpty
			}
			detail.StartDelta.Value = detail.StartDelta.Title
		}

		d.LeaseHistory = append(d.LeaseHistory, detail)
	}

	if d.rangeLogResp != nil {
		for _, event := range d.rangeLogResp.Events {
			var otherRangeID debugOutput
			if event.OtherRangeID != 0 {
				otherRangeID = debugOutput{
					Value: event.OtherRangeID.String(),
					Title: fmt.Sprintf("r%d", event.OtherRangeID),
				}
			}
			var rowClass string
			if int64(event.RangeID) != d.RangeID {
				rowClass = debugRangeClassOtherRange
			}
			var infoBuffer bytes.Buffer
			if event.Info.UpdatedDesc != nil {
				infoBuffer.WriteString("Updated Range Descriptor: ")
				infoBuffer.WriteString(event.Info.UpdatedDesc.String())
				infoBuffer.WriteRune('\n')
			}
			if event.Info.NewDesc != nil {
				infoBuffer.WriteString("New Range Descriptor: ")
				infoBuffer.WriteString(event.Info.NewDesc.String())
				infoBuffer.WriteRune('\n')
			}
			if event.Info.AddedReplica != nil {
				infoBuffer.WriteString("Added Replica: ")
				infoBuffer.WriteString(event.Info.AddedReplica.String())
				infoBuffer.WriteRune('\n')
			}
			if event.Info.RemovedReplica != nil {
				infoBuffer.WriteString("Removed Replica: ")
				infoBuffer.WriteString(event.Info.RemovedReplica.String())
				infoBuffer.WriteRune('\n')
			}

			d.RangeLog = append(d.RangeLog, debugRangeLogEvent{
				Timestamp: *outputSameTitleValue(event.Timestamp.String()),
				EventType: *outputSameTitleValue(event.EventType.String()),
				RangeID: debugOutput{
					Value: event.RangeID.String(),
					Title: fmt.Sprintf("r%d", event.RangeID),
				},
				OtherRangeID: otherRangeID,
				StoreID:      *outputSameTitleValue(fmt.Sprintf("s%d", event.StoreID)),
				Info:         *outputSameTitleValue(infoBuffer.String()),
				RowClass:     rowClass,
			})
		}
	}

	sort.Sort(d.StoreIDs)
	sort.Sort(d.Failures)
}

const debugRangeTemplate = `
<!DOCTYPE html>
<HTML>
  <HEAD>
  	<META CHARSET="UTF-8"/>
    <TITLE>Range ID:{{.RangeID}}</TITLE>
    <STYLE>
      body {
        font-family: "Helvetica Neue", Helvetica, Arial;
        font-size: 14px;
        line-height: 20px;
        font-weight: 400;
        color: #3b3b3b;
        -webkit-font-smoothing: antialiased;
        font-smoothing: antialiased;
        background: #e4e4e4;
      }
      .wrapper {
        margin: 0 auto;
        padding: 0 40px;
      }
      .table {
        margin: 0 0 40px 0;
        display: table;
        width: 100%;
      }
      .row {
        display: table-row;
        background: #f6f6f6;
      }
      .cell:nth-of-type(odd) {
        background: #e9e9e9;
      }
      .cell {
        padding: 6px 12px;
        display: table-cell;
        height: 20px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        max-width: 200px;
        border-width: 1px 1px 0 0;
        border-color: rgba(0, 0, 0, 0.1);
        border-style: solid;
      }
      .header.cell{
        font-weight: 900;
        color: #ffffff;
        background: #2980b9;
        text-overflow: clip;
        border: none;
        width: 1px;
        text-align: right;
      }
      .header.cell.warning {
        color: yellow;
      }
      .cell.warning {
        color: red;
      }
      .cell.match {
        color: green;
      }
      .cell.missing {
        color: orange;
      }
      .cell.raftstate-leader {
        color: green;
      }
      .cell.raftstate-follower {
        color: blue;
      }
      .cell.raftstate-candidate {
        color: orange;
      }
      .cell.raftstate-precandidate {
        color: darkorange;
      }
      .cell.raftstate-dormant {
        color: gray;
      }
      .cell.lease-holder {
        color: green;
      }
      .cell.lease-follower {
        color: blue;
      }
      .failure-table {
        margin: 0 0 40px 0;
        display: table;
        width: 100%;
      }
      .failure-row {
        display: table-row;
        background: #f6f6f6;
      }
      .failure-row:nth-of-type(odd) {
        background: #e9e9e9;
      }
      .failure-cell {
        padding: 6px 12px;
        display: table-cell;
        height: 20px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        border-width: 1px 1px 0 0;
        border-color: rgba(0, 0, 0, 0.1);
        border-style: solid;
        max-width: 500px;
      }
      .failure-row:first-of-type .failure-cell {
        font-weight: 900;
        color: #ffffff;
        background: #ea6153;
        border: none;
      }
      .failure-cell.small {
          max-width: 1px;
      }
      .lease-table {
        margin: 0 0 40px 0;
        display: table;
      }
      .lease-row {
        display: table-row;
        background: #f6f6f6;
      }
      .lease-row:nth-of-type(odd) {
        background: #e9e9e9;
      }
      .lease-cell {
        padding: 6px 12px;
        display: table-cell;
        height: 20px;
        overflow: hidden;
        text-overflow: ellipsis;
        border-width: 1px 1px 0 0;
        border-color: rgba(0, 0, 0, 0.1);
        border-style: solid;
      }
      .lease-row:first-of-type .lease-cell {
        font-weight: 900;
        color: #ffffff;
        background: #3d9970;
        border: none;
      }
      .log-table {
        margin: 0 0 40px 0;
        display: table;
        width: 100%;
      }
      .log-row {
        display: table-row;
        background: #f6f6f6;
      }
      .log-row:nth-of-type(odd) {
        background: #e9e9e9;
      }
      .log-row.other-range {
        background: #fffee7;
      }
      .log-cell {
        padding: 6px 12px;
        display: table-cell;
        height: 20px;
        overflow: hidden;
        text-overflow: ellipsis;
        border-width: 1px 1px 0 0;
        border-color: rgba(0, 0, 0, 0.1);
        border-style: solid;
        white-space: pre;
      }
      .log-row:first-of-type .log-cell {
        font-weight: 900;
        color: #ffffff;
        background: #FF7F00;
        border: none;
      }
      .log-cell.small {
        white-space: nowrap;
      }
    </STYLE>
  </HEAD>
  <BODY>
    <DIV CLASS="wrapper">
      <H1>Range r{{$.RangeID}}</H1>
      {{- if $.Failures}}
        <H2>Failures</H2>
        <DIV CLASS="failure-table">
          <DIV CLASS="failure-row">
            <DIV CLASS="failure-cell small">Node</DIV>
            <DIV CLASS="failure-cell small">Store</DIV>
            <DIV CLASS="failure-cell">Error</DIV>
          </DIV>
          {{- range $_, $det := $.Failures}}
            <DIV CLASS="failure-row">
              <DIV CLASS="failure-cell small">n{{$det.SourceNodeID}}</DIV>
              {{- if not (eq $det.SourceStoreID 0)}}
                <DIV CLASS="failure-cell small">n{{$det.SourceStoreID}}</DIV>
              {{- else -}}
                <DIV CLASS="failure-cell">-</DIV>
              {{- end}}
              <DIV CLASS="failure-cell" TITLE="{{$det.ErrorMessage}}">{{$det.ErrorMessage}}</DIV>
            </DIV>
          {{- end}}
        </DIV>
      {{- end}}
      {{- if $.ReplicaIDs}}
        <DIV CLASS="table">
          {{- range $_, $headerName := $.HeaderKeys}}
            {{- $data := index $.Results $headerName}}
            {{- $datum := index $data $.HeaderFakeStoreID}}
            <DIV CLASS="row">
              <DIV CLASS="header cell {{$datum.Class}}" TITLE="{{$datum.Title}}">{{$datum.Value}}</DIV>
              {{- range $_, $storeID := $.StoreIDs}}
                {{- $datum := index $data $storeID}}
                <DIV CLASS="cell {{$datum.Class}}" TITLE="{{$datum.Title}}">{{$datum.Value}}</DIV>
              {{- end}}
            </DIV>
          {{- end}}
        </DIV>
      {{- else}}
        <p>No information available for Range r{{$.RangeID}}</p>
      {{- end}}
      {{- if $.LeaseHistory}}
        <H2>Lease History</H2>
        <DIV CLASS="lease-table">
          <DIV CLASS="lease-row">
            <DIV CLASS="lease-cell">Replica</DIV>
            {{- if $.LeaseEpoch }}
              <DIV CLASS="lease-cell">Epoch</DIV>
            {{- end}}
            <DIV CLASS="lease-cell">Proposed</DIV>
            <DIV CLASS="lease-cell">Proposed Delta</DIV>
            {{- if not $.LeaseEpoch }}
              <DIV CLASS="lease-cell">Expiration</DIV>
            {{- end}}
            <DIV CLASS="lease-cell">Start</DIV>
            <DIV CLASS="lease-cell">Start Delta</DIV>
          </DIV>
          {{- range $_, $lease := $.LeaseHistory}}
            <DIV CLASS="lease-row">
              <DIV CLASS="lease-cell" TITLE="{{$lease.Replica.Title}}">{{$lease.Replica.Value}}</DIV>
              {{- if $.LeaseEpoch }}
                <DIV CLASS="lease-cell" TITLE="{{$lease.Epoch.Title}}">{{$lease.Epoch.Value}}</DIV>
              {{- end}}
              <DIV CLASS="lease-cell" TITLE="{{$lease.ProposedTS.Title}}">{{$lease.ProposedTS.Value}}</DIV>
              <DIV CLASS="lease-cell" TITLE="{{$lease.ProposedTSDelta.Title}}">{{$lease.ProposedTSDelta.Value}}</DIV>
              {{- if not $.LeaseEpoch }}
                <DIV CLASS="lease-cell" TITLE="{{$lease.Expiration.Title}}">{{$lease.Expiration.Value}}</DIV>
              {{- end}}
              <DIV CLASS="lease-cell" TITLE="{{$lease.Start.Title}}">{{$lease.Start.Value}}</DIV>
              <DIV CLASS="lease-cell" TITLE="{{$lease.StartDelta.Title}}">{{$lease.StartDelta.Value}}</DIV>
            </DIV>
          {{- end}}
        </DIV>
      {{- end}}
      {{- if $.RangeLog}}
        <H2>Range Log</H2>
        <DIV CLASS="log-table">
          <DIV CLASS="log-row">
            <DIV CLASS="log-cell small">Timestamp</DIV>
            <DIV CLASS="log-cell small">Store</DIV>
            <DIV CLASS="log-cell small">Event Type</DIV>
            <DIV CLASS="log-cell small">Range</DIV>
            <DIV CLASS="log-cell small">Other Range</DIV>
            <DIV CLASS="log-cell">Info</DIV>
          </DIV>
          {{- range $_, $log := $.RangeLog}}
            <DIV CLASS="log-row {{ $log.RowClass }}">
              <DIV CLASS="log-cell small" TITLE="{{$log.Timestamp.Title}}">{{$log.Timestamp.Value}}</DIV>
              <DIV CLASS="log-cell small" TITLE="{{$log.StoreID.Title}}">{{$log.StoreID.Value}}</DIV>
              <DIV CLASS="log-cell small" TITLE="{{$log.EventType.Title}}">{{$log.EventType.Value}}</DIV>
              <DIV CLASS="log-cell small" TITLE="{{$log.RangeID.Title}}">
                <a href="/debug/range?id={{$log.RangeID.Value}}">{{$log.RangeID.Title}}</a>
              </DIV>
              <DIV CLASS="log-cell small" TITLE="{{$log.OtherRangeID.Title}}">
                {{- if $log.OtherRangeID.Value}}
                  <a href="/debug/range?id={{$log.OtherRangeID.Value}}">{{$log.OtherRangeID.Title}}</a>
                {{- end}}
              </DIV>
              <DIV CLASS="log-cell" TITLE="{{$log.Info.Title}}">{{$log.Info.Value}}</DIV>
            </DIV>
          {{- end}}
        </DIV>
      {{- end}}
    </DIV>
  </BODY>
</HTML>
`
