// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// simplePaginate takes in an input slice, and returns a sub-slice of the next
// `limit` elements starting at `offset`. The second returned value is the
// next offset that can be used to return the next "limit" results, or
// 0 if there are no more results. The choice of a 0 return value for next
// in cases where input has been exhausted, helps when it's being returned
// back to the client as a `json:omitempty` field, as the JSON mashal code will
// simply ignore the field if it's a zero value.
func simplePaginate[T any](input []T, limit, offset int) (result []T, next int) {
	if limit <= 0 {
		return input, 0
	} else if offset < 0 {
		offset = 0
	}
	startIdx := offset
	endIdx := offset + limit
	if startIdx > len(input) {
		startIdx = len(input)
	}
	if endIdx > len(input) {
		endIdx = len(input)
	}
	next = endIdx
	if endIdx == len(input) {
		next = 0
	}
	return input[startIdx:endIdx], next
}

// paginationState represents the current state of pagination through the result
// set of an RPC-based endpoint. Meant for use with rpcNodePaginator, which
// implements most of the pagination logic.
type paginationState struct {
	nodesQueried    []roachpb.NodeID
	inProgress      roachpb.NodeID
	inProgressIndex int
	nodesToQuery    []roachpb.NodeID
}

// mergeNodeIDs merges allNodeIDs with all node IDs in the paginationState;
// adding any nodes to the end of p.nodesToQuery that don't already exist in p.
// allNodeIDs must be a sorted slice of all currently-live nodes.
func (p *paginationState) mergeNodeIDs(allNodeIDs []roachpb.NodeID) {
	sortedNodeIDs := make([]roachpb.NodeID, 0, len(p.nodesQueried)+1+len(p.nodesToQuery))
	sortedNodeIDs = append(sortedNodeIDs, p.nodesQueried...)
	if p.inProgress != 0 {
		sortedNodeIDs = append(sortedNodeIDs, p.inProgress)
	}
	sortedNodeIDs = append(sortedNodeIDs, p.nodesToQuery...)
	sort.Slice(sortedNodeIDs, func(i, j int) bool {
		return sortedNodeIDs[i] < sortedNodeIDs[j]
	})
	// As both sortedNodeIDs and allNodeIDs are sorted by node ID, and we just
	// need to add (to p.nodesToQuery) values in allNodeIDs that are *not* in
	// sortedNodeIDs, we can do this merge by iterating through both slices at the
	// same time. j is the index for sortedNodeIDs.
	j := 0
	for i := range allNodeIDs {
		// Ratchet j forward to the same ID as allNodeIDs[i].
		for j < len(sortedNodeIDs) && sortedNodeIDs[j] < allNodeIDs[i] {
			j++
		}
		// If allNodeIDs[i] is not in sortedNodeIDs, add it to p.nodesToQuery.
		if j >= len(sortedNodeIDs) || sortedNodeIDs[j] != allNodeIDs[i] {
			p.nodesToQuery = append(p.nodesToQuery, allNodeIDs[i])
		}
	}
	if p.inProgress == 0 && len(p.nodesToQuery) > 0 {
		p.inProgress = p.nodesToQuery[0]
		p.inProgressIndex = 0
		p.nodesToQuery = p.nodesToQuery[1:]
	}
}

// paginate processes the response from a given node, and returns start/end
// indices that the response should be sliced at (if it is a slice; otherwise
// inclusion/exclusion is denoted by end > start). Note that this method
// expects that it is called serially, with nodeIDs in the same order as
// p.nodesToQuery (nodes skipped in that slice are considered to have returned
// an error; out-of-order nodeIDs generate a panic).
func (p *paginationState) paginate(
	limit int, nodeID roachpb.NodeID, length int,
) (start, end, newLimit int, err error) {
	if limit <= 0 || nodeID == 0 || int(p.inProgress) == 0 {
		// Already reached limit.
		return 0, 0, 0, nil
	}
	if p.inProgress != nodeID {
		p.nodesQueried = append(p.nodesQueried, p.inProgress)
		p.inProgress = 0
		p.inProgressIndex = 0
		for i := range p.nodesToQuery {
			if p.nodesToQuery[i] == nodeID {
				// Deducing from the caller contract, all the nodes in
				// p.nodesToQuery[0:i] must have returned errors.
				p.inProgress = nodeID
				p.nodesQueried = append(p.nodesQueried, p.nodesToQuery[0:i]...)
				p.nodesToQuery = p.nodesToQuery[i+1:]
				break
			}
		}
		if p.inProgress == 0 {
			// This node isn't in list. This should never happen.
			return 0, 0, 0, errors.Errorf("could not find node %d in pagination state %v", nodeID, p)
		}
	}
	doneWithNode := false
	if length > 0 {
		start = p.inProgressIndex
		if start > length {
			start = length
		}
		// end = min(length, start + limit)
		if start+limit >= length {
			end = length
			doneWithNode = true
		} else {
			end = start + limit
		}
		limit -= end - start
		p.inProgressIndex = end
	}
	if doneWithNode {
		p.nodesQueried = append(p.nodesQueried, nodeID)
		p.inProgressIndex = 0
		if len(p.nodesToQuery) > 0 {
			p.inProgress = p.nodesToQuery[0]
			p.nodesToQuery = p.nodesToQuery[1:]
		} else {
			p.nodesToQuery = p.nodesToQuery[:0]
			p.inProgress = 0
		}
	}
	return start, end, limit, nil
}

// UnmarshalText takes a URL-friendly base64-encoded version of a continuation/
// next token (likely coming from a user HTTP request), and unmarshals it to a
// paginationState. The format is:
//
// <nodesQueried>|<inProgressNode>|<inProgressNodeIndex>|<nodesToQuery>
//
// Where:
//   - nodesQueried is a comma-separated list of node IDs that have already been
//     queried (matching p.nodesQueried).
//   - inProgressNode is the ID of the node where the cursor is currently at.
//   - inProgressNodeIndex is the index of the response from inProgressNode's
//     node-local function where the cursor is currently at.
//   - nodesToQuery is a comma-separated list of node IDs of nodes that are yet
//     to be queried.
//
// All node IDs and indices are represented as unsigned 32-bit ints, and
// comma-separated lists are allowed to have trailing commas. The character
// separating all of the above components is the pipe (|) character.
func (p *paginationState) UnmarshalText(text []byte) error {
	decoder := base64.NewDecoder(base64.URLEncoding, bytes.NewReader(text))
	var decodedText []byte
	var err error
	if decodedText, err = io.ReadAll(decoder); err != nil {
		return err
	}
	parts := strings.Split(string(decodedText), "|")
	if len(parts) != 4 {
		return errors.New("invalid pagination state")
	}
	parseNodeIDSlice := func(str string) ([]roachpb.NodeID, error) {
		nodeIDs := strings.Split(str, ",")
		res := make([]roachpb.NodeID, 0, len(nodeIDs))
		for _, part := range nodeIDs {
			// Trim space and check for length. This is because comma-separated nodeID
			// lists are allowed to have trailing commas at the end.
			part = strings.TrimSpace(part)
			if len(part) == 0 {
				continue
			}
			val, err := strconv.ParseUint(part, 10, 32)
			if err != nil {
				return nil, errors.Wrap(err, "invalid pagination state")
			}
			if val <= 0 {
				return nil, errors.New("expected positive nodeID in pagination token")
			}
			res = append(res, roachpb.NodeID(val))
		}
		return res, nil
	}
	p.nodesQueried, err = parseNodeIDSlice(parts[0])
	if err != nil {
		return err
	}
	var inProgressInt int
	inProgressInt, err = strconv.Atoi(parts[1])
	if err != nil {
		return errors.Wrap(err, "invalid pagination state")
	}
	p.inProgress = roachpb.NodeID(inProgressInt)
	p.inProgressIndex, err = strconv.Atoi(parts[2])
	if err != nil {
		return errors.Wrap(err, "invalid pagination state")
	}
	if p.inProgressIndex < 0 || (p.inProgressIndex > 0 && p.inProgress <= 0) {
		return errors.Newf("invalid pagination resumption token: (%d, %d)", p.inProgress, p.inProgressIndex)
	}
	p.nodesToQuery, err = parseNodeIDSlice(parts[3])
	if err != nil {
		return err
	}
	return nil
}

// MarshalText converts the current paginationState to an ascii text
// representation that can be sent back to the user as a next/continuation
// token. For format, see the comment on UnmarshalText.
func (p *paginationState) MarshalText() (text []byte, err error) {
	var builder, builder2 bytes.Buffer
	for _, nid := range p.nodesQueried {
		fmt.Fprintf(&builder, "%d,", nid)
	}
	fmt.Fprintf(&builder, "|%d|%d|", p.inProgress, p.inProgressIndex)
	for _, nid := range p.nodesToQuery {
		fmt.Fprintf(&builder, "%d,", nid)
	}
	encoder := base64.NewEncoder(base64.URLEncoding, &builder2)
	if _, err = encoder.Write(builder.Bytes()); err != nil {
		return nil, err
	}
	if err = encoder.Close(); err != nil {
		return nil, err
	}
	return builder2.Bytes(), nil
}

// paginatedNodeResponse stores the response from one node in a paginated fan-out
// request. For use with rpcNodePaginator.
type paginatedNodeResponse[T any] struct {
	nodeID   roachpb.NodeID
	response []T
	err      error
}

// rpcNodePaginator allows for concurrent fan-out RPC requests to be made to
// multiple nodes, and their responses ordered back in the same ordering as
// that in pagState, and with responses limit-ed to the specified limit. Uses
// reflection to limit the response in the responseFn if it's a slice, and
// treats it as an item of length 1 if it's not a slice.
//
// To use rpcNodePaginator, ensure that dialFn returns a usable node dialer,
// and that nodeFn returns a response that's a stable-sorted slice or a single
// value or nil. Stable-sorted in this context means that two successive calls
// to nodeFn should have the same ordering of any elements that exist in both
// result slices.
//
// This struct is mostly meant for use in statusServer.paginatedIterateNodes.
// It has the advantage of allowing for parallel fan-out node RPC requests to
// only the subset of nodes that are likely to contain the next `limit` results.
// Nodes already queried on past calls from the same user (according to
// pagState) are not ignored. The goroutine that calls processResponses handles
// slice truncation and response ordering.
type rpcNodePaginator[Client, Result any] struct {
	limit        int
	numNodes     int
	errorCtx     redact.RedactableString
	pagState     paginationState
	responseChan chan paginatedNodeResponse[Result]
	nodeStatuses map[serverID]livenesspb.NodeLivenessStatus

	dialFn     func(ctx context.Context, id roachpb.NodeID) (client Client, err error)
	nodeFn     func(ctx context.Context, client Client, nodeID roachpb.NodeID) ([]Result, error)
	responseFn func(nodeID roachpb.NodeID, res []Result)
	errorFn    func(nodeID roachpb.NodeID, nodeFnError error)

	mu struct {
		syncutil.Mutex

		turnCond sync.Cond

		currentIdx, currentLen int
	}

	// Stores a 1 if the limit has been reached. Must be accessed and updated
	// atomically.
	done int32
}

func (r *rpcNodePaginator[Client, Result]) init() {
	r.mu.turnCond.L = &r.mu
	r.responseChan = make(chan paginatedNodeResponse[Result], r.numNodes)
}

const noTimeout time.Duration = 0

// queryNode queries the given node, and sends the responses back through responseChan
// in order of idx (i.e. when all nodes with a lower idx have already sent theirs).
// Safe for concurrent use.
func (r *rpcNodePaginator[Client, Result]) queryNode(
	ctx context.Context, nodeID roachpb.NodeID, idx int, timeout time.Duration,
) {
	if atomic.LoadInt32(&r.done) != 0 {
		// There are more values than we need. currentLen >= limit.
		return
	}
	var client Client
	addNodeResp := func(resp paginatedNodeResponse[Result]) {
		r.mu.Lock()
		defer r.mu.Unlock()

		for r.mu.currentIdx < idx && atomic.LoadInt32(&r.done) == 0 {
			r.mu.turnCond.Wait()
			select {
			case <-ctx.Done():
				r.mu.turnCond.Broadcast()
				return
			default:
			}
		}
		if atomic.LoadInt32(&r.done) != 0 {
			// There are more values than we need. currentLen >= limit.
			r.mu.turnCond.Broadcast()
			return
		}
		r.responseChan <- resp
		r.mu.currentLen += len(resp.response)
		if nodeID == r.pagState.inProgress {
			// We're resuming partway through a node's response. Subtract away the
			// count of values already sent in previous calls (i.e. inProgressIndex).
			if len(resp.response) > r.pagState.inProgressIndex {
				r.mu.currentLen -= r.pagState.inProgressIndex
			} else {
				r.mu.currentLen -= len(resp.response)
			}
		}
		if r.mu.currentLen >= r.limit {
			atomic.StoreInt32(&r.done, 1)
			close(r.responseChan)
		}
		r.mu.currentIdx++
		r.mu.turnCond.Broadcast()
	}
	if err := timeutil.RunWithTimeout(ctx, "dial node", base.DialTimeout, func(ctx context.Context) error {
		var err error
		client, err = r.dialFn(ctx, nodeID)
		return err
	}); err != nil {
		err = errors.Wrapf(err, "failed to dial into node %d (%s)",
			nodeID, r.nodeStatuses[serverID(nodeID)])
		addNodeResp(paginatedNodeResponse[Result]{nodeID: nodeID, err: err})
		return
	}

	var res []Result
	var err error
	if timeout == noTimeout {
		res, err = r.nodeFn(ctx, client, nodeID)
	} else {
		err = timeutil.RunWithTimeout(ctx, "node fn", timeout, func(ctx context.Context) error {
			var _err error
			res, _err = r.nodeFn(ctx, client, nodeID)
			return _err
		})
	}

	if err != nil {
		err = errors.Wrapf(err, "error requesting %s from node %d (%s)",
			r.errorCtx, nodeID, r.nodeStatuses[serverID(nodeID)])
	}
	addNodeResp(paginatedNodeResponse[Result]{nodeID: nodeID, response: res, err: err})
}

// processResponses processes the responses returned into responseChan. Must only
// be called once.
func (r *rpcNodePaginator[Client, Result]) processResponses(
	ctx context.Context,
) (next paginationState, err error) {
	// Copy r.pagState, as concurrent invocations of queryNode expect it to not
	// change.
	next = r.pagState
	limit := r.limit
	numNodes := r.numNodes
	for numNodes > 0 {
		select {
		case res, ok := <-r.responseChan:
			if res.err != nil {
				r.errorFn(res.nodeID, res.err)
			} else {
				start, end, newLimit, err2 := next.paginate(limit, res.nodeID, len(res.response))
				if err2 != nil {
					r.errorFn(res.nodeID, err2)
					// Break out of select, resume loop.
					break
				}
				r.responseFn(res.nodeID, res.response[start:end])
				limit = newLimit
			}
			if !ok {
				return next, err
			}
		case <-ctx.Done():
			err = errors.Errorf("request of %s canceled before completion", r.errorCtx)
			return next, err
		}
		numNodes--
	}
	return next, err
}

// getRPCPaginationValues parses RPC pagination related values out of the query
// string of a Request. Meant for use with rpcNodePaginator.
func getRPCPaginationValues(r *http.Request) (limit int, start paginationState) {
	var err error
	if limit, err = strconv.Atoi(r.URL.Query().Get("limit")); err != nil || limit <= 0 {
		return 0, paginationState{}
	}
	if err = start.UnmarshalText([]byte(r.URL.Query().Get("start"))); err != nil {
		return limit, paginationState{}
	}
	return limit, start
}

// getSimplePaginationValues parses offset-based pagination related values out
// of the query string of a Request. Meant for use with simplePaginate.
func getSimplePaginationValues(r *http.Request) (limit, offset int) {
	var err error
	if limit, err = strconv.Atoi(r.URL.Query().Get("limit")); err != nil || limit <= 0 {
		return 0, 0
	}
	if offset, err = strconv.Atoi(r.URL.Query().Get("offset")); err != nil || offset < 0 {
		return limit, 0
	}
	return limit, offset
}
