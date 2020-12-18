// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/errors"
)

// simplePaginate takes in an input slice, and returns a sub-slice of the next
// `limit` elements starting at `offset`. The second returned value is the
// next offset that can be used to return the next "limit" results, or
// len(result) if there are no more results.
func simplePaginate(input interface{}, limit, offset int) (result interface{}, next int) {
	val := reflect.ValueOf(input)
	if limit <= 0 || val.Kind() != reflect.Slice {
		return val.Interface(), offset
	}
	startIdx := offset
	endIdx := offset + limit
	if startIdx > val.Len() {
		startIdx = val.Len()
	}
	if endIdx > val.Len() {
		endIdx = val.Len()
	}
	return val.Slice(startIdx, endIdx).Interface(), endIdx
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

// mergeNodeIDs merges sortedNodeIDs with all node IDs in the paginationState;
// adding any nodes to the end of p.nodesToQuery that don't already exist in p.
// sortedNodeIDs must be a sorted slice of all currently-live nodes.
func (p *paginationState) mergeNodeIDs(sortedNodeIDs []roachpb.NodeID) {
	allNodeIDs := make([]roachpb.NodeID, 0, len(p.nodesQueried) + 1 + len(p.nodesToQuery))
	allNodeIDs = append(allNodeIDs, p.nodesQueried...)
	if p.inProgress != 0 {
		allNodeIDs = append(allNodeIDs, p.inProgress)
	}
	allNodeIDs = append(allNodeIDs, p.nodesToQuery...)
	sort.Slice(allNodeIDs, func(i, j int) bool {
		return allNodeIDs[i] < allNodeIDs[j]
	})
	j := 0
	for i := range sortedNodeIDs {
		for j < len(allNodeIDs) && allNodeIDs[j] < sortedNodeIDs[i] {
			j++
		}
		if j >= len(allNodeIDs) || allNodeIDs[j] != sortedNodeIDs[i] {
			p.nodesToQuery = append(p.nodesToQuery, sortedNodeIDs[i])
		}
	}
	if p.inProgress == 0 && len(p.nodesToQuery) > 0 {
		p.inProgress = p.nodesToQuery[0]
		p.inProgressIndex = 0
		p.nodesToQuery = p.nodesToQuery[1:]
	}
}

func (p *paginationState) UnmarshalText(text []byte) error {
	decoder := base64.NewDecoder(base64.URLEncoding, bytes.NewReader(text))
	var decodedText []byte
	var err error
	if decodedText, err = ioutil.ReadAll(decoder); err != nil {
		return err
	}
	parts := strings.Split(string(decodedText), "|")
	if len(parts) != 4 {
		return errors.New("invalid pagination state")
	}
	parseNodeIDSlice := func(str string) ([]roachpb.NodeID, error) {
		parts := strings.Split(str, ",")
		res := make([]roachpb.NodeID, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if len(part) == 0 {
				continue
			}
			val, err := strconv.Atoi(part)
			if err != nil {
				return nil, errors.Wrap(err, "invalid pagination state")
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
	p.nodesToQuery, err = parseNodeIDSlice(parts[3])
	if err != nil {
		return err
	}
	return nil
}

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
type paginatedNodeResponse struct {
	nodeID   roachpb.NodeID
	response interface{}
	value    reflect.Value
	len      int
	err      error
}

// rpcNodePaginator allows for concurrent requests
type rpcNodePaginator struct {
	limit        int
	numNodes     int
	errorCtx     string
	pagState     paginationState
	responseChan chan paginatedNodeResponse
	nodeStatuses map[roachpb.NodeID]nodeStatusWithLiveness

	dialFn func(ctx context.Context, id roachpb.NodeID) (client interface{}, err error)
	nodeFn func(ctx context.Context, client interface{}, nodeID roachpb.NodeID) (res interface{}, err error)
	responseFn func(nodeID roachpb.NodeID, resp interface{})
	errorFn func(nodeID roachpb.NodeID, nodeFnError error)

	mu struct {
		sync.Mutex

		turnCond sync.Cond

		currentIdx, currentLen int
	}

	// Stores a 1 if the limit has been reached. Must be accessed and updated
	// atomically.
	done int32
}

func (r *rpcNodePaginator) init() {
	r.mu.turnCond.L = &r.mu
	r.responseChan = make(chan paginatedNodeResponse, r.numNodes)
}

// queryNode queries the given node, and sends the responses back through responseChan
// in order of idx (i.e. when all nodes with a lower idx have already sent theirs).
// Safe for concurrent use.
func (r *rpcNodePaginator) queryNode(ctx context.Context, nodeID roachpb.NodeID, idx int) {
	if atomic.LoadInt32(&r.done) != 0 {
		// There are more values than we need. currentLen >= limit.
		return
	}
	var client interface{}
	addNodeResp := func(resp paginatedNodeResponse) {
		r.mu.Lock()
		defer r.mu.Unlock()

		for r.mu.currentIdx < idx {
			r.mu.turnCond.Wait()
		}
		if atomic.LoadInt32(&r.done) != 0 {
			// There are more values than we need. currentLen >= limit.
			r.mu.currentIdx++
			r.mu.turnCond.Broadcast()
			return
		}
		r.responseChan <- resp
		r.mu.currentLen += resp.len
		if nodeID == r.pagState.inProgress {
			// Reduce currentLen by value already sent in previous call.
			r.mu.currentLen -= r.pagState.inProgressIndex
		}
		if r.mu.currentLen >= r.limit {
			atomic.StoreInt32(&r.done, 1)
			close(r.responseChan)
		}
		r.mu.currentIdx++
		r.mu.turnCond.Broadcast()
	}
	err := contextutil.RunWithTimeout(ctx, "dial node", base.NetworkTimeout, func(ctx context.Context) error {
		var err error
		client, err = r.dialFn(ctx, nodeID)
		return err
	})
	if err != nil {
		err = errors.Wrapf(err, "failed to dial into node %d (%s)",
			nodeID, r.nodeStatuses[nodeID].livenessStatus)
		addNodeResp(paginatedNodeResponse{nodeID: nodeID, err: err})
		return
	}

	res, err := r.nodeFn(ctx, client, nodeID)
	if err != nil {
		err = errors.Wrapf(err, "error requesting %s from node %d (%s)",
			r.errorCtx, nodeID, r.nodeStatuses[nodeID].livenessStatus)
	}
	length := 0
	value := reflect.ValueOf(res)
	if res != nil && !value.IsNil() {
		length = 1
		if value.Kind() == reflect.Slice {
			length = value.Len()
		}
	}
	addNodeResp(paginatedNodeResponse{nodeID: nodeID, response: res, len: length, value: value, err: err})
}

// processResponses processes the responses returned into responseChan. Must only
// be called once.
func (r *rpcNodePaginator) processResponses(ctx context.Context) (next paginationState, err error) {
	// Copy r.pagState, as concurrent invocations of queryNode expect it to not
	// change.
	next = r.pagState
	count := 0
	numNodes := r.numNodes
	for numNodes > 0 {
		select {
		case res, ok := <-r.responseChan:
			if res.err != nil {
				r.errorFn(res.nodeID, res.err)
			} else if res.len > 0 && count < r.limit {
				var startIdx, endIdx int
				if res.nodeID == next.inProgress && next.inProgressIndex != 0 {
					startIdx = next.inProgressIndex
				}
				if startIdx > res.len {
					startIdx = res.len
				}
				if r.limit < (count + res.len - startIdx) {
					endIdx = startIdx + (r.limit - count)
				} else {
					endIdx = res.len
				}
				count += endIdx - startIdx
				next.inProgress = res.nodeID
				next.inProgressIndex = endIdx

				var response interface{}
				if res.value.Kind() == reflect.Slice {
					response = res.value.Slice(startIdx, endIdx).Interface()
				} else if endIdx > startIdx {
					// res.len must be 1 if res.value.Kind is not Slice.
					response = res.value.Interface()
				}
				r.responseFn(res.nodeID, response)
			}
			if res.nodeID != 0 && count < r.limit {
				next.nodesQueried = append(next.nodesQueried, res.nodeID)
				if len(next.nodesToQuery) > 0 {
					next.inProgress = next.nodesToQuery[0]
					next.inProgressIndex = 0
					next.nodesToQuery = next.nodesToQuery[1:]
				} else {
					next.nodesToQuery = next.nodesToQuery[:0]
					next.inProgress = 0
					next.inProgressIndex = 0
				}
			}
			if !ok {
				return next, err
			}
		case <-ctx.Done():
			err = errors.Errorf("request of %s canceled before completion", r.errorCtx)
		}
		numNodes--
	}
	return next, err
}

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

func getSimplePaginationValues(r *http.Request) (limit, offset int) {
	var err error
	if limit, err = strconv.Atoi(r.URL.Query().Get("limit")); err != nil || limit <= 0 {
		return 0, 0
	}
	if offset, err = strconv.Atoi(r.URL.Query().Get("offset")); err != nil || offset <= 0 {
		return limit, 0
	}
	return limit, offset
}
