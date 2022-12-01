// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package replay implements a debug-server HTTP handler for managing storage-level
// workload capture.
package replay

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/replay"
	"github.com/cockroachdb/pebble/vfs"
)

type ResponseType struct {
	Data []WorkloadCollectorStatus `json:"data"`
}

type WorkloadCollectorStatus struct {
	StoreID   int  `json:"store_id"`
	IsRunning bool `json:"is_running"`
}

type WorkloadCollectorPerformActionRequest struct {
	StoreID          int    `json:"store_id"`
	Action           string `json:"action"`
	CaptureDirectory string `json:"capture_directory"`
}

type storeWithCollector struct {
	store             *kvserver.Store
	workloadCollector *replay.WorkloadCollector
}

type HTTPHandler struct {
	storeIDToCollector map[roachpb.StoreID]storeWithCollector
}

func (h *HTTPHandler) SetupStoreMap(engines []storage.Engine, stores *kvserver.Stores) error {
	h.storeIDToCollector = make(map[roachpb.StoreID]storeWithCollector)
	idToIdx := make(map[roachpb.StoreID]int)

	for i := range engines {
		id, err := kvserver.ReadStoreIdent(context.Background(), engines[i])
		if err != nil {
			return err
		}
		idToIdx[id.StoreID] = i
	}

	return stores.VisitStores(func(s *kvserver.Store) error {
		indexOfCollector, ok := idToIdx[s.StoreID()]
		if !ok {
			return errors.New("Failed to map storeId to index")
		}
		h.storeIDToCollector[s.Ident.StoreID] = storeWithCollector{
			s,
			s.GetStoreConfig().StoreToWorkloadCollector[indexOfCollector],
		}
		return nil
	})
}

func (h *HTTPHandler) HandleRequest(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "POST":
		var actionJSON WorkloadCollectorPerformActionRequest
		if err := json.NewDecoder(req.Body).Decode(&actionJSON); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		switch actionJSON.Action {
		case "start":
			captureFS := vfs.Default
			swc := h.storeIDToCollector[roachpb.StoreID(actionJSON.StoreID)]
			err := captureFS.MkdirAll(actionJSON.CaptureDirectory, 0755)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if !swc.workloadCollector.IsRunning() {
				swc.workloadCollector.Start(captureFS, actionJSON.CaptureDirectory)
				err = swc.store.Engine().CreateCheckpoint(captureFS.PathJoin(actionJSON.CaptureDirectory, "checkpoint"))
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		case "stop":
			h.storeIDToCollector[roachpb.StoreID(actionJSON.StoreID)].workloadCollector.Stop()
		default:
			http.Error(w, "Action must be one of start or stop", http.StatusBadRequest)
			return
		}
		// POST performs the action then returns the same response as the GET request
		fallthrough
	case "GET":
		var response []WorkloadCollectorStatus
		for id, v := range h.storeIDToCollector {
			response = append(response, WorkloadCollectorStatus{
				StoreID:   int(id),
				IsRunning: v.workloadCollector.IsRunning(),
			})
		}
		writeJSONResponse(w, http.StatusOK, ResponseType{Data: response})
	}
}

func writeJSONResponse(w http.ResponseWriter, code int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	err := json.NewEncoder(w).Encode(payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
