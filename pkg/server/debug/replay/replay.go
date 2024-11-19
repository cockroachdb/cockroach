// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package replay implements a debug-server HTTP handler for managing storage-level
// workload capture.
package replay

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

type HTTPHandler struct {
	syncutil.Mutex
	Stores *kvserver.Stores
}

type workloadCollectorGetter interface {
	WorkloadCollector() *replay.WorkloadCollector
}

func (h *HTTPHandler) HandleRequest(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "POST":
		var actionJSON WorkloadCollectorPerformActionRequest
		if err := json.NewDecoder(req.Body).Decode(&actionJSON); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		s, err := h.Stores.GetStore(roachpb.StoreID(actionJSON.StoreID))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		getter, ok := s.TODOEngine().(workloadCollectorGetter)
		if !ok {
			http.Error(
				w,
				fmt.Sprintf("Failed to retrieve workload collector for store with id: %d", s.StoreID()),
				http.StatusInternalServerError,
			)
			return
		}
		wc := getter.WorkloadCollector()

		switch actionJSON.Action {
		case "start":
			err := func() error {
				h.Lock()
				defer h.Unlock()
				captureFS := vfs.Default
				err := captureFS.MkdirAll(actionJSON.CaptureDirectory, 0755)
				if err != nil {
					return err
				}
				if !wc.IsRunning() {
					wc.Start(captureFS, actionJSON.CaptureDirectory)
					err = s.TODOEngine().CreateCheckpoint(captureFS.PathJoin(actionJSON.CaptureDirectory, "checkpoint"), nil)
				}
				if err != nil {
					return err
				}
				return nil
			}()

			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		case "stop":
			wc.Stop()
		default:
			http.Error(w, "Action must be one of start or stop", http.StatusBadRequest)
			return
		}
		// POST performs the action then returns the same response as the GET request
		fallthrough
	case "GET":
		var response []WorkloadCollectorStatus
		err := h.Stores.VisitStores(func(s *kvserver.Store) error {
			getter, ok := s.TODOEngine().(workloadCollectorGetter)
			if !ok {
				return errors.Newf("Failed to retrieve workload collector for store with id: %d", s.StoreID())
			}

			response = append(response, WorkloadCollectorStatus{
				StoreID:   int(s.StoreID()),
				IsRunning: getter.WorkloadCollector().IsRunning(),
			})
			return nil
		})

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
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
