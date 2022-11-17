// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package replay

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/replay"
	"github.com/cockroachdb/pebble/vfs"
)

type ResponseType struct {
	Data []WorkloadCollectorResponse `json:"data"`
}

type WorkloadCollectorResponse struct {
	StoreId   int  `json:"store_id"`
	IsRunning bool `json:"is_running"`
}

type WorkloadCollectorPerformAction struct {
	StoreId          int    `json:"store_id"`
	Action           string `json:"action"`
	CaptureDirectory string `json:"capture_directory"`
}

type storeWithCollector struct {
	store             *kvserver.Store
	workloadCollector *replay.WorkloadCollector
}

type HTTPHandler struct {
	idToCollector map[int]storeWithCollector
}

func (h *HTTPHandler) SetupStoreMap(engines []storage.Engine, stores *kvserver.Stores) error {
	h.idToCollector = make(map[int]storeWithCollector)
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
		h.idToCollector[int(s.Ident.StoreID)] = storeWithCollector{
			s,
			s.GetStoreConfig().StoreToWorkloadCollector[indexOfCollector],
		}
		return nil
	})
}

func (h *HTTPHandler) HandleRequest(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "POST":
		var actionJSON WorkloadCollectorPerformAction
		if err := ReadJSON(req.Body, &actionJSON); err != nil {
			apiError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if actionJSON.Action == "start" {
			fs := vfs.Default
			swc := h.idToCollector[actionJSON.StoreId]
			err := fs.MkdirAll(actionJSON.CaptureDirectory, 0755)
			if err != nil {
				apiError(w, http.StatusInternalServerError, err.Error())
				return
			}
			if !swc.workloadCollector.IsRunning() {
				swc.workloadCollector.Start(fs, actionJSON.CaptureDirectory)
				err = swc.store.Engine().CreateCheckpoint(fs.PathJoin(actionJSON.CaptureDirectory, "checkpoint"))
			}
			if err != nil {
				apiError(w, http.StatusInternalServerError, err.Error())
				return
			}
		} else if actionJSON.Action == "stop" {
			h.idToCollector[actionJSON.StoreId].workloadCollector.Stop()
		} else {
			apiError(w, http.StatusBadRequest, "Action must be one of start or stop")
			return
		}
		fallthrough
	case "GET":
		var response []WorkloadCollectorResponse
		for id, v := range h.idToCollector {
			response = append(response, WorkloadCollectorResponse{
				StoreId:   id,
				IsRunning: v.workloadCollector.IsRunning(),
			})
		}
		writeJSONResponse(w, http.StatusOK, ResponseType{Data: response})
	}
}

func apiError(w http.ResponseWriter, code int, es string) {
	http.Error(w, es, code)
}

func writeJSONResponse(w http.ResponseWriter, code int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	res, err := json.Marshal(payload)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
	}
	_, _ = w.Write(res)
}

func ReadJSON(closer io.ReadCloser, jsonOut any) error {
	decoder := json.NewDecoder(closer)
	err := decoder.Decode(jsonOut)
	if err != nil {
		return err
	}
	return nil
}
