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

type responseType struct {
	Data []workloadCollectorResponse `json:"data"`
}

type workloadCollectorResponse struct {
	StoreId   int  `json:"store_id"`
	IsRunning bool `json:"is_running"`
}

type workloadCollectorPerformAction struct {
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
		var actionJSON workloadCollectorPerformAction
		if err := readJSON(req, &actionJSON); err != nil {
			apiError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if actionJSON.Action == "enable" {
			fs := vfs.RelativeFS(vfs.Default, actionJSON.CaptureDirectory)
			swc := h.idToCollector[actionJSON.StoreId]
			swc.workloadCollector.StartCollectorFileListener(fs)
			err := swc.store.Engine().CreateCheckpoint(fs.PathJoin(actionJSON.CaptureDirectory, "checkpoint"))
			if err != nil {
				apiError(w, http.StatusInternalServerError, err.Error())
				return
			}
		} else if actionJSON.Action == "disable" {
			h.idToCollector[actionJSON.StoreId].workloadCollector.StopCollectorFileListener()
		} else {
			apiError(w, http.StatusBadRequest, "Action must be one of enable or disable")
			return
		}
		fallthrough
	case "GET":
		var response []workloadCollectorResponse
		for id, v := range h.idToCollector {
			response = append(response, workloadCollectorResponse{
				StoreId:   id,
				IsRunning: v.workloadCollector.IsRunning(),
			})
		}
		writeJSONResponse(w, http.StatusOK, responseType{Data: response})
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

func readJSON(req *http.Request, jsonOut any) error {
	rc := req.Body
	decoder := json.NewDecoder(rc)
	err := decoder.Decode(jsonOut)
	if err != nil {
		return err
	}
	return nil
}
