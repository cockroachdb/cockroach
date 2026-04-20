// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package promql

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

// API serves the Prometheus-compatible HTTP query API, backed by CockroachDB's
// internal time series database. It implements the endpoints that standard
// PromQL tools (Grafana, promtool, PromLens) expect.
type API struct {
	engine    *promql.Engine
	queryable storage.Queryable
	catalog   *MetricCatalog
}

// NewAPI creates a new PromQL API server.
func NewAPI(engine *promql.Engine, queryable storage.Queryable, catalog *MetricCatalog) *API {
	return &API{
		engine:    engine,
		queryable: queryable,
		catalog:   catalog,
	}
}

// RegisterRoutes registers the Prometheus-compatible API routes on the
// given router. All routes are mounted under /api/v1/.
func (api *API) RegisterRoutes(router *mux.Router) {
	sub := router.PathPrefix("/api/v1").Subrouter()
	sub.HandleFunc("/query", api.instantQuery).Methods("GET", "POST")
	sub.HandleFunc("/query_range", api.rangeQuery).Methods("GET", "POST")
	sub.HandleFunc("/labels", api.labelNames).Methods("GET", "POST")
	sub.HandleFunc("/label/{name}/values", api.labelValues).Methods("GET")
	sub.HandleFunc("/metadata", api.metadata).Methods("GET")
	sub.HandleFunc("/series", api.series).Methods("GET", "POST")
}

// Handler returns an http.Handler for the PromQL API.
func (api *API) Handler() http.Handler {
	router := mux.NewRouter()
	api.RegisterRoutes(router)
	return router
}

// apiResponse is the standard Prometheus API response envelope.
type apiResponse struct {
	Status    string      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType string      `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
	Warnings  []string    `json:"warnings,omitempty"`
}

// queryData wraps query results in the format expected by Prometheus clients.
type queryData struct {
	ResultType string      `json:"resultType"`
	Result     interface{} `json:"result"`
}

func respondJSON(w http.ResponseWriter, status int, resp apiResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}

func respondSuccess(w http.ResponseWriter, data interface{}) {
	respondJSON(w, http.StatusOK, apiResponse{
		Status: "success",
		Data:   data,
	})
}

func respondError(w http.ResponseWriter, status int, errType, msg string) {
	respondJSON(w, status, apiResponse{
		Status:    "error",
		ErrorType: errType,
		Error:     msg,
	})
}

// instantQuery handles GET/POST /api/v1/query.
func (api *API) instantQuery(w http.ResponseWriter, r *http.Request) {
	queryExpr := r.FormValue("query")
	if queryExpr == "" {
		respondError(w, http.StatusBadRequest, "bad_data", "missing query parameter")
		return
	}

	ts, err := parseTimeParam(r, "time", time.Now())
	if err != nil {
		respondError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}

	timeout := 2 * time.Minute
	if t := r.FormValue("timeout"); t != "" {
		if d, err := parseDuration(t); err == nil {
			timeout = d
		}
	}

	ctx := r.Context()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	qry, err := api.engine.NewInstantQuery(api.queryable, queryExpr, ts)
	if err != nil {
		respondError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}
	defer qry.Close()

	result := qry.Exec(ctx)
	if result.Err != nil {
		respondError(
			w, http.StatusUnprocessableEntity, "execution", result.Err.Error(),
		)
		return
	}

	respondSuccess(w, queryData{
		ResultType: string(result.Value.Type()),
		Result:     result.Value,
	})
}

// rangeQuery handles GET/POST /api/v1/query_range.
func (api *API) rangeQuery(w http.ResponseWriter, r *http.Request) {
	queryExpr := r.FormValue("query")
	if queryExpr == "" {
		respondError(w, http.StatusBadRequest, "bad_data", "missing query parameter")
		return
	}

	start, err := parseTimeParam(r, "start", time.Time{})
	if err != nil || start.IsZero() {
		respondError(w, http.StatusBadRequest, "bad_data", "invalid start parameter")
		return
	}

	end, err := parseTimeParam(r, "end", time.Time{})
	if err != nil || end.IsZero() {
		respondError(w, http.StatusBadRequest, "bad_data", "invalid end parameter")
		return
	}

	step, err := parseDuration(r.FormValue("step"))
	if err != nil || step <= 0 {
		respondError(w, http.StatusBadRequest, "bad_data", "invalid step parameter")
		return
	}

	// Limit the number of points to prevent OOM.
	if end.Sub(start)/step > 11000 {
		respondError(
			w, http.StatusBadRequest, "bad_data",
			fmt.Sprintf(
				"exceeded maximum resolution of 11,000 points per timeseries. "+
					"Try decreasing the query resolution (?step=XX)",
			),
		)
		return
	}

	timeout := 2 * time.Minute
	if t := r.FormValue("timeout"); t != "" {
		if d, err := parseDuration(t); err == nil {
			timeout = d
		}
	}

	ctx := r.Context()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	qry, err := api.engine.NewRangeQuery(
		api.queryable, queryExpr, start, end, step,
	)
	if err != nil {
		respondError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}
	defer qry.Close()

	result := qry.Exec(ctx)
	if result.Err != nil {
		respondError(
			w, http.StatusUnprocessableEntity, "execution", result.Err.Error(),
		)
		return
	}

	respondSuccess(w, queryData{
		ResultType: string(result.Value.Type()),
		Result:     result.Value,
	})
}

// labelNames handles GET/POST /api/v1/labels.
func (api *API) labelNames(w http.ResponseWriter, r *http.Request) {
	respondSuccess(w, []string{
		"__name__",
		instanceTypeLabel,
		nodeIDLabel,
		storeIDLabel,
	})
}

// labelValues handles GET /api/v1/label/{name}/values.
func (api *API) labelValues(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	querier, err := api.queryable.Querier(r.Context(), 0, math.MaxInt64)
	if err != nil {
		respondError(
			w, http.StatusInternalServerError, "execution", err.Error(),
		)
		return
	}
	defer func() { _ = querier.Close() }()

	values, _, err := querier.LabelValues(name)
	if err != nil {
		respondError(
			w, http.StatusInternalServerError, "execution", err.Error(),
		)
		return
	}
	if values == nil {
		values = []string{}
	}
	respondSuccess(w, values)
}

// metadata handles GET /api/v1/metadata.
func (api *API) metadata(w http.ResponseWriter, r *http.Request) {
	respondSuccess(w, api.catalog.AllMetadata())
}

// series handles GET/POST /api/v1/series.
func (api *API) series(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		respondError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}
	matcherSets := r.Form["match[]"]
	if len(matcherSets) == 0 {
		respondError(
			w, http.StatusBadRequest, "bad_data", "no match[] parameter provided",
		)
		return
	}

	// For now, just return the metric names as label sets. A full
	// implementation would query TSDB to discover which sources have data.
	var result []map[string]string
	for _, name := range api.catalog.AllPromNames() {
		result = append(result, map[string]string{
			"__name__": name,
		})
	}
	respondSuccess(w, result)
}

// parseTimeParam parses a time parameter from the request. It supports Unix
// timestamps (as float64 seconds) and RFC3339 format.
func parseTimeParam(r *http.Request, param string, defaultVal time.Time) (time.Time, error) {
	val := r.FormValue(param)
	if val == "" {
		return defaultVal, nil
	}

	// Try Unix timestamp (float64 seconds).
	if f, err := strconv.ParseFloat(val, 64); err == nil {
		sec := int64(f)
		nsec := int64((f - float64(sec)) * 1e9)
		return time.Unix(sec, nsec).UTC(), nil
	}

	// Try RFC3339.
	t, err := time.Parse(time.RFC3339, val)
	if err != nil {
		return time.Time{}, fmt.Errorf("cannot parse %q: %w", val, err)
	}
	return t, nil
}

// parseDuration parses a duration string. It supports Go duration format
// and plain seconds as float64.
func parseDuration(s string) (time.Duration, error) {
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return time.Duration(f * float64(time.Second)), nil
	}
	return 0, fmt.Errorf("cannot parse duration %q", s)
}
