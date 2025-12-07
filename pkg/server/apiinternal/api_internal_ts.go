// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package apiinternal

// registerTimeSeriesRoutes sets up all the time series REST endpoints.
func (r *apiInternalServer) registerTimeSeriesRoutes() {
	routes := []route{
		{POST, "/ts/query", createHandler(r.timeseries.Query)},
	}

	for _, route := range routes {
		r.mux.HandleFunc(route.path, route.handler).Methods(string(route.method))
	}
}
