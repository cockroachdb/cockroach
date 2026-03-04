// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbconsole

import (
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// ExplainPlanResponse contains the decoded explain plan.
type ExplainPlanResponse struct {
	// ExplainPlan is the decoded plan gist as a human-readable string.
	ExplainPlan string `json:"explain_plan"`
}

// IndexRecommendationsResponse contains the list of index recommendations.
type IndexRecommendationsResponse struct {
	// Recommendations is a list of index recommendation strings (e.g.
	// "CREATE INDEX ...").
	Recommendations []string `json:"recommendations"`
}

// GetExplainPlan decodes a plan gist and returns the human-readable explain
// plan.
//
// ---
// @Summary Decode plan gist
// @Description Decodes a plan gist into a human-readable explain plan.
// @Tags Plans
// @Produce json
// @Param plan_gist query string true "The plan gist to decode"
// @Success 200 {object} ExplainPlanResponse
// @Failure 400 {string} string "Missing plan_gist parameter"
// @Failure 500 {object} ErrorResponse
// @Security ApiKeyAuth
// @Router /explain-plan [get]
func (api *ApiV2DBConsole) GetExplainPlan(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)

	planGist := r.URL.Query().Get("plan_gist")
	if planGist == "" {
		http.Error(w, "missing required parameter: plan_gist", http.StatusBadRequest)
		return
	}

	ie := api.InternalDB.Executor()
	row, err := ie.QueryRowEx(
		ctx, "dbconsole-decode-plan-gist", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: sqlUser},
		`SELECT crdb_internal.decode_plan_gist($1)`,
		planGist,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	var plan string
	if row != nil && row[0] != tree.DNull {
		plan = string(tree.MustBeDString(row[0]))
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, ExplainPlanResponse{
		ExplainPlan: plan,
	})
}

// GetIndexRecommendations returns index recommendations for a given statement
// execution.
//
// ---
// @Summary Get index recommendations
// @Description Returns index recommendations for a statement identified by
// its plan gist, application name, and query text.
// @Tags Plans
// @Produce json
// @Param plan_gist query string true "Plan gist"
// @Param app_name query string true "Application name"
// @Param query query string true "Query text"
// @Success 200 {object} IndexRecommendationsResponse
// @Failure 400 {string} string "Missing required parameter"
// @Failure 500 {object} ErrorResponse
// @Security ApiKeyAuth
// @Router /index-recommendations [get]
func (api *ApiV2DBConsole) GetIndexRecommendations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)

	planGist := r.URL.Query().Get("plan_gist")
	appName := r.URL.Query().Get("app_name")
	query := r.URL.Query().Get("query")
	if planGist == "" || appName == "" || query == "" {
		http.Error(
			w, "missing required parameters: plan_gist, app_name, query",
			http.StatusBadRequest,
		)
		return
	}

	ie := api.InternalDB.Executor()
	rows, err := ie.QueryBufferedEx(
		ctx, "dbconsole-get-index-recommendations", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: sqlUser},
		`SELECT index_recommendations
     FROM crdb_internal.statement_statistics
     WHERE plan_gist = $1
       AND app_name = $2
       AND metadata->>'query' = $3
     ORDER BY aggregated_ts DESC
     LIMIT 1`,
		planGist, appName, query,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	var recommendations []string
	if len(rows) > 0 && rows[0][0] != tree.DNull {
		arr := tree.MustBeDArray(rows[0][0])
		for _, d := range arr.Array {
			if d != tree.DNull {
				recommendations = append(recommendations, string(tree.MustBeDString(d)))
			}
		}
	}
	if recommendations == nil {
		recommendations = []string{}
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, IndexRecommendationsResponse{
		Recommendations: recommendations,
	})
}
