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

// DatabasesResponse contains the list of databases in the cluster.
type DatabasesResponse struct {
	// Databases is the list of database names.
	Databases []string `json:"databases"`
}

// GetDatabases returns the list of databases in the cluster.
//
// ---
// @Summary List databases
// @Description Returns the list of all databases in the cluster.
// @Tags Databases
// @Produce json
// @Success 200 {object} DatabasesResponse
// @Failure 500 {object} ErrorResponse
// @Security ApiKeyAuth
// @Router /databases [get]
func (api *ApiV2DBConsole) GetDatabases(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)

	ie := api.InternalDB.Executor()
	rows, err := ie.QueryBufferedEx(
		ctx, "dbconsole-get-databases", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: sqlUser},
		`SELECT database_name FROM [SHOW DATABASES]`,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	databases := make([]string, 0, len(rows))
	for _, row := range rows {
		databases = append(databases, string(tree.MustBeDString(row[0])))
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, DatabasesResponse{
		Databases: databases,
	})
}
