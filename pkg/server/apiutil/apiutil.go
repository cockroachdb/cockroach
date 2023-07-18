// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package apiutil

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
)

// WriteJSONResponse returns a payload as JSON to the HTTP client.
func WriteJSONResponse(ctx context.Context, w http.ResponseWriter, code int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	res, err := json.Marshal(payload)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	_, _ = w.Write(res)
}
