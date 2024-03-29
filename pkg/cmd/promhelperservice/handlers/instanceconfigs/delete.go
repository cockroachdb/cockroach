// Copyright 2024 The Cockroach Authors.

// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package instanceconfigs

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cmd/promhelperservice/logging"
)

// DeleteHandler handles DELETE /instance-configs/{cluster_id}
// Doc reference - https://cockroachlabs.atlassian.net/wiki/x/MAAlzg
func DeleteHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := logging.MakeLogger(ctx, "delete instance config")
	clusterID := r.PathValue(ClusterIDParam)
	if clusterID == "" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(formFailureResponse("", "could not extract cluster id from URL"))
		return
	}
	// search for all files in the directory
	file := fmt.Sprintf("%s/%s.%s",
		getDir(), clusterID, instanceConfigFileExt)
	log.Infof("Deleting file: %s", file)
	if err := os.Remove(file); err != nil {
		log.Errorf("failed to delete file for cluster id %s with file name: %s - %v\n",
			clusterID, file, err)
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write(formFailureResponse("", "could not find cluster file"))
	}
	w.WriteHeader(http.StatusNoContent)
}
