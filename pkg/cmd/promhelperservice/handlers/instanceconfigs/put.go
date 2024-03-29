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
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/cmd/promhelperservice/logging"
)

// PutHandler handles PUT /instance-configs/{cluster_id}
// Doc reference - https://cockroachlabs.atlassian.net/wiki/x/MAAlzg
func PutHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := logging.MakeLogger(ctx, "create instance config")
	clusterID := r.PathValue(ClusterIDParam)
	if clusterID == "" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(formFailureResponse("", "could not extract cluster id from URL"))
		return
	}
	requestBody, err := getConfig(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(formFailureResponse(clusterID, "invalid/empty request"))
		log.Errorf("parse request failed: %v\n", err)
		return
	}
	if err = requestBody.validate(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(formFailureResponse(clusterID, fmt.Sprintf("invalid request: %v", err)))
		log.Errorf("request validation failure: %v\n", err)
		return
	}
	if err = createClusterConfigFile(clusterID, requestBody, log); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write(formFailureResponse(clusterID, "error creating cluster config"))
		log.Errorf("cluster config creation failed: %v\n", err)
		return
	}
	res, err := generateInstanceConfigResponse(&instanceConfigResponse{
		ClusterID: clusterID,
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write(formFailureResponse(clusterID, "error generating response"))
		log.Errorf("parse request failed: %v\n", err)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(res)
}

// ccParams are the params for the clusterConfFileTemplate
type ccParams struct {
	Targets []string
	Labels  []string
}

const clusterConfFileTemplate = `- targets:
{{range $val := .Targets}}  - {{$val}}
{{end}}  labels:
{{range $val := .Labels}}    {{$val}}
{{end}}
`

// createClusterConfigFile creates the cluster config file per node
func createClusterConfigFile(clusterID string, r *instanceConfig, log logging.Logger) error {
	fileName := fmt.Sprintf("%s/%s.%s", getDir(),
		clusterID, instanceConfigFileExt)
	cc, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	defer func() { _ = cc.Close() }()
	_, _ = cc.Write([]byte("---\n"))
	for _, n := range r.Nodes {
		if n.Labels == nil {
			n.Labels = make(map[string]string)
		}
		// automatically add the node based on the nodeID
		n.Labels["node"] = fmt.Sprintf("\"%s\"", n.NodeID)
		params := &ccParams{
			Targets: n.Targets,
			Labels:  make([]string, 0),
		}
		for key, value := range n.Labels {
			params.Labels = append(params.Labels, fmt.Sprintf("%s: %s", key, value))
		}
		t := template.Must(template.New("start").Parse(clusterConfFileTemplate))
		if err := t.Execute(cc, params); err != nil {
			return err
		}
	}
	log.Infof("File created: %v", fileName)
	return nil
}
