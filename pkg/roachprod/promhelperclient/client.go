// Copyright 2024 The Cockroach Authors.

// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Utility to connect and invoke APIs of the promhelperservice.
// Doc reference - https://cockroachlabs.atlassian.net/wiki/x/MAAlzg

package promhelperclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
)

const (
	resourceName    = "instance-configs"
	resourceVersion = "v1"
)

// Node has the configuration of a specific node of a cluster
type Node struct {
	NodeID string `json:"node_id"`
	// Targets are the list of targets in the host:port format
	Targets []string `json:"targets"`
	// Labels are the labels to be added for the node. Note that the "node" label is automatically added from NodeID
	Labels map[string]string `json:"labels"`
}

// instanceConfigRequest is the HTTP request received for generating instance config
type instanceConfigRequest struct {
	ClusterID string  `json:"cluster_id"`
	Nodes     []*Node `json:"nodes"`
}

// CreateClusterConfig creates the cluster config in the promUrl
func CreateClusterConfig(ctx context.Context, promUrl, clusterName string, nodes []string) error {
	insConfigRequest := &instanceConfigRequest{
		ClusterID: clusterName,
		Nodes:     make([]*Node, len(nodes)),
	}
	for i, n := range nodes {
		insConfigRequest.Nodes[i] = &Node{
			NodeID:  strconv.Itoa(i + 1),
			Targets: []string{n},
			Labels:  map[string]string{"tenant": "system"},
		}
	}
	req, err := forCreateRequest(insConfigRequest)
	if err != nil {
		return err
	}
	response, err := httputil.Post(ctx, getCreateUrl(promUrl), "application/json", req)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("request failed with status %d and error %s", response.StatusCode,
			string(body))
	}
	return nil
}

// DeleteClusterConfig deletes the cluster config in the promUrl
func DeleteClusterConfig(ctx context.Context, promUrl, clusterName string) error {
	response, err := httputil.Delete(ctx, getDeleteUrl(promUrl, clusterName))
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusNoContent {
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("request failed with status %d and error %s", response.StatusCode,
			string(body))
	}
	return nil
}

func getDeleteUrl(promUrl, clusterName string) string {
	return fmt.Sprintf("%s/%s/%s/%s", promUrl, resourceVersion, resourceName, clusterName)
}

func getCreateUrl(promUrl string) string {
	return fmt.Sprintf("%s/%s/%s", promUrl, resourceVersion, resourceName)
}

func forCreateRequest(req *instanceConfigRequest) (io.Reader, error) {
	var b []byte
	b, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), nil
}

//func main() {
//	//fmt.Println(CreateClusterConfig(context.Background(), "http://localhost:25780", "c1",
//	//	[]string{"10.10.10.10", "11.11.11.11"}, 29003))
//	fmt.Println(DeleteClusterConfig(context.Background(), "http://localhost:25780", "c1"))
//}
