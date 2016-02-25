package client

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/docker/engine-api/types"
)

// ContainerInspect returns the container information.
func (cli *Client) ContainerInspect(containerID string) (types.ContainerJSON, error) {
	serverResp, err := cli.get("/containers/"+containerID+"/json", nil, nil)
	if err != nil {
		if serverResp.statusCode == http.StatusNotFound {
			return types.ContainerJSON{}, containerNotFoundError{containerID}
		}
		return types.ContainerJSON{}, err
	}

	var response types.ContainerJSON
	err = json.NewDecoder(serverResp.body).Decode(&response)
	ensureReaderClosed(serverResp)
	return response, err
}

// ContainerInspectWithRaw returns the container information and it's raw representation.
func (cli *Client) ContainerInspectWithRaw(containerID string, getSize bool) (types.ContainerJSON, []byte, error) {
	query := url.Values{}
	if getSize {
		query.Set("size", "1")
	}
	serverResp, err := cli.get("/containers/"+containerID+"/json", query, nil)
	if err != nil {
		if serverResp.statusCode == http.StatusNotFound {
			return types.ContainerJSON{}, nil, containerNotFoundError{containerID}
		}
		return types.ContainerJSON{}, nil, err
	}
	defer ensureReaderClosed(serverResp)

	body, err := ioutil.ReadAll(serverResp.body)
	if err != nil {
		return types.ContainerJSON{}, nil, err
	}

	var response types.ContainerJSON
	rdr := bytes.NewReader(body)
	err = json.NewDecoder(rdr).Decode(&response)
	return response, body, err
}

func (cli *Client) containerInspectWithResponse(containerID string, query url.Values) (types.ContainerJSON, *serverResponse, error) {
	serverResp, err := cli.get("/containers/"+containerID+"/json", nil, nil)
	if err != nil {
		return types.ContainerJSON{}, serverResp, err
	}

	var response types.ContainerJSON
	err = json.NewDecoder(serverResp.body).Decode(&response)
	return response, serverResp, err
}
