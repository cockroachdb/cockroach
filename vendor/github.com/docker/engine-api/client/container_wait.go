package client

import (
	"encoding/json"

	"golang.org/x/net/context"

	"github.com/docker/engine-api/types"
)

// ContainerWait pauses execution util a container is exits.
// It returns the API status code as response of its readiness.
func (cli *Client) ContainerWait(ctx context.Context, containerID string) (int, error) {
	resp, err := cli.postWithContext(ctx, "/containers/"+containerID+"/wait", nil, nil, nil)
	if err != nil {
		return -1, err
	}
	defer ensureReaderClosed(resp)

	var res types.ContainerWaitResponse
	if err := json.NewDecoder(resp.body).Decode(&res); err != nil {
		return -1, err
	}

	return res.StatusCode, nil
}
