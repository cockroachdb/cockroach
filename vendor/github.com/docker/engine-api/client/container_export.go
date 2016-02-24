package client

import (
	"io"
	"net/url"

	"golang.org/x/net/context"
)

// ContainerExport retrieves the raw contents of a container
// and returns them as a io.ReadCloser. It's up to the caller
// to close the stream.
func (cli *Client) ContainerExport(ctx context.Context, containerID string) (io.ReadCloser, error) {
	serverResp, err := cli.getWithContext(ctx, "/containers/"+containerID+"/export", url.Values{}, nil)
	if err != nil {
		return nil, err
	}

	return serverResp.body, nil
}
