package client

import (
	"net/url"
	"strconv"
)

// ContainerStop stops a container without terminating the process.
// The process is blocked until the container stops or the timeout expires.
func (cli *Client) ContainerStop(containerID string, timeout int) error {
	query := url.Values{}
	query.Set("t", strconv.Itoa(timeout))
	resp, err := cli.post("/containers/"+containerID+"/stop", query, nil, nil)
	ensureReaderClosed(resp)
	return err
}
