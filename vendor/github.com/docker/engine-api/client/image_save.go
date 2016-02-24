package client

import (
	"io"
	"net/url"

	"golang.org/x/net/context"
)

// ImageSave retrieves one or more images from the docker host as a io.ReadCloser.
// It's up to the caller to store the images and close the stream.
func (cli *Client) ImageSave(ctx context.Context, imageIDs []string) (io.ReadCloser, error) {
	query := url.Values{
		"names": imageIDs,
	}

	resp, err := cli.getWithContext(ctx, "/images/get", query, nil)
	if err != nil {
		return nil, err
	}
	return resp.body, nil
}
