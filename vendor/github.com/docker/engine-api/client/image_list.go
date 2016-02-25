package client

import (
	"encoding/json"
	"net/url"

	"github.com/docker/engine-api/types"
	"github.com/docker/engine-api/types/filters"
)

// ImageList returns a list of images in the docker host.
func (cli *Client) ImageList(options types.ImageListOptions) ([]types.Image, error) {
	var images []types.Image
	query := url.Values{}

	if options.Filters.Len() > 0 {
		filterJSON, err := filters.ToParam(options.Filters)
		if err != nil {
			return images, err
		}
		query.Set("filters", filterJSON)
	}
	if options.MatchName != "" {
		// FIXME rename this parameter, to not be confused with the filters flag
		query.Set("filter", options.MatchName)
	}
	if options.All {
		query.Set("all", "1")
	}

	serverResp, err := cli.get("/images/json", query, nil)
	if err != nil {
		return images, err
	}

	err = json.NewDecoder(serverResp.body).Decode(&images)
	ensureReaderClosed(serverResp)
	return images, err
}
