package slack

import (
	"context"
	"net/url"
	"strconv"
	"time"
)

const (
	DEFAULT_STARS_USER  = ""
	DEFAULT_STARS_COUNT = 100
	DEFAULT_STARS_PAGE  = 1
)

type StarsParameters struct {
	User  string
	Count int
	Page  int
}

type StarredItem Item

type listResponseFull struct {
	Items  []Item `json:"items"`
	Paging `json:"paging"`
	SlackResponse
}

// NewStarsParameters initialises StarsParameters with default values
func NewStarsParameters() StarsParameters {
	return StarsParameters{
		User:  DEFAULT_STARS_USER,
		Count: DEFAULT_STARS_COUNT,
		Page:  DEFAULT_STARS_PAGE,
	}
}

// AddStar stars an item in a channel
func (api *Client) AddStar(channel string, item ItemRef) error {
	return api.AddStarContext(context.Background(), channel, item)
}

// AddStarContext stars an item in a channel with a custom context
func (api *Client) AddStarContext(ctx context.Context, channel string, item ItemRef) error {
	values := url.Values{
		"channel": {channel},
		"token":   {api.token},
	}
	if item.Timestamp != "" {
		values.Set("timestamp", item.Timestamp)
	}
	if item.File != "" {
		values.Set("file", item.File)
	}
	if item.Comment != "" {
		values.Set("file_comment", item.Comment)
	}

	response := &SlackResponse{}
	if err := api.postMethod(ctx, "stars.add", values, response); err != nil {
		return err
	}

	return response.Err()
}

// RemoveStar removes a starred item from a channel
func (api *Client) RemoveStar(channel string, item ItemRef) error {
	return api.RemoveStarContext(context.Background(), channel, item)
}

// RemoveStarContext removes a starred item from a channel with a custom context
func (api *Client) RemoveStarContext(ctx context.Context, channel string, item ItemRef) error {
	values := url.Values{
		"channel": {channel},
		"token":   {api.token},
	}
	if item.Timestamp != "" {
		values.Set("timestamp", item.Timestamp)
	}
	if item.File != "" {
		values.Set("file", item.File)
	}
	if item.Comment != "" {
		values.Set("file_comment", item.Comment)
	}

	response := &SlackResponse{}
	if err := api.postMethod(ctx, "stars.remove", values, response); err != nil {
		return err
	}

	return response.Err()
}

// ListStars returns information about the stars a user added
func (api *Client) ListStars(params StarsParameters) ([]Item, *Paging, error) {
	return api.ListStarsContext(context.Background(), params)
}

// ListStarsContext returns information about the stars a user added with a custom context
func (api *Client) ListStarsContext(ctx context.Context, params StarsParameters) ([]Item, *Paging, error) {
	values := url.Values{
		"token": {api.token},
	}
	if params.User != DEFAULT_STARS_USER {
		values.Add("user", params.User)
	}
	if params.Count != DEFAULT_STARS_COUNT {
		values.Add("count", strconv.Itoa(params.Count))
	}
	if params.Page != DEFAULT_STARS_PAGE {
		values.Add("page", strconv.Itoa(params.Page))
	}

	response := &listResponseFull{}
	err := api.postMethod(ctx, "stars.list", values, response)
	if err != nil {
		return nil, nil, err
	}

	if err := response.Err(); err != nil {
		return nil, nil, err
	}

	return response.Items, &response.Paging, nil
}

// GetStarred returns a list of StarredItem items.
//
// The user then has to iterate over them and figure out what they should
// be looking at according to what is in the Type.
//    for _, item := range items {
//        switch c.Type {
//        case "file_comment":
//            log.Println(c.Comment)
//        case "file":
//             ...
//
//    }
// This function still exists to maintain backwards compatibility.
// I exposed it as returning []StarredItem, so it shall stay as StarredItem
func (api *Client) GetStarred(params StarsParameters) ([]StarredItem, *Paging, error) {
	return api.GetStarredContext(context.Background(), params)
}

// GetStarredContext returns a list of StarredItem items with a custom context
//
// For more details see GetStarred
func (api *Client) GetStarredContext(ctx context.Context, params StarsParameters) ([]StarredItem, *Paging, error) {
	items, paging, err := api.ListStarsContext(ctx, params)
	if err != nil {
		return nil, nil, err
	}
	starredItems := make([]StarredItem, len(items))
	for i, item := range items {
		starredItems[i] = StarredItem(item)
	}
	return starredItems, paging, nil
}

type listResponsePaginated struct {
	Items []Item `json:"items"`
	SlackResponse
	Metadata ResponseMetadata `json:"response_metadata"`
}

// StarredItemPagination allows for paginating over the starred items
type StarredItemPagination struct {
	Items        []Item
	limit        int
	previousResp *ResponseMetadata
	c            *Client
}

// ListStarsOption options for the GetUsers method call.
type ListStarsOption func(*StarredItemPagination)

// ListAllStars returns the complete list of starred items
func (api *Client) ListAllStars() ([]Item, error) {
	return api.ListAllStarsContext(context.Background())
}

// ListAllStarsContext returns the list of users (with their detailed information) with a custom context
func (api *Client) ListAllStarsContext(ctx context.Context) (results []Item, err error) {
	p := api.ListStarsPaginated()
	for err == nil {
		p, err = p.next(ctx)
		if err == nil {
			results = append(results, p.Items...)
		} else if rateLimitedError, ok := err.(*RateLimitedError); ok {
			select {
			case <-ctx.Done():
				err = ctx.Err()
			case <-time.After(rateLimitedError.RetryAfter):
				err = nil
			}
		}
	}

	return results, p.failure(err)
}

// ListStarsPaginated fetches users in a paginated fashion, see ListStarsPaginationContext for usage.
func (api *Client) ListStarsPaginated(options ...ListStarsOption) StarredItemPagination {
	return newStarPagination(api, options...)
}

func newStarPagination(c *Client, options ...ListStarsOption) (sip StarredItemPagination) {
	sip = StarredItemPagination{
		c:     c,
		limit: 200, // per slack api documentation.
	}

	for _, opt := range options {
		opt(&sip)
	}

	return sip
}

// done checks if the pagination has completed
func (StarredItemPagination) done(err error) bool {
	return err == errPaginationComplete
}

// done checks if pagination failed.
func (t StarredItemPagination) failure(err error) error {
	if t.done(err) {
		return nil
	}

	return err
}

// next gets the next list of starred items based on the cursor value
func (t StarredItemPagination) next(ctx context.Context) (_ StarredItemPagination, err error) {
	var (
		resp *listResponsePaginated
	)

	if t.c == nil || (t.previousResp != nil && t.previousResp.Cursor == "") {
		return t, errPaginationComplete
	}

	t.previousResp = t.previousResp.initialize()

	values := url.Values{
		"limit":  {strconv.Itoa(t.limit)},
		"token":  {t.c.token},
		"cursor": {t.previousResp.Cursor},
	}

	if err = t.c.postMethod(ctx, "stars.list", values, &resp); err != nil {
		return t, err
	}

	t.previousResp = &resp.Metadata
	t.Items = resp.Items

	return t, nil
}
