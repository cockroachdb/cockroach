package slack

import (
	"context"
	"net/url"
	"strconv"
)

// ItemReaction is the reactions that have happened on an item.
type ItemReaction struct {
	Name  string   `json:"name"`
	Count int      `json:"count"`
	Users []string `json:"users"`
}

// ReactedItem is an item that was reacted to, and the details of the
// reactions.
type ReactedItem struct {
	Item
	Reactions []ItemReaction
}

// GetReactionsParameters is the inputs to get reactions to an item.
type GetReactionsParameters struct {
	Full bool
}

// NewGetReactionsParameters initializes the inputs to get reactions to an item.
func NewGetReactionsParameters() GetReactionsParameters {
	return GetReactionsParameters{
		Full: false,
	}
}

type getReactionsResponseFull struct {
	Type string
	M    struct {
		Reactions []ItemReaction
	} `json:"message"`
	F struct {
		Reactions []ItemReaction
	} `json:"file"`
	FC struct {
		Reactions []ItemReaction
	} `json:"comment"`
	SlackResponse
}

func (res getReactionsResponseFull) extractReactions() []ItemReaction {
	switch res.Type {
	case "message":
		return res.M.Reactions
	case "file":
		return res.F.Reactions
	case "file_comment":
		return res.FC.Reactions
	}
	return []ItemReaction{}
}

const (
	DEFAULT_REACTIONS_USER  = ""
	DEFAULT_REACTIONS_COUNT = 100
	DEFAULT_REACTIONS_PAGE  = 1
	DEFAULT_REACTIONS_FULL  = false
)

// ListReactionsParameters is the inputs to find all reactions by a user.
type ListReactionsParameters struct {
	User  string
	Count int
	Page  int
	Full  bool
}

// NewListReactionsParameters initializes the inputs to find all reactions
// performed by a user.
func NewListReactionsParameters() ListReactionsParameters {
	return ListReactionsParameters{
		User:  DEFAULT_REACTIONS_USER,
		Count: DEFAULT_REACTIONS_COUNT,
		Page:  DEFAULT_REACTIONS_PAGE,
		Full:  DEFAULT_REACTIONS_FULL,
	}
}

type listReactionsResponseFull struct {
	Items []struct {
		Type    string
		Channel string
		M       struct {
			*Message
		} `json:"message"`
		F struct {
			*File
			Reactions []ItemReaction
		} `json:"file"`
		FC struct {
			*Comment
			Reactions []ItemReaction
		} `json:"comment"`
	}
	Paging `json:"paging"`
	SlackResponse
}

func (res listReactionsResponseFull) extractReactedItems() []ReactedItem {
	items := make([]ReactedItem, len(res.Items))
	for i, input := range res.Items {
		item := ReactedItem{}
		item.Type = input.Type
		switch input.Type {
		case "message":
			item.Channel = input.Channel
			item.Message = input.M.Message
			item.Reactions = input.M.Reactions
		case "file":
			item.File = input.F.File
			item.Reactions = input.F.Reactions
		case "file_comment":
			item.File = input.F.File
			item.Comment = input.FC.Comment
			item.Reactions = input.FC.Reactions
		}
		items[i] = item
	}
	return items
}

// AddReaction adds a reaction emoji to a message, file or file comment.
func (api *Client) AddReaction(name string, item ItemRef) error {
	return api.AddReactionContext(context.Background(), name, item)
}

// AddReactionContext adds a reaction emoji to a message, file or file comment with a custom context.
func (api *Client) AddReactionContext(ctx context.Context, name string, item ItemRef) error {
	values := url.Values{
		"token": {api.token},
	}
	if name != "" {
		values.Set("name", name)
	}
	if item.Channel != "" {
		values.Set("channel", item.Channel)
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
	if err := api.postMethod(ctx, "reactions.add", values, response); err != nil {
		return err
	}

	return response.Err()
}

// RemoveReaction removes a reaction emoji from a message, file or file comment.
func (api *Client) RemoveReaction(name string, item ItemRef) error {
	return api.RemoveReactionContext(context.Background(), name, item)
}

// RemoveReactionContext removes a reaction emoji from a message, file or file comment with a custom context.
func (api *Client) RemoveReactionContext(ctx context.Context, name string, item ItemRef) error {
	values := url.Values{
		"token": {api.token},
	}
	if name != "" {
		values.Set("name", name)
	}
	if item.Channel != "" {
		values.Set("channel", item.Channel)
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
	if err := api.postMethod(ctx, "reactions.remove", values, response); err != nil {
		return err
	}

	return response.Err()
}

// GetReactions returns details about the reactions on an item.
func (api *Client) GetReactions(item ItemRef, params GetReactionsParameters) ([]ItemReaction, error) {
	return api.GetReactionsContext(context.Background(), item, params)
}

// GetReactionsContext returns details about the reactions on an item with a custom context
func (api *Client) GetReactionsContext(ctx context.Context, item ItemRef, params GetReactionsParameters) ([]ItemReaction, error) {
	values := url.Values{
		"token": {api.token},
	}
	if item.Channel != "" {
		values.Set("channel", item.Channel)
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
	if params.Full != DEFAULT_REACTIONS_FULL {
		values.Set("full", strconv.FormatBool(params.Full))
	}

	response := &getReactionsResponseFull{}
	if err := api.postMethod(ctx, "reactions.get", values, response); err != nil {
		return nil, err
	}

	if err := response.Err(); err != nil {
		return nil, err
	}

	return response.extractReactions(), nil
}

// ListReactions returns information about the items a user reacted to.
func (api *Client) ListReactions(params ListReactionsParameters) ([]ReactedItem, *Paging, error) {
	return api.ListReactionsContext(context.Background(), params)
}

// ListReactionsContext returns information about the items a user reacted to with a custom context.
func (api *Client) ListReactionsContext(ctx context.Context, params ListReactionsParameters) ([]ReactedItem, *Paging, error) {
	values := url.Values{
		"token": {api.token},
	}
	if params.User != DEFAULT_REACTIONS_USER {
		values.Add("user", params.User)
	}
	if params.Count != DEFAULT_REACTIONS_COUNT {
		values.Add("count", strconv.Itoa(params.Count))
	}
	if params.Page != DEFAULT_REACTIONS_PAGE {
		values.Add("page", strconv.Itoa(params.Page))
	}
	if params.Full != DEFAULT_REACTIONS_FULL {
		values.Add("full", strconv.FormatBool(params.Full))
	}

	response := &listReactionsResponseFull{}
	err := api.postMethod(ctx, "reactions.list", values, response)
	if err != nil {
		return nil, nil, err
	}

	if err := response.Err(); err != nil {
		return nil, nil, err
	}

	return response.extractReactedItems(), &response.Paging, nil
}
