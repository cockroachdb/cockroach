package slack

import (
	"context"
	"encoding/json"
)

const (
	VTModal   ViewType = "modal"
	VTHomeTab ViewType = "home"
)

type ViewType string

type ViewState struct {
	Values map[string]map[string]BlockAction `json:"values"`
}

type View struct {
	SlackResponse
	ID              string           `json:"id"`
	TeamID          string           `json:"team_id"`
	Type            ViewType         `json:"type"`
	Title           *TextBlockObject `json:"title"`
	Close           *TextBlockObject `json:"close"`
	Submit          *TextBlockObject `json:"submit"`
	Blocks          Blocks           `json:"blocks"`
	PrivateMetadata string           `json:"private_metadata"`
	CallbackID      string           `json:"callback_id"`
	State           *ViewState       `json:"state"`
	Hash            string           `json:"hash"`
	ClearOnClose    bool             `json:"clear_on_close"`
	NotifyOnClose   bool             `json:"notify_on_close"`
	RootViewID      string           `json:"root_view_id"`
	PreviousViewID  string           `json:"previous_view_id"`
	AppID           string           `json:"app_id"`
	ExternalID      string           `json:"external_id"`
	BotID           string           `json:"bot_id"`
}

type ViewSubmissionCallbackResponseURL struct {
	BlockID     string `json:"block_id"`
	ActionID    string `json:"action_id"`
	ChannelID   string `json:"channel_id"`
	ResponseURL string `json:"response_url"`
}

type ViewSubmissionCallback struct {
	Hash         string                              `json:"hash"`
	ResponseURLs []ViewSubmissionCallbackResponseURL `json:"response_urls,omitempty"`
}

type ViewClosedCallback struct {
	IsCleared bool `json:"is_cleared"`
}

const (
	RAClear  ViewResponseAction = "clear"
	RAUpdate ViewResponseAction = "update"
	RAPush   ViewResponseAction = "push"
	RAErrors ViewResponseAction = "errors"
)

type ViewResponseAction string

type ViewSubmissionResponse struct {
	ResponseAction ViewResponseAction `json:"response_action"`
	View           *ModalViewRequest  `json:"view,omitempty"`
	Errors         map[string]string  `json:"errors,omitempty"`
}

func NewClearViewSubmissionResponse() *ViewSubmissionResponse {
	return &ViewSubmissionResponse{
		ResponseAction: RAClear,
	}
}

func NewUpdateViewSubmissionResponse(view *ModalViewRequest) *ViewSubmissionResponse {
	return &ViewSubmissionResponse{
		ResponseAction: RAUpdate,
		View:           view,
	}
}

func NewPushViewSubmissionResponse(view *ModalViewRequest) *ViewSubmissionResponse {
	return &ViewSubmissionResponse{
		ResponseAction: RAPush,
		View:           view,
	}
}

func NewErrorsViewSubmissionResponse(errors map[string]string) *ViewSubmissionResponse {
	return &ViewSubmissionResponse{
		ResponseAction: RAErrors,
		Errors:         errors,
	}
}

type ModalViewRequest struct {
	Type            ViewType         `json:"type"`
	Title           *TextBlockObject `json:"title"`
	Blocks          Blocks           `json:"blocks"`
	Close           *TextBlockObject `json:"close,omitempty"`
	Submit          *TextBlockObject `json:"submit,omitempty"`
	PrivateMetadata string           `json:"private_metadata,omitempty"`
	CallbackID      string           `json:"callback_id,omitempty"`
	ClearOnClose    bool             `json:"clear_on_close,omitempty"`
	NotifyOnClose   bool             `json:"notify_on_close,omitempty"`
	ExternalID      string           `json:"external_id,omitempty"`
}

func (v *ModalViewRequest) ViewType() ViewType {
	return v.Type
}

type HomeTabViewRequest struct {
	Type            ViewType `json:"type"`
	Blocks          Blocks   `json:"blocks"`
	PrivateMetadata string   `json:"private_metadata,omitempty"`
	CallbackID      string   `json:"callback_id,omitempty"`
	ExternalID      string   `json:"external_id,omitempty"`
}

func (v *HomeTabViewRequest) ViewType() ViewType {
	return v.Type
}

type openViewRequest struct {
	TriggerID string           `json:"trigger_id"`
	View      ModalViewRequest `json:"view"`
}

type publishViewRequest struct {
	UserID string             `json:"user_id"`
	View   HomeTabViewRequest `json:"view"`
	Hash   string             `json:"hash,omitempty"`
}

type pushViewRequest struct {
	TriggerID string           `json:"trigger_id"`
	View      ModalViewRequest `json:"view"`
}

type updateViewRequest struct {
	View       ModalViewRequest `json:"view"`
	ExternalID string           `json:"external_id,omitempty"`
	Hash       string           `json:"hash,omitempty"`
	ViewID     string           `json:"view_id,omitempty"`
}

type ViewResponse struct {
	SlackResponse
	View `json:"view"`
}

// OpenView opens a view for a user.
func (api *Client) OpenView(triggerID string, view ModalViewRequest) (*ViewResponse, error) {
	return api.OpenViewContext(context.Background(), triggerID, view)
}

// ValidateUniqueBlockID will verify if each input block has a unique block ID if set
func ValidateUniqueBlockID(view ModalViewRequest) bool {

	uniqueBlockID := map[string]bool{}

	for _, b := range view.Blocks.BlockSet {
		if inputBlock, ok := b.(*InputBlock); ok {
			if _, ok := uniqueBlockID[inputBlock.BlockID]; ok {
				return false
			}
			uniqueBlockID[inputBlock.BlockID] = true
		}
	}

	return true
}

// OpenViewContext opens a view for a user with a custom context.
func (api *Client) OpenViewContext(
	ctx context.Context,
	triggerID string,
	view ModalViewRequest,
) (*ViewResponse, error) {
	if triggerID == "" {
		return nil, ErrParametersMissing
	}

	if !ValidateUniqueBlockID(view) {
		return nil, ErrBlockIDNotUnique
	}

	req := openViewRequest{
		TriggerID: triggerID,
		View:      view,
	}
	encoded, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	endpoint := api.endpoint + "views.open"
	resp := &ViewResponse{}
	err = postJSON(ctx, api.httpclient, endpoint, api.token, encoded, resp, api)
	if err != nil {
		return nil, err
	}
	return resp, resp.Err()
}

// PublishView publishes a static view for a user.
func (api *Client) PublishView(userID string, view HomeTabViewRequest, hash string) (*ViewResponse, error) {
	return api.PublishViewContext(context.Background(), userID, view, hash)
}

// PublishViewContext publishes a static view for a user with a custom context.
func (api *Client) PublishViewContext(
	ctx context.Context,
	userID string,
	view HomeTabViewRequest,
	hash string,
) (*ViewResponse, error) {
	if userID == "" {
		return nil, ErrParametersMissing
	}
	req := publishViewRequest{
		UserID: userID,
		View:   view,
		Hash:   hash,
	}
	encoded, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	endpoint := api.endpoint + "views.publish"
	resp := &ViewResponse{}
	err = postJSON(ctx, api.httpclient, endpoint, api.token, encoded, resp, api)
	if err != nil {
		return nil, err
	}
	return resp, resp.Err()
}

// PushView pushes a view onto the stack of a root view.
func (api *Client) PushView(triggerID string, view ModalViewRequest) (*ViewResponse, error) {
	return api.PushViewContext(context.Background(), triggerID, view)
}

// PublishViewContext pushes a view onto the stack of a root view with a custom context.
func (api *Client) PushViewContext(
	ctx context.Context,
	triggerID string,
	view ModalViewRequest,
) (*ViewResponse, error) {
	if triggerID == "" {
		return nil, ErrParametersMissing
	}
	req := pushViewRequest{
		TriggerID: triggerID,
		View:      view,
	}
	encoded, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	endpoint := api.endpoint + "views.push"
	resp := &ViewResponse{}
	err = postJSON(ctx, api.httpclient, endpoint, api.token, encoded, resp, api)
	if err != nil {
		return nil, err
	}
	return resp, resp.Err()
}

// UpdateView updates an existing view.
func (api *Client) UpdateView(view ModalViewRequest, externalID, hash, viewID string) (*ViewResponse, error) {
	return api.UpdateViewContext(context.Background(), view, externalID, hash, viewID)
}

// UpdateViewContext updates an existing view with a custom context.
func (api *Client) UpdateViewContext(
	ctx context.Context,
	view ModalViewRequest,
	externalID, hash,
	viewID string,
) (*ViewResponse, error) {
	if externalID == "" && viewID == "" {
		return nil, ErrParametersMissing
	}
	req := updateViewRequest{
		View:       view,
		ExternalID: externalID,
		Hash:       hash,
		ViewID:     viewID,
	}
	encoded, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	endpoint := api.endpoint + "views.update"
	resp := &ViewResponse{}
	err = postJSON(ctx, api.httpclient, endpoint, api.token, encoded, resp, api)
	if err != nil {
		return nil, err
	}
	return resp, resp.Err()
}
