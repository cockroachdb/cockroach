package slack

import (
	"context"
	"net/url"
	"strconv"
	"strings"
)

// Conversation is the foundation for IM and BaseGroupConversation
type Conversation struct {
	ID                 string   `json:"id"`
	Created            JSONTime `json:"created"`
	IsOpen             bool     `json:"is_open"`
	LastRead           string   `json:"last_read,omitempty"`
	Latest             *Message `json:"latest,omitempty"`
	UnreadCount        int      `json:"unread_count,omitempty"`
	UnreadCountDisplay int      `json:"unread_count_display,omitempty"`
	IsGroup            bool     `json:"is_group"`
	IsShared           bool     `json:"is_shared"`
	IsIM               bool     `json:"is_im"`
	IsExtShared        bool     `json:"is_ext_shared"`
	IsOrgShared        bool     `json:"is_org_shared"`
	IsPendingExtShared bool     `json:"is_pending_ext_shared"`
	IsPrivate          bool     `json:"is_private"`
	IsMpIM             bool     `json:"is_mpim"`
	Unlinked           int      `json:"unlinked"`
	NameNormalized     string   `json:"name_normalized"`
	NumMembers         int      `json:"num_members"`
	Priority           float64  `json:"priority"`
	User               string   `json:"user"`

	// TODO support pending_shared
	// TODO support previous_names
}

// GroupConversation is the foundation for Group and Channel
type GroupConversation struct {
	Conversation
	Name       string   `json:"name"`
	Creator    string   `json:"creator"`
	IsArchived bool     `json:"is_archived"`
	Members    []string `json:"members"`
	Topic      Topic    `json:"topic"`
	Purpose    Purpose  `json:"purpose"`
}

// Topic contains information about the topic
type Topic struct {
	Value   string   `json:"value"`
	Creator string   `json:"creator"`
	LastSet JSONTime `json:"last_set"`
}

// Purpose contains information about the purpose
type Purpose struct {
	Value   string   `json:"value"`
	Creator string   `json:"creator"`
	LastSet JSONTime `json:"last_set"`
}

type GetUsersInConversationParameters struct {
	ChannelID string
	Cursor    string
	Limit     int
}

type GetConversationsForUserParameters struct {
	UserID          string
	Cursor          string
	Types           []string
	Limit           int
	ExcludeArchived bool
}

type responseMetaData struct {
	NextCursor string `json:"next_cursor"`
}

// GetUsersInConversation returns the list of users in a conversation
func (api *Client) GetUsersInConversation(params *GetUsersInConversationParameters) ([]string, string, error) {
	return api.GetUsersInConversationContext(context.Background(), params)
}

// GetUsersInConversationContext returns the list of users in a conversation with a custom context
func (api *Client) GetUsersInConversationContext(ctx context.Context, params *GetUsersInConversationParameters) ([]string, string, error) {
	values := url.Values{
		"token":   {api.token},
		"channel": {params.ChannelID},
	}
	if params.Cursor != "" {
		values.Add("cursor", params.Cursor)
	}
	if params.Limit != 0 {
		values.Add("limit", strconv.Itoa(params.Limit))
	}
	response := struct {
		Members          []string         `json:"members"`
		ResponseMetaData responseMetaData `json:"response_metadata"`
		SlackResponse
	}{}

	err := api.postMethod(ctx, "conversations.members", values, &response)
	if err != nil {
		return nil, "", err
	}

	if err := response.Err(); err != nil {
		return nil, "", err
	}

	return response.Members, response.ResponseMetaData.NextCursor, nil
}

// GetConversationsForUser returns the list conversations for a given user
func (api *Client) GetConversationsForUser(params *GetConversationsForUserParameters) (channels []Channel, nextCursor string, err error) {
	return api.GetConversationsForUserContext(context.Background(), params)
}

// GetConversationsForUserContext returns the list conversations for a given user with a custom context
func (api *Client) GetConversationsForUserContext(ctx context.Context, params *GetConversationsForUserParameters) (channels []Channel, nextCursor string, err error) {
	values := url.Values{
		"token": {api.token},
	}
	if params.UserID != "" {
		values.Add("user", params.UserID)
	}
	if params.Cursor != "" {
		values.Add("cursor", params.Cursor)
	}
	if params.Limit != 0 {
		values.Add("limit", strconv.Itoa(params.Limit))
	}
	if params.Types != nil {
		values.Add("types", strings.Join(params.Types, ","))
	}
	if params.ExcludeArchived {
		values.Add("exclude_archived", "true")
	}
	response := struct {
		Channels         []Channel        `json:"channels"`
		ResponseMetaData responseMetaData `json:"response_metadata"`
		SlackResponse
	}{}
	err = api.postMethod(ctx, "users.conversations", values, &response)
	if err != nil {
		return nil, "", err
	}

	return response.Channels, response.ResponseMetaData.NextCursor, response.Err()
}

// ArchiveConversation archives a conversation
func (api *Client) ArchiveConversation(channelID string) error {
	return api.ArchiveConversationContext(context.Background(), channelID)
}

// ArchiveConversationContext archives a conversation with a custom context
func (api *Client) ArchiveConversationContext(ctx context.Context, channelID string) error {
	values := url.Values{
		"token":   {api.token},
		"channel": {channelID},
	}

	response := SlackResponse{}
	err := api.postMethod(ctx, "conversations.archive", values, &response)
	if err != nil {
		return err
	}

	return response.Err()
}

// UnArchiveConversation reverses conversation archival
func (api *Client) UnArchiveConversation(channelID string) error {
	return api.UnArchiveConversationContext(context.Background(), channelID)
}

// UnArchiveConversationContext reverses conversation archival with a custom context
func (api *Client) UnArchiveConversationContext(ctx context.Context, channelID string) error {
	values := url.Values{
		"token":   {api.token},
		"channel": {channelID},
	}
	response := SlackResponse{}
	err := api.postMethod(ctx, "conversations.unarchive", values, &response)
	if err != nil {
		return err
	}

	return response.Err()
}

// SetTopicOfConversation sets the topic for a conversation
func (api *Client) SetTopicOfConversation(channelID, topic string) (*Channel, error) {
	return api.SetTopicOfConversationContext(context.Background(), channelID, topic)
}

// SetTopicOfConversationContext sets the topic for a conversation with a custom context
func (api *Client) SetTopicOfConversationContext(ctx context.Context, channelID, topic string) (*Channel, error) {
	values := url.Values{
		"token":   {api.token},
		"channel": {channelID},
		"topic":   {topic},
	}
	response := struct {
		SlackResponse
		Channel *Channel `json:"channel"`
	}{}
	err := api.postMethod(ctx, "conversations.setTopic", values, &response)
	if err != nil {
		return nil, err
	}

	return response.Channel, response.Err()
}

// SetPurposeOfConversation sets the purpose for a conversation
func (api *Client) SetPurposeOfConversation(channelID, purpose string) (*Channel, error) {
	return api.SetPurposeOfConversationContext(context.Background(), channelID, purpose)
}

// SetPurposeOfConversationContext sets the purpose for a conversation with a custom context
func (api *Client) SetPurposeOfConversationContext(ctx context.Context, channelID, purpose string) (*Channel, error) {
	values := url.Values{
		"token":   {api.token},
		"channel": {channelID},
		"purpose": {purpose},
	}
	response := struct {
		SlackResponse
		Channel *Channel `json:"channel"`
	}{}

	err := api.postMethod(ctx, "conversations.setPurpose", values, &response)
	if err != nil {
		return nil, err
	}

	return response.Channel, response.Err()
}

// RenameConversation renames a conversation
func (api *Client) RenameConversation(channelID, channelName string) (*Channel, error) {
	return api.RenameConversationContext(context.Background(), channelID, channelName)
}

// RenameConversationContext renames a conversation with a custom context
func (api *Client) RenameConversationContext(ctx context.Context, channelID, channelName string) (*Channel, error) {
	values := url.Values{
		"token":   {api.token},
		"channel": {channelID},
		"name":    {channelName},
	}
	response := struct {
		SlackResponse
		Channel *Channel `json:"channel"`
	}{}

	err := api.postMethod(ctx, "conversations.rename", values, &response)
	if err != nil {
		return nil, err
	}

	return response.Channel, response.Err()
}

// InviteUsersToConversation invites users to a channel
func (api *Client) InviteUsersToConversation(channelID string, users ...string) (*Channel, error) {
	return api.InviteUsersToConversationContext(context.Background(), channelID, users...)
}

// InviteUsersToConversationContext invites users to a channel with a custom context
func (api *Client) InviteUsersToConversationContext(ctx context.Context, channelID string, users ...string) (*Channel, error) {
	values := url.Values{
		"token":   {api.token},
		"channel": {channelID},
		"users":   {strings.Join(users, ",")},
	}
	response := struct {
		SlackResponse
		Channel *Channel `json:"channel"`
	}{}

	err := api.postMethod(ctx, "conversations.invite", values, &response)
	if err != nil {
		return nil, err
	}

	return response.Channel, response.Err()
}

// KickUserFromConversation removes a user from a conversation
func (api *Client) KickUserFromConversation(channelID string, user string) error {
	return api.KickUserFromConversationContext(context.Background(), channelID, user)
}

// KickUserFromConversationContext removes a user from a conversation with a custom context
func (api *Client) KickUserFromConversationContext(ctx context.Context, channelID string, user string) error {
	values := url.Values{
		"token":   {api.token},
		"channel": {channelID},
		"user":    {user},
	}

	response := SlackResponse{}
	err := api.postMethod(ctx, "conversations.kick", values, &response)
	if err != nil {
		return err
	}

	return response.Err()
}

// CloseConversation closes a direct message or multi-person direct message
func (api *Client) CloseConversation(channelID string) (noOp bool, alreadyClosed bool, err error) {
	return api.CloseConversationContext(context.Background(), channelID)
}

// CloseConversationContext closes a direct message or multi-person direct message with a custom context
func (api *Client) CloseConversationContext(ctx context.Context, channelID string) (noOp bool, alreadyClosed bool, err error) {
	values := url.Values{
		"token":   {api.token},
		"channel": {channelID},
	}
	response := struct {
		SlackResponse
		NoOp          bool `json:"no_op"`
		AlreadyClosed bool `json:"already_closed"`
	}{}

	err = api.postMethod(ctx, "conversations.close", values, &response)
	if err != nil {
		return false, false, err
	}

	return response.NoOp, response.AlreadyClosed, response.Err()
}

// CreateConversation initiates a public or private channel-based conversation
func (api *Client) CreateConversation(channelName string, isPrivate bool) (*Channel, error) {
	return api.CreateConversationContext(context.Background(), channelName, isPrivate)
}

// CreateConversationContext initiates a public or private channel-based conversation with a custom context
func (api *Client) CreateConversationContext(ctx context.Context, channelName string, isPrivate bool) (*Channel, error) {
	values := url.Values{
		"token":      {api.token},
		"name":       {channelName},
		"is_private": {strconv.FormatBool(isPrivate)},
	}
	response, err := api.channelRequest(ctx, "conversations.create", values)
	if err != nil {
		return nil, err
	}

	return &response.Channel, nil
}

// GetConversationInfo retrieves information about a conversation
func (api *Client) GetConversationInfo(channelID string, includeLocale bool) (*Channel, error) {
	return api.GetConversationInfoContext(context.Background(), channelID, includeLocale)
}

// GetConversationInfoContext retrieves information about a conversation with a custom context
func (api *Client) GetConversationInfoContext(ctx context.Context, channelID string, includeLocale bool) (*Channel, error) {
	values := url.Values{
		"token":          {api.token},
		"channel":        {channelID},
		"include_locale": {strconv.FormatBool(includeLocale)},
	}
	response, err := api.channelRequest(ctx, "conversations.info", values)
	if err != nil {
		return nil, err
	}

	return &response.Channel, response.Err()
}

// LeaveConversation leaves a conversation
func (api *Client) LeaveConversation(channelID string) (bool, error) {
	return api.LeaveConversationContext(context.Background(), channelID)
}

// LeaveConversationContext leaves a conversation with a custom context
func (api *Client) LeaveConversationContext(ctx context.Context, channelID string) (bool, error) {
	values := url.Values{
		"token":   {api.token},
		"channel": {channelID},
	}

	response, err := api.channelRequest(ctx, "conversations.leave", values)
	if err != nil {
		return false, err
	}

	return response.NotInChannel, err
}

type GetConversationRepliesParameters struct {
	ChannelID string
	Timestamp string
	Cursor    string
	Inclusive bool
	Latest    string
	Limit     int
	Oldest    string
}

// GetConversationReplies retrieves a thread of messages posted to a conversation
func (api *Client) GetConversationReplies(params *GetConversationRepliesParameters) (msgs []Message, hasMore bool, nextCursor string, err error) {
	return api.GetConversationRepliesContext(context.Background(), params)
}

// GetConversationRepliesContext retrieves a thread of messages posted to a conversation with a custom context
func (api *Client) GetConversationRepliesContext(ctx context.Context, params *GetConversationRepliesParameters) (msgs []Message, hasMore bool, nextCursor string, err error) {
	values := url.Values{
		"token":   {api.token},
		"channel": {params.ChannelID},
		"ts":      {params.Timestamp},
	}
	if params.Cursor != "" {
		values.Add("cursor", params.Cursor)
	}
	if params.Latest != "" {
		values.Add("latest", params.Latest)
	}
	if params.Limit != 0 {
		values.Add("limit", strconv.Itoa(params.Limit))
	}
	if params.Oldest != "" {
		values.Add("oldest", params.Oldest)
	}
	if params.Inclusive {
		values.Add("inclusive", "1")
	} else {
		values.Add("inclusive", "0")
	}
	response := struct {
		SlackResponse
		HasMore          bool `json:"has_more"`
		ResponseMetaData struct {
			NextCursor string `json:"next_cursor"`
		} `json:"response_metadata"`
		Messages []Message `json:"messages"`
	}{}

	err = api.postMethod(ctx, "conversations.replies", values, &response)
	if err != nil {
		return nil, false, "", err
	}

	return response.Messages, response.HasMore, response.ResponseMetaData.NextCursor, response.Err()
}

type GetConversationsParameters struct {
	Cursor          string
	ExcludeArchived bool
	Limit           int
	Types           []string
	TeamID          string
}

// GetConversations returns the list of channels in a Slack team
func (api *Client) GetConversations(params *GetConversationsParameters) (channels []Channel, nextCursor string, err error) {
	return api.GetConversationsContext(context.Background(), params)
}

// GetConversationsContext returns the list of channels in a Slack team with a custom context
func (api *Client) GetConversationsContext(ctx context.Context, params *GetConversationsParameters) (channels []Channel, nextCursor string, err error) {
	values := url.Values{
		"token": {api.token},
	}
	if params.Cursor != "" {
		values.Add("cursor", params.Cursor)
	}
	if params.Limit != 0 {
		values.Add("limit", strconv.Itoa(params.Limit))
	}
	if params.Types != nil {
		values.Add("types", strings.Join(params.Types, ","))
	}
	if params.ExcludeArchived {
		values.Add("exclude_archived", strconv.FormatBool(params.ExcludeArchived))
	}
	if params.TeamID != "" {
		values.Add("team_id", params.TeamID)
	}

	response := struct {
		Channels         []Channel        `json:"channels"`
		ResponseMetaData responseMetaData `json:"response_metadata"`
		SlackResponse
	}{}

	err = api.postMethod(ctx, "conversations.list", values, &response)
	if err != nil {
		return nil, "", err
	}

	return response.Channels, response.ResponseMetaData.NextCursor, response.Err()
}

type OpenConversationParameters struct {
	ChannelID string
	ReturnIM  bool
	Users     []string
}

// OpenConversation opens or resumes a direct message or multi-person direct message
func (api *Client) OpenConversation(params *OpenConversationParameters) (*Channel, bool, bool, error) {
	return api.OpenConversationContext(context.Background(), params)
}

// OpenConversationContext opens or resumes a direct message or multi-person direct message with a custom context
func (api *Client) OpenConversationContext(ctx context.Context, params *OpenConversationParameters) (*Channel, bool, bool, error) {
	values := url.Values{
		"token":     {api.token},
		"return_im": {strconv.FormatBool(params.ReturnIM)},
	}
	if params.ChannelID != "" {
		values.Add("channel", params.ChannelID)
	}
	if params.Users != nil {
		values.Add("users", strings.Join(params.Users, ","))
	}
	response := struct {
		Channel     *Channel `json:"channel"`
		NoOp        bool     `json:"no_op"`
		AlreadyOpen bool     `json:"already_open"`
		SlackResponse
	}{}

	err := api.postMethod(ctx, "conversations.open", values, &response)
	if err != nil {
		return nil, false, false, err
	}

	return response.Channel, response.NoOp, response.AlreadyOpen, response.Err()
}

// JoinConversation joins an existing conversation
func (api *Client) JoinConversation(channelID string) (*Channel, string, []string, error) {
	return api.JoinConversationContext(context.Background(), channelID)
}

// JoinConversationContext joins an existing conversation with a custom context
func (api *Client) JoinConversationContext(ctx context.Context, channelID string) (*Channel, string, []string, error) {
	values := url.Values{"token": {api.token}, "channel": {channelID}}
	response := struct {
		Channel          *Channel `json:"channel"`
		Warning          string   `json:"warning"`
		ResponseMetaData *struct {
			Warnings []string `json:"warnings"`
		} `json:"response_metadata"`
		SlackResponse
	}{}

	err := api.postMethod(ctx, "conversations.join", values, &response)
	if err != nil {
		return nil, "", nil, err
	}
	if response.Err() != nil {
		return nil, "", nil, response.Err()
	}
	var warnings []string
	if response.ResponseMetaData != nil {
		warnings = response.ResponseMetaData.Warnings
	}
	return response.Channel, response.Warning, warnings, nil
}

type GetConversationHistoryParameters struct {
	ChannelID string
	Cursor    string
	Inclusive bool
	Latest    string
	Limit     int
	Oldest    string
}

type GetConversationHistoryResponse struct {
	SlackResponse
	HasMore          bool   `json:"has_more"`
	PinCount         int    `json:"pin_count"`
	Latest           string `json:"latest"`
	ResponseMetaData struct {
		NextCursor string `json:"next_cursor"`
	} `json:"response_metadata"`
	Messages []Message `json:"messages"`
}

// GetConversationHistory joins an existing conversation
func (api *Client) GetConversationHistory(params *GetConversationHistoryParameters) (*GetConversationHistoryResponse, error) {
	return api.GetConversationHistoryContext(context.Background(), params)
}

// GetConversationHistoryContext joins an existing conversation with a custom context
func (api *Client) GetConversationHistoryContext(ctx context.Context, params *GetConversationHistoryParameters) (*GetConversationHistoryResponse, error) {
	values := url.Values{"token": {api.token}, "channel": {params.ChannelID}}
	if params.Cursor != "" {
		values.Add("cursor", params.Cursor)
	}
	if params.Inclusive {
		values.Add("inclusive", "1")
	} else {
		values.Add("inclusive", "0")
	}
	if params.Latest != "" {
		values.Add("latest", params.Latest)
	}
	if params.Limit != 0 {
		values.Add("limit", strconv.Itoa(params.Limit))
	}
	if params.Oldest != "" {
		values.Add("oldest", params.Oldest)
	}

	response := GetConversationHistoryResponse{}

	err := api.postMethod(ctx, "conversations.history", values, &response)
	if err != nil {
		return nil, err
	}

	return &response, response.Err()
}

// MarkConversation sets the read mark of a conversation to a specific point
func (api *Client) MarkConversation(channel, ts string) (err error) {
	return api.MarkConversationContext(context.Background(), channel, ts)
}

// MarkConversationContext sets the read mark of a conversation to a specific point with a custom context
func (api *Client) MarkConversationContext(ctx context.Context, channel, ts string) error {
	values := url.Values{
		"token":   {api.token},
		"channel": {channel},
		"ts":      {ts},
	}

	response := &SlackResponse{}

	err := api.postMethod(ctx, "conversations.mark", values, response)
	if err != nil {
		return err
	}
	return response.Err()
}
