package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"github.com/slack-go/slack/slackutilsx"
)

const (
	DEFAULT_MESSAGE_USERNAME         = ""
	DEFAULT_MESSAGE_REPLY_BROADCAST  = false
	DEFAULT_MESSAGE_ASUSER           = false
	DEFAULT_MESSAGE_PARSE            = ""
	DEFAULT_MESSAGE_THREAD_TIMESTAMP = ""
	DEFAULT_MESSAGE_LINK_NAMES       = 0
	DEFAULT_MESSAGE_UNFURL_LINKS     = false
	DEFAULT_MESSAGE_UNFURL_MEDIA     = true
	DEFAULT_MESSAGE_ICON_URL         = ""
	DEFAULT_MESSAGE_ICON_EMOJI       = ""
	DEFAULT_MESSAGE_MARKDOWN         = true
	DEFAULT_MESSAGE_ESCAPE_TEXT      = true
)

type chatResponseFull struct {
	Channel            string `json:"channel"`
	Timestamp          string `json:"ts"`                             //Regular message timestamp
	MessageTimeStamp   string `json:"message_ts"`                     //Ephemeral message timestamp
	ScheduledMessageID string `json:"scheduled_message_id,omitempty"` //Scheduled message id
	Text               string `json:"text"`
	SlackResponse
}

// getMessageTimestamp will inspect the `chatResponseFull` to ruturn a timestamp value
// in `chat.postMessage` its under `ts`
// in `chat.postEphemeral` its under `message_ts`
func (c chatResponseFull) getMessageTimestamp() string {
	if len(c.Timestamp) > 0 {
		return c.Timestamp
	}
	return c.MessageTimeStamp
}

// PostMessageParameters contains all the parameters necessary (including the optional ones) for a PostMessage() request
type PostMessageParameters struct {
	Username        string `json:"username"`
	AsUser          bool   `json:"as_user"`
	Parse           string `json:"parse"`
	ThreadTimestamp string `json:"thread_ts"`
	ReplyBroadcast  bool   `json:"reply_broadcast"`
	LinkNames       int    `json:"link_names"`
	UnfurlLinks     bool   `json:"unfurl_links"`
	UnfurlMedia     bool   `json:"unfurl_media"`
	IconURL         string `json:"icon_url"`
	IconEmoji       string `json:"icon_emoji"`
	Markdown        bool   `json:"mrkdwn,omitempty"`
	EscapeText      bool   `json:"escape_text"`

	// chat.postEphemeral support
	Channel string `json:"channel"`
	User    string `json:"user"`
}

// NewPostMessageParameters provides an instance of PostMessageParameters with all the sane default values set
func NewPostMessageParameters() PostMessageParameters {
	return PostMessageParameters{
		Username:        DEFAULT_MESSAGE_USERNAME,
		User:            DEFAULT_MESSAGE_USERNAME,
		AsUser:          DEFAULT_MESSAGE_ASUSER,
		Parse:           DEFAULT_MESSAGE_PARSE,
		ThreadTimestamp: DEFAULT_MESSAGE_THREAD_TIMESTAMP,
		LinkNames:       DEFAULT_MESSAGE_LINK_NAMES,
		UnfurlLinks:     DEFAULT_MESSAGE_UNFURL_LINKS,
		UnfurlMedia:     DEFAULT_MESSAGE_UNFURL_MEDIA,
		IconURL:         DEFAULT_MESSAGE_ICON_URL,
		IconEmoji:       DEFAULT_MESSAGE_ICON_EMOJI,
		Markdown:        DEFAULT_MESSAGE_MARKDOWN,
		EscapeText:      DEFAULT_MESSAGE_ESCAPE_TEXT,
	}
}

// DeleteMessage deletes a message in a channel
func (api *Client) DeleteMessage(channel, messageTimestamp string) (string, string, error) {
	respChannel, respTimestamp, _, err := api.SendMessageContext(
		context.Background(),
		channel,
		MsgOptionDelete(messageTimestamp),
	)
	return respChannel, respTimestamp, err
}

// DeleteMessageContext deletes a message in a channel with a custom context
func (api *Client) DeleteMessageContext(ctx context.Context, channel, messageTimestamp string) (string, string, error) {
	respChannel, respTimestamp, _, err := api.SendMessageContext(
		ctx,
		channel,
		MsgOptionDelete(messageTimestamp),
	)
	return respChannel, respTimestamp, err
}

// ScheduleMessage sends a message to a channel.
// Message is escaped by default according to https://api.slack.com/docs/formatting
// Use http://davestevens.github.io/slack-message-builder/ to help crafting your message.
func (api *Client) ScheduleMessage(channelID, postAt string, options ...MsgOption) (string, string, error) {
	respChannel, respTimestamp, _, err := api.SendMessageContext(
		context.Background(),
		channelID,
		MsgOptionSchedule(postAt),
		MsgOptionCompose(options...),
	)
	return respChannel, respTimestamp, err
}

// PostMessage sends a message to a channel.
// Message is escaped by default according to https://api.slack.com/docs/formatting
// Use http://davestevens.github.io/slack-message-builder/ to help crafting your message.
func (api *Client) PostMessage(channelID string, options ...MsgOption) (string, string, error) {
	respChannel, respTimestamp, _, err := api.SendMessageContext(
		context.Background(),
		channelID,
		MsgOptionPost(),
		MsgOptionCompose(options...),
	)
	return respChannel, respTimestamp, err
}

// PostMessageContext sends a message to a channel with a custom context
// For more details, see PostMessage documentation.
func (api *Client) PostMessageContext(ctx context.Context, channelID string, options ...MsgOption) (string, string, error) {
	respChannel, respTimestamp, _, err := api.SendMessageContext(
		ctx,
		channelID,
		MsgOptionPost(),
		MsgOptionCompose(options...),
	)
	return respChannel, respTimestamp, err
}

// PostEphemeral sends an ephemeral message to a user in a channel.
// Message is escaped by default according to https://api.slack.com/docs/formatting
// Use http://davestevens.github.io/slack-message-builder/ to help crafting your message.
func (api *Client) PostEphemeral(channelID, userID string, options ...MsgOption) (string, error) {
	return api.PostEphemeralContext(
		context.Background(),
		channelID,
		userID,
		options...,
	)
}

// PostEphemeralContext sends an ephemeal message to a user in a channel with a custom context
// For more details, see PostEphemeral documentation
func (api *Client) PostEphemeralContext(ctx context.Context, channelID, userID string, options ...MsgOption) (timestamp string, err error) {
	_, timestamp, _, err = api.SendMessageContext(
		ctx,
		channelID,
		MsgOptionPostEphemeral(userID),
		MsgOptionCompose(options...),
	)
	return timestamp, err
}

// UpdateMessage updates a message in a channel
func (api *Client) UpdateMessage(channelID, timestamp string, options ...MsgOption) (string, string, string, error) {
	return api.SendMessageContext(
		context.Background(),
		channelID,
		MsgOptionUpdate(timestamp),
		MsgOptionCompose(options...),
	)
}

// UpdateMessageContext updates a message in a channel
func (api *Client) UpdateMessageContext(ctx context.Context, channelID, timestamp string, options ...MsgOption) (string, string, string, error) {
	return api.SendMessageContext(
		ctx,
		channelID,
		MsgOptionUpdate(timestamp),
		MsgOptionCompose(options...),
	)
}

// UnfurlMessage unfurls a message in a channel
func (api *Client) UnfurlMessage(channelID, timestamp string, unfurls map[string]Attachment, options ...MsgOption) (string, string, string, error) {
	return api.SendMessageContext(context.Background(), channelID, MsgOptionUnfurl(timestamp, unfurls), MsgOptionCompose(options...))
}

// UnfurlMessageWithAuthURL sends an unfurl request containing an
// authentication URL.
// For more details see:
// https://api.slack.com/reference/messaging/link-unfurling#authenticated_unfurls
func (api *Client) UnfurlMessageWithAuthURL(channelID, timestamp string, userAuthURL string, options ...MsgOption) (string, string, string, error) {
	return api.UnfurlMessageWithAuthURLContext(context.Background(), channelID, timestamp, userAuthURL, options...)
}

// UnfurlMessageWithAuthURLContext sends an unfurl request containing an
// authentication URL.
// For more details see:
// https://api.slack.com/reference/messaging/link-unfurling#authenticated_unfurls
func (api *Client) UnfurlMessageWithAuthURLContext(ctx context.Context, channelID, timestamp string, userAuthURL string, options ...MsgOption) (string, string, string, error) {
	return api.SendMessageContext(ctx, channelID, MsgOptionUnfurlAuthURL(timestamp, userAuthURL), MsgOptionCompose(options...))
}

// SendMessage more flexible method for configuring messages.
func (api *Client) SendMessage(channel string, options ...MsgOption) (string, string, string, error) {
	return api.SendMessageContext(context.Background(), channel, options...)
}

// SendMessageContext more flexible method for configuring messages with a custom context.
func (api *Client) SendMessageContext(ctx context.Context, channelID string, options ...MsgOption) (_channel string, _timestamp string, _text string, err error) {
	var (
		req      *http.Request
		parser   func(*chatResponseFull) responseParser
		response chatResponseFull
	)

	if req, parser, err = buildSender(api.endpoint, options...).BuildRequest(api.token, channelID); err != nil {
		return "", "", "", err
	}

	if api.Debug() {
		reqBody, err := ioutil.ReadAll(req.Body)
		if err != nil {
			return "", "", "", err
		}
		req.Body = ioutil.NopCloser(bytes.NewBuffer(reqBody))
		api.Debugf("Sending request: %s", string(reqBody))
	}

	if err = doPost(ctx, api.httpclient, req, parser(&response), api); err != nil {
		return "", "", "", err
	}

	return response.Channel, response.getMessageTimestamp(), response.Text, response.Err()
}

// UnsafeApplyMsgOptions utility function for debugging/testing chat requests.
// NOTE: USE AT YOUR OWN RISK: No issues relating to the use of this function
// will be supported by the library.
func UnsafeApplyMsgOptions(token, channel, apiurl string, options ...MsgOption) (string, url.Values, error) {
	config, err := applyMsgOptions(token, channel, apiurl, options...)
	return config.endpoint, config.values, err
}

func applyMsgOptions(token, channel, apiurl string, options ...MsgOption) (sendConfig, error) {
	config := sendConfig{
		apiurl:   apiurl,
		endpoint: apiurl + string(chatPostMessage),
		values: url.Values{
			"token":   {token},
			"channel": {channel},
		},
	}

	for _, opt := range options {
		if err := opt(&config); err != nil {
			return config, err
		}
	}

	return config, nil
}

func buildSender(apiurl string, options ...MsgOption) sendConfig {
	return sendConfig{
		apiurl:  apiurl,
		options: options,
	}
}

type sendMode string

const (
	chatUpdate          sendMode = "chat.update"
	chatPostMessage     sendMode = "chat.postMessage"
	chatScheduleMessage sendMode = "chat.scheduleMessage"
	chatDelete          sendMode = "chat.delete"
	chatPostEphemeral   sendMode = "chat.postEphemeral"
	chatResponse        sendMode = "chat.responseURL"
	chatMeMessage       sendMode = "chat.meMessage"
	chatUnfurl          sendMode = "chat.unfurl"
)

type sendConfig struct {
	apiurl          string
	options         []MsgOption
	mode            sendMode
	endpoint        string
	values          url.Values
	attachments     []Attachment
	blocks          Blocks
	responseType    string
	replaceOriginal bool
	deleteOriginal  bool
}

func (t sendConfig) BuildRequest(token, channelID string) (req *http.Request, _ func(*chatResponseFull) responseParser, err error) {
	if t, err = applyMsgOptions(token, channelID, t.apiurl, t.options...); err != nil {
		return nil, nil, err
	}

	switch t.mode {
	case chatResponse:
		return responseURLSender{
			endpoint:        t.endpoint,
			values:          t.values,
			attachments:     t.attachments,
			blocks:          t.blocks,
			responseType:    t.responseType,
			replaceOriginal: t.replaceOriginal,
			deleteOriginal:  t.deleteOriginal,
		}.BuildRequest()
	default:
		return formSender{endpoint: t.endpoint, values: t.values}.BuildRequest()
	}
}

type formSender struct {
	endpoint string
	values   url.Values
}

func (t formSender) BuildRequest() (*http.Request, func(*chatResponseFull) responseParser, error) {
	req, err := formReq(t.endpoint, t.values)
	return req, func(resp *chatResponseFull) responseParser {
		return newJSONParser(resp)
	}, err
}

type responseURLSender struct {
	endpoint        string
	values          url.Values
	attachments     []Attachment
	blocks          Blocks
	responseType    string
	replaceOriginal bool
	deleteOriginal  bool
}

func (t responseURLSender) BuildRequest() (*http.Request, func(*chatResponseFull) responseParser, error) {
	req, err := jsonReq(t.endpoint, Msg{
		Text:            t.values.Get("text"),
		Timestamp:       t.values.Get("ts"),
		Attachments:     t.attachments,
		Blocks:          t.blocks,
		ResponseType:    t.responseType,
		ReplaceOriginal: t.replaceOriginal,
		DeleteOriginal:  t.deleteOriginal,
	})
	return req, func(resp *chatResponseFull) responseParser {
		return newContentTypeParser(resp)
	}, err
}

// MsgOption option provided when sending a message.
type MsgOption func(*sendConfig) error

// MsgOptionSchedule schedules a messages.
func MsgOptionSchedule(postAt string) MsgOption {
	return func(config *sendConfig) error {
		config.endpoint = config.apiurl + string(chatScheduleMessage)
		config.values.Add("post_at", postAt)
		return nil
	}
}

// MsgOptionPost posts a messages, this is the default.
func MsgOptionPost() MsgOption {
	return func(config *sendConfig) error {
		config.endpoint = config.apiurl + string(chatPostMessage)
		config.values.Del("ts")
		return nil
	}
}

// MsgOptionPostEphemeral - posts an ephemeral message to the provided user.
func MsgOptionPostEphemeral(userID string) MsgOption {
	return func(config *sendConfig) error {
		config.endpoint = config.apiurl + string(chatPostEphemeral)
		MsgOptionUser(userID)(config)
		config.values.Del("ts")

		return nil
	}
}

// MsgOptionMeMessage posts a "me message" type from the calling user
func MsgOptionMeMessage() MsgOption {
	return func(config *sendConfig) error {
		config.endpoint = config.apiurl + string(chatMeMessage)
		return nil
	}
}

// MsgOptionUpdate updates a message based on the timestamp.
func MsgOptionUpdate(timestamp string) MsgOption {
	return func(config *sendConfig) error {
		config.endpoint = config.apiurl + string(chatUpdate)
		config.values.Add("ts", timestamp)
		return nil
	}
}

// MsgOptionDelete deletes a message based on the timestamp.
func MsgOptionDelete(timestamp string) MsgOption {
	return func(config *sendConfig) error {
		config.endpoint = config.apiurl + string(chatDelete)
		config.values.Add("ts", timestamp)
		return nil
	}
}

// MsgOptionUnfurl unfurls a message based on the timestamp.
func MsgOptionUnfurl(timestamp string, unfurls map[string]Attachment) MsgOption {
	return func(config *sendConfig) error {
		config.endpoint = config.apiurl + string(chatUnfurl)
		config.values.Add("ts", timestamp)
		unfurlsStr, err := json.Marshal(unfurls)
		if err == nil {
			config.values.Add("unfurls", string(unfurlsStr))
		}
		return err
	}
}

// MsgOptionUnfurlAuthURL unfurls a message using an auth url based on the timestamp.
func MsgOptionUnfurlAuthURL(timestamp string, userAuthURL string) MsgOption {
	return func(config *sendConfig) error {
		config.endpoint = config.apiurl + string(chatUnfurl)
		config.values.Add("ts", timestamp)
		config.values.Add("user_auth_url", userAuthURL)
		return nil
	}
}

// MsgOptionUnfurlAuthRequired requests that the user installs the
// Slack app for unfurling.
func MsgOptionUnfurlAuthRequired(timestamp string) MsgOption {
	return func(config *sendConfig) error {
		config.endpoint = config.apiurl + string(chatUnfurl)
		config.values.Add("ts", timestamp)
		config.values.Add("user_auth_required", "true")
		return nil
	}
}

// MsgOptionUnfurlAuthMessage attaches a message inviting the user to
// authenticate.
func MsgOptionUnfurlAuthMessage(timestamp string, msg string) MsgOption {
	return func(config *sendConfig) error {
		config.endpoint = config.apiurl + string(chatUnfurl)
		config.values.Add("ts", timestamp)
		config.values.Add("user_auth_message", msg)
		return nil
	}
}

// MsgOptionResponseURL supplies a url to use as the endpoint.
func MsgOptionResponseURL(url string, responseType string) MsgOption {
	return func(config *sendConfig) error {
		config.mode = chatResponse
		config.endpoint = url
		config.responseType = responseType
		config.values.Del("ts")
		return nil
	}
}

// MsgOptionReplaceOriginal replaces original message with response url
func MsgOptionReplaceOriginal(responseURL string) MsgOption {
	return func(config *sendConfig) error {
		config.mode = chatResponse
		config.endpoint = responseURL
		config.replaceOriginal = true
		return nil
	}
}

// MsgOptionDeleteOriginal deletes original message with response url
func MsgOptionDeleteOriginal(responseURL string) MsgOption {
	return func(config *sendConfig) error {
		config.mode = chatResponse
		config.endpoint = responseURL
		config.deleteOriginal = true
		return nil
	}
}

// MsgOptionAsUser whether or not to send the message as the user.
func MsgOptionAsUser(b bool) MsgOption {
	return func(config *sendConfig) error {
		if b != DEFAULT_MESSAGE_ASUSER {
			config.values.Set("as_user", "true")
		}
		return nil
	}
}

// MsgOptionUser set the user for the message.
func MsgOptionUser(userID string) MsgOption {
	return func(config *sendConfig) error {
		config.values.Set("user", userID)
		return nil
	}
}

// MsgOptionUsername set the username for the message.
func MsgOptionUsername(username string) MsgOption {
	return func(config *sendConfig) error {
		config.values.Set("username", username)
		return nil
	}
}

// MsgOptionText provide the text for the message, optionally escape the provided
// text.
func MsgOptionText(text string, escape bool) MsgOption {
	return func(config *sendConfig) error {
		if escape {
			text = slackutilsx.EscapeMessage(text)
		}
		config.values.Add("text", text)
		return nil
	}
}

// MsgOptionAttachments provide attachments for the message.
func MsgOptionAttachments(attachments ...Attachment) MsgOption {
	return func(config *sendConfig) error {
		if attachments == nil {
			return nil
		}

		config.attachments = attachments

		// FIXME: We are setting the attachments on the message twice: above for
		// the json version, and below for the html version.  The marshalled bytes
		// we put into config.values below don't work directly in the Msg version.

		attachmentBytes, err := json.Marshal(attachments)
		if err == nil {
			config.values.Set("attachments", string(attachmentBytes))
		}

		return err
	}
}

// MsgOptionBlocks sets blocks for the message
func MsgOptionBlocks(blocks ...Block) MsgOption {
	return func(config *sendConfig) error {
		if blocks == nil {
			return nil
		}

		config.blocks.BlockSet = append(config.blocks.BlockSet, blocks...)

		blocks, err := json.Marshal(blocks)
		if err == nil {
			config.values.Set("blocks", string(blocks))
		}
		return err
	}
}

// MsgOptionEnableLinkUnfurl enables link unfurling
func MsgOptionEnableLinkUnfurl() MsgOption {
	return func(config *sendConfig) error {
		config.values.Set("unfurl_links", "true")
		return nil
	}
}

// MsgOptionDisableLinkUnfurl disables link unfurling
func MsgOptionDisableLinkUnfurl() MsgOption {
	return func(config *sendConfig) error {
		config.values.Set("unfurl_links", "false")
		return nil
	}
}

// MsgOptionDisableMediaUnfurl disables media unfurling.
func MsgOptionDisableMediaUnfurl() MsgOption {
	return func(config *sendConfig) error {
		config.values.Set("unfurl_media", "false")
		return nil
	}
}

// MsgOptionDisableMarkdown disables markdown.
func MsgOptionDisableMarkdown() MsgOption {
	return func(config *sendConfig) error {
		config.values.Set("mrkdwn", "false")
		return nil
	}
}

// MsgOptionTS sets the thread TS of the message to enable creating or replying to a thread
func MsgOptionTS(ts string) MsgOption {
	return func(config *sendConfig) error {
		config.values.Set("thread_ts", ts)
		return nil
	}
}

// MsgOptionBroadcast sets reply_broadcast to true
func MsgOptionBroadcast() MsgOption {
	return func(config *sendConfig) error {
		config.values.Set("reply_broadcast", "true")
		return nil
	}
}

// MsgOptionCompose combines multiple options into a single option.
func MsgOptionCompose(options ...MsgOption) MsgOption {
	return func(config *sendConfig) error {
		for _, opt := range options {
			if err := opt(config); err != nil {
				return err
			}
		}
		return nil
	}
}

// MsgOptionParse set parse option.
func MsgOptionParse(b bool) MsgOption {
	return func(config *sendConfig) error {
		var v string
		if b {
			v = "full"
		} else {
			v = "none"
		}
		config.values.Set("parse", v)
		return nil
	}
}

// MsgOptionIconURL sets an icon URL
func MsgOptionIconURL(iconURL string) MsgOption {
	return func(config *sendConfig) error {
		config.values.Set("icon_url", iconURL)
		return nil
	}
}

// MsgOptionIconEmoji sets an icon emoji
func MsgOptionIconEmoji(iconEmoji string) MsgOption {
	return func(config *sendConfig) error {
		config.values.Set("icon_emoji", iconEmoji)
		return nil
	}
}

// UnsafeMsgOptionEndpoint deliver the message to the specified endpoint.
// NOTE: USE AT YOUR OWN RISK: No issues relating to the use of this Option
// will be supported by the library, it is subject to change without notice that
// may result in compilation errors or runtime behaviour changes.
func UnsafeMsgOptionEndpoint(endpoint string, update func(url.Values)) MsgOption {
	return func(config *sendConfig) error {
		config.endpoint = endpoint
		update(config.values)
		return nil
	}
}

// MsgOptionPostMessageParameters maintain backwards compatibility.
func MsgOptionPostMessageParameters(params PostMessageParameters) MsgOption {
	return func(config *sendConfig) error {
		if params.Username != DEFAULT_MESSAGE_USERNAME {
			config.values.Set("username", params.Username)
		}

		// chat.postEphemeral support
		if params.User != DEFAULT_MESSAGE_USERNAME {
			config.values.Set("user", params.User)
		}

		// never generates an error.
		MsgOptionAsUser(params.AsUser)(config)

		if params.Parse != DEFAULT_MESSAGE_PARSE {
			config.values.Set("parse", params.Parse)
		}
		if params.LinkNames != DEFAULT_MESSAGE_LINK_NAMES {
			config.values.Set("link_names", "1")
		}

		if params.UnfurlLinks != DEFAULT_MESSAGE_UNFURL_LINKS {
			config.values.Set("unfurl_links", "true")
		}

		// I want to send a message with explicit `as_user` `true` and `unfurl_links` `false` in request.
		// Because setting `as_user` to `true` will change the default value for `unfurl_links` to `true` on Slack API side.
		if params.AsUser != DEFAULT_MESSAGE_ASUSER && params.UnfurlLinks == DEFAULT_MESSAGE_UNFURL_LINKS {
			config.values.Set("unfurl_links", "false")
		}
		if params.UnfurlMedia != DEFAULT_MESSAGE_UNFURL_MEDIA {
			config.values.Set("unfurl_media", "false")
		}
		if params.IconURL != DEFAULT_MESSAGE_ICON_URL {
			config.values.Set("icon_url", params.IconURL)
		}
		if params.IconEmoji != DEFAULT_MESSAGE_ICON_EMOJI {
			config.values.Set("icon_emoji", params.IconEmoji)
		}
		if params.Markdown != DEFAULT_MESSAGE_MARKDOWN {
			config.values.Set("mrkdwn", "false")
		}

		if params.ThreadTimestamp != DEFAULT_MESSAGE_THREAD_TIMESTAMP {
			config.values.Set("thread_ts", params.ThreadTimestamp)
		}
		if params.ReplyBroadcast != DEFAULT_MESSAGE_REPLY_BROADCAST {
			config.values.Set("reply_broadcast", "true")
		}

		return nil
	}
}

// PermalinkParameters are the parameters required to get a permalink to a
// message. Slack documentation can be found here:
// https://api.slack.com/methods/chat.getPermalink
type PermalinkParameters struct {
	Channel string
	Ts      string
}

// GetPermalink returns the permalink for a message. It takes
// PermalinkParameters and returns a string containing the permalink. It
// returns an error if unable to retrieve the permalink.
func (api *Client) GetPermalink(params *PermalinkParameters) (string, error) {
	return api.GetPermalinkContext(context.Background(), params)
}

// GetPermalinkContext returns the permalink for a message using a custom context.
func (api *Client) GetPermalinkContext(ctx context.Context, params *PermalinkParameters) (string, error) {
	values := url.Values{
		"channel":    {params.Channel},
		"message_ts": {params.Ts},
	}

	response := struct {
		Channel   string `json:"channel"`
		Permalink string `json:"permalink"`
		SlackResponse
	}{}
	err := api.getMethod(ctx, "chat.getPermalink", api.token, values, &response)
	if err != nil {
		return "", err
	}
	return response.Permalink, response.Err()
}

type GetScheduledMessagesParameters struct {
	Channel string
	Cursor  string
	Latest  string
	Limit   int
	Oldest  string
}

// GetScheduledMessages returns the list of scheduled messages based on params
func (api *Client) GetScheduledMessages(params *GetScheduledMessagesParameters) (channels []ScheduledMessage, nextCursor string, err error) {
	return api.GetScheduledMessagesContext(context.Background(), params)
}

// GetScheduledMessagesContext returns the list of scheduled messages in a Slack team with a custom context
func (api *Client) GetScheduledMessagesContext(ctx context.Context, params *GetScheduledMessagesParameters) (channels []ScheduledMessage, nextCursor string, err error) {
	values := url.Values{
		"token": {api.token},
	}
	if params.Channel != "" {
		values.Add("channel", params.Channel)
	}
	if params.Cursor != "" {
		values.Add("cursor", params.Cursor)
	}
	if params.Limit != 0 {
		values.Add("limit", strconv.Itoa(params.Limit))
	}
	if params.Latest != "" {
		values.Add("latest", params.Latest)
	}
	if params.Oldest != "" {
		values.Add("oldest", params.Oldest)
	}
	response := struct {
		Messages         []ScheduledMessage `json:"scheduled_messages"`
		ResponseMetaData responseMetaData   `json:"response_metadata"`
		SlackResponse
	}{}

	err = api.postMethod(ctx, "chat.scheduledMessages.list", values, &response)
	if err != nil {
		return nil, "", err
	}

	return response.Messages, response.ResponseMetaData.NextCursor, response.Err()
}

type DeleteScheduledMessageParameters struct {
	Channel            string
	ScheduledMessageID string
	AsUser             bool
}

// DeleteScheduledMessage returns the list of scheduled messages based on params
func (api *Client) DeleteScheduledMessage(params *DeleteScheduledMessageParameters) (bool, error) {
	return api.DeleteScheduledMessageContext(context.Background(), params)
}

// DeleteScheduledMessageContext returns the list of scheduled messages in a Slack team with a custom context
func (api *Client) DeleteScheduledMessageContext(ctx context.Context, params *DeleteScheduledMessageParameters) (bool, error) {
	values := url.Values{
		"token":                {api.token},
		"channel":              {params.Channel},
		"scheduled_message_id": {params.ScheduledMessageID},
		"as_user":              {strconv.FormatBool(params.AsUser)},
	}
	response := struct {
		SlackResponse
	}{}

	err := api.postMethod(ctx, "chat.deleteScheduledMessage", values, &response)
	if err != nil {
		return false, err
	}

	return response.Ok, response.Err()
}
