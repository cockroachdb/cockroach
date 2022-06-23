package slack

import (
	"context"
	"net/url"
)

type Reminder struct {
	ID         string `json:"id"`
	Creator    string `json:"creator"`
	User       string `json:"user"`
	Text       string `json:"text"`
	Recurring  bool   `json:"recurring"`
	Time       int    `json:"time"`
	CompleteTS int    `json:"complete_ts"`
}

type reminderResp struct {
	SlackResponse
	Reminder Reminder `json:"reminder"`
}

type remindersResp struct {
	SlackResponse
	Reminders []*Reminder `json:"reminders"`
}

func (api *Client) doReminder(ctx context.Context, path string, values url.Values) (*Reminder, error) {
	response := &reminderResp{}
	if err := api.postMethod(ctx, path, values, response); err != nil {
		return nil, err
	}
	return &response.Reminder, response.Err()
}

func (api *Client) doReminders(ctx context.Context, path string, values url.Values) ([]*Reminder, error) {
	response := &remindersResp{}
	if err := api.postMethod(ctx, path, values, response); err != nil {
		return nil, err
	}

	// create an array of pointers to reminders
	var reminders = make([]*Reminder, 0, len(response.Reminders))
	for _, reminder := range response.Reminders {
		reminders = append(reminders, reminder)
	}

	return reminders, response.Err()
}

// ListReminders lists all the reminders created by or for the authenticated user
//
// See https://api.slack.com/methods/reminders.list
func (api *Client) ListReminders() ([]*Reminder, error) {
	values := url.Values{
		"token": {api.token},
	}
	return api.doReminders(context.Background(), "reminders.list", values)
}

// AddChannelReminder adds a reminder for a channel.
//
// See https://api.slack.com/methods/reminders.add (NOTE: the ability to set
// reminders on a channel is currently undocumented but has been tested to
// work)
func (api *Client) AddChannelReminder(channelID, text, time string) (*Reminder, error) {
	values := url.Values{
		"token":   {api.token},
		"text":    {text},
		"time":    {time},
		"channel": {channelID},
	}
	return api.doReminder(context.Background(), "reminders.add", values)
}

// AddUserReminder adds a reminder for a user.
//
// See https://api.slack.com/methods/reminders.add (NOTE: the ability to set
// reminders on a channel is currently undocumented but has been tested to
// work)
func (api *Client) AddUserReminder(userID, text, time string) (*Reminder, error) {
	values := url.Values{
		"token": {api.token},
		"text":  {text},
		"time":  {time},
		"user":  {userID},
	}
	return api.doReminder(context.Background(), "reminders.add", values)
}

// DeleteReminder deletes an existing reminder.
//
// See https://api.slack.com/methods/reminders.delete
func (api *Client) DeleteReminder(id string) error {
	values := url.Values{
		"token":    {api.token},
		"reminder": {id},
	}
	response := &SlackResponse{}
	if err := api.postMethod(context.Background(), "reminders.delete", values, response); err != nil {
		return err
	}
	return response.Err()
}
