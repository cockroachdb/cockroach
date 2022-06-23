package slack

// IMCreatedEvent represents the IM created event
type IMCreatedEvent struct {
	Type    string             `json:"type"`
	User    string             `json:"user"`
	Channel ChannelCreatedInfo `json:"channel"`
}

// IMHistoryChangedEvent represents the IM history changed event
type IMHistoryChangedEvent ChannelHistoryChangedEvent

// IMOpenEvent represents the IM open event
type IMOpenEvent ChannelInfoEvent

// IMCloseEvent represents the IM close event
type IMCloseEvent ChannelInfoEvent

// IMMarkedEvent represents the IM marked event
type IMMarkedEvent ChannelInfoEvent

// IMMarkedHistoryChanged represents the IM marked history changed event
type IMMarkedHistoryChanged ChannelInfoEvent
