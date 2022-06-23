package slack

// ChannelCreatedEvent represents the Channel created event
type ChannelCreatedEvent struct {
	Type           string             `json:"type"`
	Channel        ChannelCreatedInfo `json:"channel"`
	EventTimestamp string             `json:"event_ts"`
}

// ChannelCreatedInfo represents the information associated with the Channel created event
type ChannelCreatedInfo struct {
	ID        string `json:"id"`
	IsChannel bool   `json:"is_channel"`
	Name      string `json:"name"`
	Created   int    `json:"created"`
	Creator   string `json:"creator"`
}

// ChannelJoinedEvent represents the Channel joined event
type ChannelJoinedEvent struct {
	Type    string  `json:"type"`
	Channel Channel `json:"channel"`
}

// ChannelInfoEvent represents the Channel info event
type ChannelInfoEvent struct {
	// channel_left
	// channel_deleted
	// channel_archive
	// channel_unarchive
	Type      string `json:"type"`
	Channel   string `json:"channel"`
	User      string `json:"user,omitempty"`
	Timestamp string `json:"ts,omitempty"`
}

// ChannelRenameEvent represents the Channel rename event
type ChannelRenameEvent struct {
	Type      string            `json:"type"`
	Channel   ChannelRenameInfo `json:"channel"`
	Timestamp string            `json:"event_ts"`
}

// ChannelRenameInfo represents the information associated with a Channel rename event
type ChannelRenameInfo struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Created int    `json:"created"`
}

// ChannelHistoryChangedEvent represents the Channel history changed event
type ChannelHistoryChangedEvent struct {
	Type           string `json:"type"`
	Latest         string `json:"latest"`
	Timestamp      string `json:"ts"`
	EventTimestamp string `json:"event_ts"`
}

// ChannelMarkedEvent represents the Channel marked event
type ChannelMarkedEvent ChannelInfoEvent

// ChannelLeftEvent represents the Channel left event
type ChannelLeftEvent ChannelInfoEvent

// ChannelDeletedEvent represents the Channel deleted event
type ChannelDeletedEvent ChannelInfoEvent

// ChannelArchiveEvent represents the Channel archive event
type ChannelArchiveEvent ChannelInfoEvent

// ChannelUnarchiveEvent represents the Channel unarchive event
type ChannelUnarchiveEvent ChannelInfoEvent
