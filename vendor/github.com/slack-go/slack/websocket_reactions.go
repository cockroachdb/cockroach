package slack

// reactionItem is a lighter-weight item than is returned by the reactions list.
type reactionItem struct {
	Type        string `json:"type"`
	Channel     string `json:"channel,omitempty"`
	File        string `json:"file,omitempty"`
	FileComment string `json:"file_comment,omitempty"`
	Timestamp   string `json:"ts,omitempty"`
}

type reactionEvent struct {
	Type           string       `json:"type"`
	User           string       `json:"user"`
	ItemUser       string       `json:"item_user"`
	Item           reactionItem `json:"item"`
	Reaction       string       `json:"reaction"`
	EventTimestamp string       `json:"event_ts"`
}

// ReactionAddedEvent represents the Reaction added event
type ReactionAddedEvent reactionEvent

// ReactionRemovedEvent represents the Reaction removed event
type ReactionRemovedEvent reactionEvent
