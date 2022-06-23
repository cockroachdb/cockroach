package slack

type pinEvent struct {
	Type           string `json:"type"`
	User           string `json:"user"`
	Item           Item   `json:"item"`
	Channel        string `json:"channel_id"`
	EventTimestamp string `json:"event_ts"`
	HasPins        bool   `json:"has_pins,omitempty"`
}

// PinAddedEvent represents the Pin added event
type PinAddedEvent pinEvent

// PinRemovedEvent represents the Pin removed event
type PinRemovedEvent pinEvent
