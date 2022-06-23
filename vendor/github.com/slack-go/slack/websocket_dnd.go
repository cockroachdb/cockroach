package slack

// DNDUpdatedEvent represents the update event for Do Not Disturb
type DNDUpdatedEvent struct {
	Type   string    `json:"type"`
	User   string    `json:"user"`
	Status DNDStatus `json:"dnd_status"`
}
