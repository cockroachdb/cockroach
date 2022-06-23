package slack

// TeamJoinEvent represents the Team join event
type TeamJoinEvent struct {
	Type string `json:"type"`
	User User   `json:"user"`
}

// TeamRenameEvent represents the Team rename event
type TeamRenameEvent struct {
	Type           string `json:"type"`
	Name           string `json:"name,omitempty"`
	EventTimestamp string `json:"event_ts,omitempty"`
}

// TeamPrefChangeEvent represents the Team preference change event
type TeamPrefChangeEvent struct {
	Type  string   `json:"type"`
	Name  string   `json:"name,omitempty"`
	Value []string `json:"value,omitempty"`
}

// TeamDomainChangeEvent represents the Team domain change event
type TeamDomainChangeEvent struct {
	Type   string `json:"type"`
	URL    string `json:"url"`
	Domain string `json:"domain"`
}

// TeamMigrationStartedEvent represents the Team migration started event
type TeamMigrationStartedEvent struct {
	Type string `json:"type"`
}
