package slack

// SubteamCreatedEvent represents the Subteam created event
type SubteamCreatedEvent struct {
	Type    string    `json:"type"`
	Subteam UserGroup `json:"subteam"`
}

// SubteamCreatedEvent represents the membership of an existing User Group has changed event
type SubteamMembersChangedEvent struct {
	Type               string   `json:"type"`
	SubteamID          string   `json:"subteam_id"`
	TeamID             string   `json:"team_id"`
	DatePreviousUpdate JSONTime `json:"date_previous_update"`
	DateUpdate         JSONTime `json:"date_update"`
	AddedUsers         []string `json:"added_users"`
	AddedUsersCount    string   `json:"added_users_count"`
	RemovedUsers       []string `json:"removed_users"`
	RemovedUsersCount  string   `json:"removed_users_count"`
}

// SubteamSelfAddedEvent represents an event of you have been added to a User Group
type SubteamSelfAddedEvent struct {
	Type      string `json:"type"`
	SubteamID string `json:"subteam_id"`
}

// SubteamSelfRemovedEvent represents an event of you have been removed from a User Group
type SubteamSelfRemovedEvent SubteamSelfAddedEvent

// SubteamUpdatedEvent represents an event of an existing User Group has been updated or its members changed
type SubteamUpdatedEvent struct {
	Type    string    `json:"type"`
	Subteam UserGroup `json:"subteam"`
}
