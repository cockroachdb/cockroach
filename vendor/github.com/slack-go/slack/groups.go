package slack

// Group contains all the information for a group
type Group struct {
	GroupConversation
	IsGroup bool `json:"is_group"`
}
