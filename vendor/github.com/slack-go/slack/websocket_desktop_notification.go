package slack

// DesktopNotificationEvent represents the update event for Desktop Notification.
type DesktopNotificationEvent struct {
	Type            string `json:"type"`
	Title           string `json:"title"`
	Subtitle        string `json:"subtitle"`
	Message         string `json:"msg"`
	Timestamp       string `json:"ts"`
	Content         string `json:"content"`
	Channel         string `json:"channel"`
	LaunchURI       string `json:"launchUri"`
	AvatarImage     string `json:"avatarImage"`
	SsbFilename     string `json:"ssbFilename"`
	ImageURI        string `json:"imageUri"`
	IsShared        bool   `json:"is_shared"`
	IsChannelInvite bool   `json:"is_channel_invite"`
	EventTimestamp  string `json:"event_ts"`
}
