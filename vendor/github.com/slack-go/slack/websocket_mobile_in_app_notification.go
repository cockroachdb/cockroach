package slack

// MobileInAppNotificationEvent represents the update event for Mobile App Notification.
type MobileInAppNotificationEvent struct {
	Type              string `json:"type"`
	Title             string `json:"title"`
	Subtitle          string `json:"subtitle"`
	Timestamp         string `json:"ts"`
	Channel           string `json:"channel"`
	AvatarImage       string `json:"avatarImage"`
	IsShared          bool   `json:"is_shared"`
	ChannelName       string `json:"channel_name"`
	AuthorID          string `json:"author_id"`
	AuthorDisplayName string `json:"author_display_name"`
	MessageText       string `json:"msg_text"`
	PushID            string `json:"push_id"`
	NotifcationID     string `json:"notif_id"`
	MobileLaunchURI   string `json:"mobileLaunchUri"`
	EventTimestamp    string `json:"event_ts"`
}
