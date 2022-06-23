package slack

// FileActionEvent represents the File action event
type fileActionEvent struct {
	Type           string `json:"type"`
	EventTimestamp string `json:"event_ts"`
	File           File   `json:"file"`
	// FileID is used for FileDeletedEvent
	FileID string `json:"file_id,omitempty"`
}

// FileCreatedEvent represents the File created event
type FileCreatedEvent fileActionEvent

// FileSharedEvent represents the File shared event
type FileSharedEvent fileActionEvent

// FilePublicEvent represents the File public event
type FilePublicEvent fileActionEvent

// FileUnsharedEvent represents the File unshared event
type FileUnsharedEvent fileActionEvent

// FileChangeEvent represents the File change event
type FileChangeEvent fileActionEvent

// FileDeletedEvent represents the File deleted event
type FileDeletedEvent fileActionEvent

// FilePrivateEvent represents the File private event
type FilePrivateEvent fileActionEvent

// FileCommentAddedEvent represents the File comment added event
type FileCommentAddedEvent struct {
	fileActionEvent
	Comment Comment `json:"comment"`
}

// FileCommentEditedEvent represents the File comment edited event
type FileCommentEditedEvent struct {
	fileActionEvent
	Comment Comment `json:"comment"`
}

// FileCommentDeletedEvent represents the File comment deleted event
type FileCommentDeletedEvent struct {
	fileActionEvent
	Comment string `json:"comment"`
}
