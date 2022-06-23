package slack

type imChannel struct {
	ID string `json:"id"`
}

type imResponseFull struct {
	NoOp          bool      `json:"no_op"`
	AlreadyClosed bool      `json:"already_closed"`
	AlreadyOpen   bool      `json:"already_open"`
	Channel       imChannel `json:"channel"`
	IMs           []IM      `json:"ims"`
	History
	SlackResponse
}

// IM contains information related to the Direct Message channel
type IM struct {
	Conversation
	IsUserDeleted bool `json:"is_user_deleted"`
}
