package slack

const (
	DEFAULT_HISTORY_LATEST    = ""
	DEFAULT_HISTORY_OLDEST    = "0"
	DEFAULT_HISTORY_COUNT     = 100
	DEFAULT_HISTORY_INCLUSIVE = false
	DEFAULT_HISTORY_UNREADS   = false
)

// HistoryParameters contains all the necessary information to help in the retrieval of history for Channels/Groups/DMs
type HistoryParameters struct {
	Latest    string
	Oldest    string
	Count     int
	Inclusive bool
	Unreads   bool
}

// History contains message history information needed to navigate a Channel / Group / DM history
type History struct {
	Latest   string    `json:"latest"`
	Messages []Message `json:"messages"`
	HasMore  bool      `json:"has_more"`
	Unread   int       `json:"unread_count_display"`
}

// NewHistoryParameters provides an instance of HistoryParameters with all the sane default values set
func NewHistoryParameters() HistoryParameters {
	return HistoryParameters{
		Latest:    DEFAULT_HISTORY_LATEST,
		Oldest:    DEFAULT_HISTORY_OLDEST,
		Count:     DEFAULT_HISTORY_COUNT,
		Inclusive: DEFAULT_HISTORY_INCLUSIVE,
		Unreads:   DEFAULT_HISTORY_UNREADS,
	}
}
