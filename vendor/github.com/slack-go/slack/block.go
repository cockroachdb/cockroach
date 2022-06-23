package slack

// @NOTE: Blocks are in beta and subject to change.

// More Information: https://api.slack.com/block-kit

// MessageBlockType defines a named string type to define each block type
// as a constant for use within the package.
type MessageBlockType string

const (
	MBTSection MessageBlockType = "section"
	MBTDivider MessageBlockType = "divider"
	MBTImage   MessageBlockType = "image"
	MBTAction  MessageBlockType = "actions"
	MBTContext MessageBlockType = "context"
	MBTFile    MessageBlockType = "file"
	MBTInput   MessageBlockType = "input"
	MBTHeader  MessageBlockType = "header"
)

// Block defines an interface all block types should implement
// to ensure consistency between blocks.
type Block interface {
	BlockType() MessageBlockType
}

// Blocks is a convenience struct defined to allow dynamic unmarshalling of
// the "blocks" value in Slack's JSON response, which varies depending on block type
type Blocks struct {
	BlockSet []Block `json:"blocks,omitempty"`
}

// BlockAction is the action callback sent when a block is interacted with
type BlockAction struct {
	ActionID              string              `json:"action_id"`
	BlockID               string              `json:"block_id"`
	Type                  ActionType          `json:"type"`
	Text                  TextBlockObject     `json:"text"`
	Value                 string              `json:"value"`
	ActionTs              string              `json:"action_ts"`
	SelectedOption        OptionBlockObject   `json:"selected_option"`
	SelectedOptions       []OptionBlockObject `json:"selected_options"`
	SelectedUser          string              `json:"selected_user"`
	SelectedUsers         []string            `json:"selected_users"`
	SelectedChannel       string              `json:"selected_channel"`
	SelectedChannels      []string            `json:"selected_channels"`
	SelectedConversation  string              `json:"selected_conversation"`
	SelectedConversations []string            `json:"selected_conversations"`
	SelectedDate          string              `json:"selected_date"`
	SelectedTime          string              `json:"selected_time"`
	InitialOption         OptionBlockObject   `json:"initial_option"`
	InitialUser           string              `json:"initial_user"`
	InitialChannel        string              `json:"initial_channel"`
	InitialConversation   string              `json:"initial_conversation"`
	InitialDate           string              `json:"initial_date"`
	InitialTime           string              `json:"initial_time"`
}

// actionType returns the type of the action
func (b BlockAction) actionType() ActionType {
	return b.Type
}

// NewBlockMessage creates a new Message that contains one or more blocks to be displayed
func NewBlockMessage(blocks ...Block) Message {
	return Message{
		Msg: Msg{
			Blocks: Blocks{
				BlockSet: blocks,
			},
		},
	}
}

// AddBlockMessage appends a block to the end of the existing list of blocks
func AddBlockMessage(message Message, newBlk Block) Message {
	message.Msg.Blocks.BlockSet = append(message.Msg.Blocks.BlockSet, newBlk)
	return message
}
