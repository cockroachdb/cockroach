package slack

// InputBlock defines data that is used to display user input fields.
//
// More Information: https://api.slack.com/reference/block-kit/blocks#input
type InputBlock struct {
	Type           MessageBlockType `json:"type"`
	BlockID        string           `json:"block_id,omitempty"`
	Label          *TextBlockObject `json:"label"`
	Element        BlockElement     `json:"element"`
	Hint           *TextBlockObject `json:"hint,omitempty"`
	Optional       bool             `json:"optional,omitempty"`
	DispatchAction bool             `json:"dispatch_action,omitempty"`
}

// BlockType returns the type of the block
func (s InputBlock) BlockType() MessageBlockType {
	return s.Type
}

// NewInputBlock returns a new instance of an input block
func NewInputBlock(blockID string, label *TextBlockObject, element BlockElement) *InputBlock {
	return &InputBlock{
		Type:    MBTInput,
		BlockID: blockID,
		Label:   label,
		Element: element,
	}
}
