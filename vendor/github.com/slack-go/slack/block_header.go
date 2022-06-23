package slack

// HeaderBlock defines a new block of type header
//
// More Information: https://api.slack.com/reference/messaging/blocks#header
type HeaderBlock struct {
	Type    MessageBlockType `json:"type"`
	Text    *TextBlockObject `json:"text,omitempty"`
	BlockID string           `json:"block_id,omitempty"`
}

// BlockType returns the type of the block
func (s HeaderBlock) BlockType() MessageBlockType {
	return s.Type
}

// HeaderBlockOption allows configuration of options for a new header block
type HeaderBlockOption func(*HeaderBlock)

func HeaderBlockOptionBlockID(blockID string) HeaderBlockOption {
	return func(block *HeaderBlock) {
		block.BlockID = blockID
	}
}

// NewHeaderBlock returns a new instance of a header block to be rendered
func NewHeaderBlock(textObj *TextBlockObject, options ...HeaderBlockOption) *HeaderBlock {
	block := HeaderBlock{
		Type: MBTHeader,
		Text: textObj,
	}

	for _, option := range options {
		option(&block)
	}

	return &block
}
