package slack

// ContextBlock defines data that is used to display message context, which can
// include both images and text.
//
// More Information: https://api.slack.com/reference/messaging/blocks#context
type ContextBlock struct {
	Type            MessageBlockType `json:"type"`
	BlockID         string           `json:"block_id,omitempty"`
	ContextElements ContextElements  `json:"elements"`
}

// BlockType returns the type of the block
func (s ContextBlock) BlockType() MessageBlockType {
	return s.Type
}

type ContextElements struct {
	Elements []MixedElement
}

// NewContextBlock returns a new instance of a context block
func NewContextBlock(blockID string, mixedElements ...MixedElement) *ContextBlock {
	elements := ContextElements{
		Elements: mixedElements,
	}
	return &ContextBlock{
		Type:            MBTContext,
		BlockID:         blockID,
		ContextElements: elements,
	}
}
