package slack

// UnknownBlock represents a block type that is not yet known. This block type exists to prevent Slack from introducing
// new and unknown block types that break this library.
type UnknownBlock struct {
	Type    MessageBlockType `json:"type"`
	BlockID string           `json:"block_id,omitempty"`
}

// BlockType returns the type of the block
func (b UnknownBlock) BlockType() MessageBlockType {
	return b.Type
}
