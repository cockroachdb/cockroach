package slack

// FileBlock defines data that is used to display a remote file.
//
// More Information: https://api.slack.com/reference/block-kit/blocks#file
type FileBlock struct {
	Type       MessageBlockType `json:"type"`
	BlockID    string           `json:"block_id,omitempty"`
	ExternalID string           `json:"external_id"`
	Source     string           `json:"source"`
}

// BlockType returns the type of the block
func (s FileBlock) BlockType() MessageBlockType {
	return s.Type
}

// NewFileBlock returns a new instance of a file block
func NewFileBlock(blockID string, externalID string, source string) *FileBlock {
	return &FileBlock{
		Type:       MBTFile,
		BlockID:    blockID,
		ExternalID: externalID,
		Source:     source,
	}
}
