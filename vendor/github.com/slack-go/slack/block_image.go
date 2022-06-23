package slack

// ImageBlock defines data required to display an image as a block element
//
// More Information: https://api.slack.com/reference/messaging/blocks#image
type ImageBlock struct {
	Type     MessageBlockType `json:"type"`
	ImageURL string           `json:"image_url"`
	AltText  string           `json:"alt_text"`
	BlockID  string           `json:"block_id,omitempty"`
	Title    *TextBlockObject `json:"title,omitempty"`
}

// BlockType returns the type of the block
func (s ImageBlock) BlockType() MessageBlockType {
	return s.Type
}

// NewImageBlock returns an instance of a new Image Block type
func NewImageBlock(imageURL, altText, blockID string, title *TextBlockObject) *ImageBlock {
	return &ImageBlock{
		Type:     MBTImage,
		ImageURL: imageURL,
		AltText:  altText,
		BlockID:  blockID,
		Title:    title,
	}
}
