package slack

// TextInputSubtype Accepts email, number, tel, or url. In some form factors, optimized input is provided for this subtype.
type TextInputSubtype string

// TextInputOption handle to extra inputs options.
type TextInputOption func(*TextInputElement)

const (
	// InputSubtypeEmail email keyboard
	InputSubtypeEmail TextInputSubtype = "email"
	// InputSubtypeNumber numeric keyboard
	InputSubtypeNumber TextInputSubtype = "number"
	// InputSubtypeTel Phone keyboard
	InputSubtypeTel TextInputSubtype = "tel"
	// InputSubtypeURL Phone keyboard
	InputSubtypeURL TextInputSubtype = "url"
)

// TextInputElement subtype of DialogInput
//	https://api.slack.com/dialogs#option_element_attributes#text_element_attributes
type TextInputElement struct {
	DialogInput
	MaxLength int              `json:"max_length,omitempty"`
	MinLength int              `json:"min_length,omitempty"`
	Hint      string           `json:"hint,omitempty"`
	Subtype   TextInputSubtype `json:"subtype"`
	Value     string           `json:"value"`
}

// NewTextInput constructor for a `text` input
func NewTextInput(name, label, text string, options ...TextInputOption) *TextInputElement {
	t := &TextInputElement{
		DialogInput: DialogInput{
			Type:  InputTypeText,
			Name:  name,
			Label: label,
		},
		Value: text,
	}

	for _, opt := range options {
		opt(t)
	}

	return t
}

// NewTextAreaInput constructor for a `textarea` input
func NewTextAreaInput(name, label, text string) *TextInputElement {
	return &TextInputElement{
		DialogInput: DialogInput{
			Type:  InputTypeTextArea,
			Name:  name,
			Label: label,
		},
		Value: text,
	}
}
