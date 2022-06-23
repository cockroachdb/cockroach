package slack

import "encoding/json"

// AttachmentField contains information for an attachment field
// An Attachment can contain multiple of these
type AttachmentField struct {
	Title string `json:"title"`
	Value string `json:"value"`
	Short bool   `json:"short"`
}

// AttachmentAction is a button or menu to be included in the attachment. Required when
// using message buttons or menus and otherwise not useful. A maximum of 5 actions may be
// provided per attachment.
type AttachmentAction struct {
	Name            string                        `json:"name"`                       // Required.
	Text            string                        `json:"text"`                       // Required.
	Style           string                        `json:"style,omitempty"`            // Optional. Allowed values: "default", "primary", "danger".
	Type            ActionType                    `json:"type"`                       // Required. Must be set to "button" or "select".
	Value           string                        `json:"value,omitempty"`            // Optional.
	DataSource      string                        `json:"data_source,omitempty"`      // Optional.
	MinQueryLength  int                           `json:"min_query_length,omitempty"` // Optional. Default value is 1.
	Options         []AttachmentActionOption      `json:"options,omitempty"`          // Optional. Maximum of 100 options can be provided in each menu.
	SelectedOptions []AttachmentActionOption      `json:"selected_options,omitempty"` // Optional. The first element of this array will be set as the pre-selected option for this menu.
	OptionGroups    []AttachmentActionOptionGroup `json:"option_groups,omitempty"`    // Optional.
	Confirm         *ConfirmationField            `json:"confirm,omitempty"`          // Optional.
	URL             string                        `json:"url,omitempty"`              // Optional.
}

// actionType returns the type of the action
func (a AttachmentAction) actionType() ActionType {
	return a.Type
}

// AttachmentActionOption the individual option to appear in action menu.
type AttachmentActionOption struct {
	Text        string `json:"text"`                  // Required.
	Value       string `json:"value"`                 // Required.
	Description string `json:"description,omitempty"` // Optional. Up to 30 characters.
}

// AttachmentActionOptionGroup is a semi-hierarchal way to list available options to appear in action menu.
type AttachmentActionOptionGroup struct {
	Text    string                   `json:"text"`    // Required.
	Options []AttachmentActionOption `json:"options"` // Required.
}

// AttachmentActionCallback is sent from Slack when a user clicks a button in an interactive message (aka AttachmentAction)
// DEPRECATED: use InteractionCallback
type AttachmentActionCallback InteractionCallback

// ConfirmationField are used to ask users to confirm actions
type ConfirmationField struct {
	Title       string `json:"title,omitempty"`        // Optional.
	Text        string `json:"text"`                   // Required.
	OkText      string `json:"ok_text,omitempty"`      // Optional. Defaults to "Okay"
	DismissText string `json:"dismiss_text,omitempty"` // Optional. Defaults to "Cancel"
}

// Attachment contains all the information for an attachment
type Attachment struct {
	Color    string `json:"color,omitempty"`
	Fallback string `json:"fallback,omitempty"`

	CallbackID string `json:"callback_id,omitempty"`
	ID         int    `json:"id,omitempty"`

	AuthorID      string `json:"author_id,omitempty"`
	AuthorName    string `json:"author_name,omitempty"`
	AuthorSubname string `json:"author_subname,omitempty"`
	AuthorLink    string `json:"author_link,omitempty"`
	AuthorIcon    string `json:"author_icon,omitempty"`

	Title     string `json:"title,omitempty"`
	TitleLink string `json:"title_link,omitempty"`
	Pretext   string `json:"pretext,omitempty"`
	Text      string `json:"text,omitempty"`

	ImageURL string `json:"image_url,omitempty"`
	ThumbURL string `json:"thumb_url,omitempty"`

	ServiceName string `json:"service_name,omitempty"`
	ServiceIcon string `json:"service_icon,omitempty"`
	FromURL     string `json:"from_url,omitempty"`
	OriginalURL string `json:"original_url,omitempty"`

	Fields     []AttachmentField  `json:"fields,omitempty"`
	Actions    []AttachmentAction `json:"actions,omitempty"`
	MarkdownIn []string           `json:"mrkdwn_in,omitempty"`

	Blocks Blocks `json:"blocks,omitempty"`

	Footer     string `json:"footer,omitempty"`
	FooterIcon string `json:"footer_icon,omitempty"`

	Ts json.Number `json:"ts,omitempty"`
}
