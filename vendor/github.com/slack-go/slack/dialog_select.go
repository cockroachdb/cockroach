package slack

// SelectDataSource types of select datasource
type SelectDataSource string

const (
	// DialogDataSourceStatic menu with static Options/OptionGroups
	DialogDataSourceStatic SelectDataSource = "static"
	// DialogDataSourceExternal dynamic datasource
	DialogDataSourceExternal SelectDataSource = "external"
	// DialogDataSourceConversations provides a list of conversations
	DialogDataSourceConversations SelectDataSource = "conversations"
	// DialogDataSourceChannels provides a list of channels
	DialogDataSourceChannels SelectDataSource = "channels"
	// DialogDataSourceUsers provides a list of users
	DialogDataSourceUsers SelectDataSource = "users"
)

// DialogInputSelect dialog support for select boxes.
type DialogInputSelect struct {
	DialogInput
	Value           string               `json:"value,omitempty"`            //Optional.
	DataSource      SelectDataSource     `json:"data_source,omitempty"`      //Optional. Allowed values: "users", "channels", "conversations", "external".
	SelectedOptions []DialogSelectOption `json:"selected_options,omitempty"` //Optional. May hold at most one element, for use with "external" only.
	Options         []DialogSelectOption `json:"options,omitempty"`          //One of options or option_groups is required.
	OptionGroups    []DialogOptionGroup  `json:"option_groups,omitempty"`    //Provide up to 100 options.
	MinQueryLength  int                  `json:"min_query_length,omitempty"` //Optional. minimum characters before query is sent.
	Hint            string               `json:"hint,omitempty"`             //Optional. Additional hint text.
}

// DialogSelectOption is an option for the user to select from the menu
type DialogSelectOption struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

// DialogOptionGroup is a collection of options for creating a segmented table
type DialogOptionGroup struct {
	Label   string               `json:"label"`
	Options []DialogSelectOption `json:"options"`
}

// NewStaticSelectDialogInput constructor for a `static` datasource menu input
func NewStaticSelectDialogInput(name, label string, options []DialogSelectOption) *DialogInputSelect {
	return &DialogInputSelect{
		DialogInput: DialogInput{
			Type:     InputTypeSelect,
			Name:     name,
			Label:    label,
			Optional: true,
		},
		DataSource: DialogDataSourceStatic,
		Options:    options,
	}
}

// NewExternalSelectDialogInput constructor for a `external` datasource menu input
func NewExternalSelectDialogInput(name, label string, options []DialogSelectOption) *DialogInputSelect {
	return &DialogInputSelect{
		DialogInput: DialogInput{
			Type:     InputTypeSelect,
			Name:     name,
			Label:    label,
			Optional: true,
		},
		DataSource: DialogDataSourceExternal,
		Options:    options,
	}
}

// NewGroupedSelectDialogInput creates grouped options select input for Dialogs.
func NewGroupedSelectDialogInput(name, label string, options []DialogOptionGroup) *DialogInputSelect {
	return &DialogInputSelect{
		DialogInput: DialogInput{
			Type:  InputTypeSelect,
			Name:  name,
			Label: label,
		},
		DataSource:   DialogDataSourceStatic,
		OptionGroups: options}
}

// NewDialogOptionGroup creates a DialogOptionGroup from several select options
func NewDialogOptionGroup(label string, options ...DialogSelectOption) DialogOptionGroup {
	return DialogOptionGroup{
		Label:   label,
		Options: options,
	}
}

// NewConversationsSelect returns a `Conversations` select
func NewConversationsSelect(name, label string) *DialogInputSelect {
	return newPresetSelect(name, label, DialogDataSourceConversations)
}

// NewChannelsSelect returns a `Channels` select
func NewChannelsSelect(name, label string) *DialogInputSelect {
	return newPresetSelect(name, label, DialogDataSourceChannels)
}

// NewUsersSelect returns a `Users` select
func NewUsersSelect(name, label string) *DialogInputSelect {
	return newPresetSelect(name, label, DialogDataSourceUsers)
}

func newPresetSelect(name, label string, dataSourceType SelectDataSource) *DialogInputSelect {
	return &DialogInputSelect{
		DialogInput: DialogInput{
			Type:  InputTypeSelect,
			Label: label,
			Name:  name,
		},
		DataSource: dataSourceType,
	}
}
