package slack

import (
	"bytes"
	"encoding/json"
)

// InteractionType type of interactions
type InteractionType string

// ActionType type represents the type of action (attachment, block, etc.)
type ActionType string

// action is an interface that should be implemented by all callback action types
type action interface {
	actionType() ActionType
}

// Types of interactions that can be received.
const (
	InteractionTypeDialogCancellation = InteractionType("dialog_cancellation")
	InteractionTypeDialogSubmission   = InteractionType("dialog_submission")
	InteractionTypeDialogSuggestion   = InteractionType("dialog_suggestion")
	InteractionTypeInteractionMessage = InteractionType("interactive_message")
	InteractionTypeMessageAction      = InteractionType("message_action")
	InteractionTypeBlockActions       = InteractionType("block_actions")
	InteractionTypeBlockSuggestion    = InteractionType("block_suggestion")
	InteractionTypeViewSubmission     = InteractionType("view_submission")
	InteractionTypeViewClosed         = InteractionType("view_closed")
	InteractionTypeShortcut           = InteractionType("shortcut")
)

// InteractionCallback is sent from slack when a user interactions with a button or dialog.
type InteractionCallback struct {
	Type            InteractionType `json:"type"`
	Token           string          `json:"token"`
	CallbackID      string          `json:"callback_id"`
	ResponseURL     string          `json:"response_url"`
	TriggerID       string          `json:"trigger_id"`
	ActionTs        string          `json:"action_ts"`
	Team            Team            `json:"team"`
	Channel         Channel         `json:"channel"`
	User            User            `json:"user"`
	OriginalMessage Message         `json:"original_message"`
	Message         Message         `json:"message"`
	Name            string          `json:"name"`
	Value           string          `json:"value"`
	MessageTs       string          `json:"message_ts"`
	AttachmentID    string          `json:"attachment_id"`
	ActionCallback  ActionCallbacks `json:"actions"`
	View            View            `json:"view"`
	ActionID        string          `json:"action_id"`
	APIAppID        string          `json:"api_app_id"`
	BlockID         string          `json:"block_id"`
	Container       Container       `json:"container"`
	Enterprise      Enterprise      `json:"enterprise"`
	DialogSubmissionCallback
	ViewSubmissionCallback
	ViewClosedCallback

	// FIXME(kanata2): just workaround for backward-compatibility.
	// See also https://github.com/slack-go/slack/issues/816
	RawState json.RawMessage `json:"state,omitempty"`

	// BlockActionState stands for the `state` field in block_actions type.
	// NOTE: InteractionCallback.State has a role for the state of dialog_submission type,
	// so we cannot use this field for backward-compatibility for now.
	BlockActionState *BlockActionStates `json:"-"`
}

type BlockActionStates struct {
	Values map[string]map[string]BlockAction `json:"values"`
}

func (ic *InteractionCallback) MarshalJSON() ([]byte, error) {
	type alias InteractionCallback
	tmp := alias(*ic)
	if tmp.Type == InteractionTypeBlockActions {
		if tmp.BlockActionState == nil {
			tmp.RawState = []byte(`{}`)
		} else {
			state, err := json.Marshal(tmp.BlockActionState.Values)
			if err != nil {
				return nil, err
			}
			tmp.RawState = []byte(`{"values":` + string(state) + `}`)
		}
	} else if ic.Type == InteractionTypeDialogSubmission {
		tmp.RawState = []byte(tmp.State)
	}
	// Use pointer for go1.7
	return json.Marshal(&tmp)
}

func (ic *InteractionCallback) UnmarshalJSON(b []byte) error {
	type alias InteractionCallback
	tmp := struct {
		Type InteractionType `json:"type"`
		*alias
	}{
		alias: (*alias)(ic),
	}
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}
	*ic = InteractionCallback(*tmp.alias)
	ic.Type = tmp.Type
	if ic.Type == InteractionTypeBlockActions {
		if len(ic.RawState) > 0 {
			err := json.Unmarshal(ic.RawState, &ic.BlockActionState)
			if err != nil {
				return err
			}
		}
	} else if ic.Type == InteractionTypeDialogSubmission {
		ic.State = string(ic.RawState)
	}
	return nil
}

type Container struct {
	Type         string      `json:"type"`
	ViewID       string      `json:"view_id"`
	MessageTs    string      `json:"message_ts"`
	ThreadTs     string      `json:"thread_ts,omitempty"`
	AttachmentID json.Number `json:"attachment_id"`
	ChannelID    string      `json:"channel_id"`
	IsEphemeral  bool        `json:"is_ephemeral"`
	IsAppUnfurl  bool        `json:"is_app_unfurl"`
}

type Enterprise struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// ActionCallback is a convenience struct defined to allow dynamic unmarshalling of
// the "actions" value in Slack's JSON response, which varies depending on block type
type ActionCallbacks struct {
	AttachmentActions []*AttachmentAction
	BlockActions      []*BlockAction
}

// MarshalJSON implements the Marshaller interface in order to combine both
// action callback types back into a single array, like how the api responds.
// This makes Marshaling and Unmarshaling an InteractionCallback symmetrical
func (a ActionCallbacks) MarshalJSON() ([]byte, error) {
	count := 0
	length := len(a.AttachmentActions) + len(a.BlockActions)
	buffer := bytes.NewBufferString("[")

	f := func(obj interface{}) error {
		js, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		_, err = buffer.Write(js)
		if err != nil {
			return err
		}

		count++
		if count < length {
			_, err = buffer.WriteString(",")
			return err
		}
		return nil
	}

	for _, act := range a.AttachmentActions {
		err := f(act)
		if err != nil {
			return nil, err
		}
	}
	for _, blk := range a.BlockActions {
		err := f(blk)
		if err != nil {
			return nil, err
		}
	}
	buffer.WriteString("]")
	return buffer.Bytes(), nil
}

// UnmarshalJSON implements the Marshaller interface in order to delegate
// marshalling and allow for proper type assertion when decoding the response
func (a *ActionCallbacks) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return err
	}

	for _, r := range raw {
		var obj map[string]interface{}
		err := json.Unmarshal(r, &obj)
		if err != nil {
			return err
		}

		if _, ok := obj["block_id"].(string); ok {
			action, err := unmarshalAction(r, &BlockAction{})
			if err != nil {
				return err
			}

			a.BlockActions = append(a.BlockActions, action.(*BlockAction))
			continue
		}

		action, err := unmarshalAction(r, &AttachmentAction{})
		if err != nil {
			return err
		}
		a.AttachmentActions = append(a.AttachmentActions, action.(*AttachmentAction))
	}

	return nil
}

func unmarshalAction(r json.RawMessage, callbackAction action) (action, error) {
	err := json.Unmarshal(r, callbackAction)
	if err != nil {
		return nil, err
	}
	return callbackAction, nil
}
