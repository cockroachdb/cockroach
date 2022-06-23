package slack

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

type sumtype struct {
	TypeVal string `json:"type"`
}

// MarshalJSON implements the Marshaller interface for Blocks so that any JSON
// marshalling is delegated and proper type determination can be made before marshal
func (b Blocks) MarshalJSON() ([]byte, error) {
	bytes, err := json.Marshal(b.BlockSet)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

// UnmarshalJSON implements the Unmarshaller interface for Blocks, so that any JSON
// unmarshalling is delegated and proper type determination can be made before unmarshal
func (b *Blocks) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage

	if string(data) == "{}" {
		return nil
	}

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return err
	}

	var blocks Blocks
	for _, r := range raw {
		s := sumtype{}
		err := json.Unmarshal(r, &s)
		if err != nil {
			return err
		}

		var blockType string
		if s.TypeVal != "" {
			blockType = s.TypeVal
		}

		var block Block
		switch blockType {
		case "actions":
			block = &ActionBlock{}
		case "context":
			block = &ContextBlock{}
		case "divider":
			block = &DividerBlock{}
		case "file":
			block = &FileBlock{}
		case "header":
			block = &HeaderBlock{}
		case "image":
			block = &ImageBlock{}
		case "input":
			block = &InputBlock{}
		case "section":
			block = &SectionBlock{}
		default:
			block = &UnknownBlock{}
		}

		err = json.Unmarshal(r, block)
		if err != nil {
			return err
		}

		blocks.BlockSet = append(blocks.BlockSet, block)
	}

	*b = blocks
	return nil
}

// UnmarshalJSON implements the Unmarshaller interface for InputBlock, so that any JSON
// unmarshalling is delegated and proper type determination can be made before unmarshal
func (b *InputBlock) UnmarshalJSON(data []byte) error {
	type alias InputBlock
	a := struct {
		Element json.RawMessage `json:"element"`
		*alias
	}{
		alias: (*alias)(b),
	}

	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}

	s := sumtype{}
	if err := json.Unmarshal(a.Element, &s); err != nil {
		return nil
	}

	var e BlockElement
	switch s.TypeVal {
	case "datepicker":
		e = &DatePickerBlockElement{}
	case "timepicker":
		e = &TimePickerBlockElement{}
	case "plain_text_input":
		e = &PlainTextInputBlockElement{}
	case "static_select", "external_select", "users_select", "conversations_select", "channels_select":
		e = &SelectBlockElement{}
	case "multi_static_select", "multi_external_select", "multi_users_select", "multi_conversations_select", "multi_channels_select":
		e = &MultiSelectBlockElement{}
	case "checkboxes":
		e = &CheckboxGroupsBlockElement{}
	case "overflow":
		e = &OverflowBlockElement{}
	case "radio_buttons":
		e = &RadioButtonsBlockElement{}
	default:
		return errors.New("unsupported block element type")
	}

	if err := json.Unmarshal(a.Element, e); err != nil {
		return err
	}
	b.Element = e

	return nil
}

// MarshalJSON implements the Marshaller interface for BlockElements so that any JSON
// marshalling is delegated and proper type determination can be made before marshal
func (b *BlockElements) MarshalJSON() ([]byte, error) {
	bytes, err := json.Marshal(b.ElementSet)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

// UnmarshalJSON implements the Unmarshaller interface for BlockElements, so that any JSON
// unmarshalling is delegated and proper type determination can be made before unmarshal
func (b *BlockElements) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage

	if string(data) == "{}" {
		return nil
	}

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return err
	}

	var blockElements BlockElements
	for _, r := range raw {
		s := sumtype{}
		err := json.Unmarshal(r, &s)
		if err != nil {
			return err
		}

		var blockElementType string
		if s.TypeVal != "" {
			blockElementType = s.TypeVal
		}

		var blockElement BlockElement
		switch blockElementType {
		case "image":
			blockElement = &ImageBlockElement{}
		case "button":
			blockElement = &ButtonBlockElement{}
		case "overflow":
			blockElement = &OverflowBlockElement{}
		case "datepicker":
			blockElement = &DatePickerBlockElement{}
		case "timepicker":
			blockElement = &TimePickerBlockElement{}
		case "plain_text_input":
			blockElement = &PlainTextInputBlockElement{}
		case "checkboxes":
			blockElement = &CheckboxGroupsBlockElement{}
		case "radio_buttons":
			blockElement = &RadioButtonsBlockElement{}
		case "static_select", "external_select", "users_select", "conversations_select", "channels_select":
			blockElement = &SelectBlockElement{}
		default:
			return fmt.Errorf("unsupported block element type %v", blockElementType)
		}

		err = json.Unmarshal(r, blockElement)
		if err != nil {
			return err
		}

		blockElements.ElementSet = append(blockElements.ElementSet, blockElement)
	}

	*b = blockElements
	return nil
}

// MarshalJSON implements the Marshaller interface for Accessory so that any JSON
// marshalling is delegated and proper type determination can be made before marshal
func (a *Accessory) MarshalJSON() ([]byte, error) {
	bytes, err := json.Marshal(toBlockElement(a))
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

// UnmarshalJSON implements the Unmarshaller interface for Accessory, so that any JSON
// unmarshalling is delegated and proper type determination can be made before unmarshal
func (a *Accessory) UnmarshalJSON(data []byte) error {
	var r json.RawMessage

	if string(data) == "{\"accessory\":null}" {
		return nil
	}

	err := json.Unmarshal(data, &r)
	if err != nil {
		return err
	}

	s := sumtype{}
	err = json.Unmarshal(r, &s)
	if err != nil {
		return err
	}

	var blockElementType string
	if s.TypeVal != "" {
		blockElementType = s.TypeVal
	}

	switch blockElementType {
	case "image":
		element, err := unmarshalBlockElement(r, &ImageBlockElement{})
		if err != nil {
			return err
		}
		a.ImageElement = element.(*ImageBlockElement)
	case "button":
		element, err := unmarshalBlockElement(r, &ButtonBlockElement{})
		if err != nil {
			return err
		}
		a.ButtonElement = element.(*ButtonBlockElement)
	case "overflow":
		element, err := unmarshalBlockElement(r, &OverflowBlockElement{})
		if err != nil {
			return err
		}
		a.OverflowElement = element.(*OverflowBlockElement)
	case "datepicker":
		element, err := unmarshalBlockElement(r, &DatePickerBlockElement{})
		if err != nil {
			return err
		}
		a.DatePickerElement = element.(*DatePickerBlockElement)
	case "timepicker":
		element, err := unmarshalBlockElement(r, &TimePickerBlockElement{})
		if err != nil {
			return err
		}
		a.TimePickerElement = element.(*TimePickerBlockElement)
	case "plain_text_input":
		element, err := unmarshalBlockElement(r, &PlainTextInputBlockElement{})
		if err != nil {
			return err
		}
		a.PlainTextInputElement = element.(*PlainTextInputBlockElement)
	case "radio_buttons":
		element, err := unmarshalBlockElement(r, &RadioButtonsBlockElement{})
		if err != nil {
			return err
		}
		a.RadioButtonsElement = element.(*RadioButtonsBlockElement)
	case "static_select", "external_select", "users_select", "conversations_select", "channels_select":
		element, err := unmarshalBlockElement(r, &SelectBlockElement{})
		if err != nil {
			return err
		}
		a.SelectElement = element.(*SelectBlockElement)
	case "multi_static_select", "multi_external_select", "multi_users_select", "multi_conversations_select", "multi_channels_select":
		element, err := unmarshalBlockElement(r, &MultiSelectBlockElement{})
		if err != nil {
			return err
		}
		a.MultiSelectElement = element.(*MultiSelectBlockElement)
	case "checkboxes":
		element, err := unmarshalBlockElement(r, &CheckboxGroupsBlockElement{})
		if err != nil {
			return err
		}
		a.CheckboxGroupsBlockElement = element.(*CheckboxGroupsBlockElement)
	default:
		element, err := unmarshalBlockElement(r, &UnknownBlockElement{})
		if err != nil {
			return err
		}
		a.UnknownElement = element.(*UnknownBlockElement)
	}

	return nil
}

func unmarshalBlockElement(r json.RawMessage, element BlockElement) (BlockElement, error) {
	err := json.Unmarshal(r, element)
	if err != nil {
		return nil, err
	}
	return element, nil
}

func toBlockElement(element *Accessory) BlockElement {
	if element.ImageElement != nil {
		return element.ImageElement
	}
	if element.ButtonElement != nil {
		return element.ButtonElement
	}
	if element.OverflowElement != nil {
		return element.OverflowElement
	}
	if element.DatePickerElement != nil {
		return element.DatePickerElement
	}
	if element.TimePickerElement != nil {
		return element.TimePickerElement
	}
	if element.PlainTextInputElement != nil {
		return element.PlainTextInputElement
	}
	if element.RadioButtonsElement != nil {
		return element.RadioButtonsElement
	}
	if element.CheckboxGroupsBlockElement != nil {
		return element.CheckboxGroupsBlockElement
	}
	if element.SelectElement != nil {
		return element.SelectElement
	}
	if element.MultiSelectElement != nil {
		return element.MultiSelectElement
	}

	return nil
}

// MarshalJSON implements the Marshaller interface for ContextElements so that any JSON
// marshalling is delegated and proper type determination can be made before marshal
func (e *ContextElements) MarshalJSON() ([]byte, error) {
	bytes, err := json.Marshal(e.Elements)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

// UnmarshalJSON implements the Unmarshaller interface for ContextElements, so that any JSON
// unmarshalling is delegated and proper type determination can be made before unmarshal
func (e *ContextElements) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage

	if string(data) == "{\"elements\":null}" {
		return nil
	}

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return err
	}

	for _, r := range raw {
		s := sumtype{}
		err := json.Unmarshal(r, &s)
		if err != nil {
			return err
		}

		var contextElementType string
		if s.TypeVal != "" {
			contextElementType = s.TypeVal
		}

		switch contextElementType {
		case PlainTextType, MarkdownType:
			elem, err := unmarshalBlockObject(r, &TextBlockObject{})
			if err != nil {
				return err
			}

			e.Elements = append(e.Elements, elem.(*TextBlockObject))
		case "image":
			elem, err := unmarshalBlockElement(r, &ImageBlockElement{})
			if err != nil {
				return err
			}

			e.Elements = append(e.Elements, elem.(*ImageBlockElement))
		default:
			return errors.New("unsupported context element type")
		}
	}

	return nil
}
