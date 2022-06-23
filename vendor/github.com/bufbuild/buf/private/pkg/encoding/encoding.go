// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package encoding provides encoding utilities.
package encoding

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"go.uber.org/multierr"
	"gopkg.in/yaml.v3"
)

// UnmarshalJSONStrict unmarshals the data as JSON, returning a user error on failure.
//
// If the data length is 0, this is a no-op.
func UnmarshalJSONStrict(data []byte, v interface{}) error {
	if len(data) == 0 {
		return nil
	}
	jsonDecoder := json.NewDecoder(bytes.NewReader(data))
	jsonDecoder.DisallowUnknownFields()
	if err := jsonDecoder.Decode(v); err != nil {
		return fmt.Errorf("could not unmarshal as JSON: %v", err)
	}
	return nil
}

// UnmarshalYAMLStrict unmarshals the data as YAML, returning a user error on failure.
//
// If the data length is 0, this is a no-op.
func UnmarshalYAMLStrict(data []byte, v interface{}) error {
	if len(data) == 0 {
		return nil
	}
	yamlDecoder := NewYAMLDecoderStrict(bytes.NewReader(data))
	if err := yamlDecoder.Decode(v); err != nil {
		return fmt.Errorf("could not unmarshal as YAML: %v", err)
	}
	return nil
}

// UnmarshalJSONOrYAMLStrict unmarshals the data as JSON or YAML in order, returning
// a user error with both errors on failure.
//
// If the data length is 0, this is a no-op.
func UnmarshalJSONOrYAMLStrict(data []byte, v interface{}) error {
	if len(data) == 0 {
		return nil
	}
	if jsonErr := UnmarshalJSONStrict(data, v); jsonErr != nil {
		if yamlErr := UnmarshalYAMLStrict(data, v); yamlErr != nil {
			return errors.New(jsonErr.Error() + "\n" + yamlErr.Error())
		}
	}
	return nil
}

// UnmarshalJSONNonStrict unmarshals the data as JSON, returning a user error on failure.
//
// If the data length is 0, this is a no-op.
func UnmarshalJSONNonStrict(data []byte, v interface{}) error {
	if len(data) == 0 {
		return nil
	}
	jsonDecoder := json.NewDecoder(bytes.NewReader(data))
	if err := jsonDecoder.Decode(v); err != nil {
		return fmt.Errorf("could not unmarshal as JSON: %v", err)
	}
	return nil
}

// UnmarshalYAMLNonStrict unmarshals the data as YAML, returning a user error on failure.
//
// If the data length is 0, this is a no-op.
func UnmarshalYAMLNonStrict(data []byte, v interface{}) error {
	if len(data) == 0 {
		return nil
	}
	yamlDecoder := NewYAMLDecoderNonStrict(bytes.NewReader(data))
	if err := yamlDecoder.Decode(v); err != nil {
		return fmt.Errorf("could not unmarshal as YAML: %v", err)
	}
	return nil
}

// UnmarshalJSONOrYAMLNonStrict unmarshals the data as JSON or YAML in order, returning
// a user error with both errors on failure.
//
// If the data length is 0, this is a no-op.
func UnmarshalJSONOrYAMLNonStrict(data []byte, v interface{}) error {
	if len(data) == 0 {
		return nil
	}
	if jsonErr := UnmarshalJSONNonStrict(data, v); jsonErr != nil {
		if yamlErr := UnmarshalYAMLNonStrict(data, v); yamlErr != nil {
			return multierr.Append(jsonErr, yamlErr)
		}
	}
	return nil
}

// GetJSONStringOrStringValue returns the JSON string for the RawMessage if the
// RawMessage is a string, and the raw value as a string otherwise.
//
// If the RawMessage is empty, this returns "".
func GetJSONStringOrStringValue(rawMessage json.RawMessage) string {
	if len(rawMessage) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(rawMessage, &s); err == nil {
		return s
	}
	return string(rawMessage)
}

// MarshalYAML marshals the given value into YAML.
func MarshalYAML(v interface{}) (_ []byte, retErr error) {
	buffer := bytes.NewBuffer(nil)
	yamlEncoder := NewYAMLEncoder(buffer)
	defer func() {
		retErr = multierr.Append(retErr, yamlEncoder.Close())
	}()
	if err := yamlEncoder.Encode(v); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// NewYAMLEncoder creates a new YAML encoder reader from the Writer.
// The encoder must be closed after use.
func NewYAMLEncoder(writer io.Writer) *yaml.Encoder {
	yamlEncoder := yaml.NewEncoder(writer)
	yamlEncoder.SetIndent(2)
	return yamlEncoder
}

// NewYAMLDecoderStrict creates a new YAML decoder from the reader.
func NewYAMLDecoderStrict(reader io.Reader) *yaml.Decoder {
	yamlDecoder := yaml.NewDecoder(reader)
	yamlDecoder.KnownFields(true)
	return yamlDecoder
}

// NewYAMLDecoderNonStrict creates a new YAML decoder from the reader.
func NewYAMLDecoderNonStrict(reader io.Reader) *yaml.Decoder {
	return yaml.NewDecoder(reader)
}

// InterfaceSliceOrStringToCommaSepString parses the input as a
// slice or string into a comma separated string. This is commonly
// used with JSON or YAML fields that need to support both string slices
// and string literals.
func InterfaceSliceOrStringToCommaSepString(in interface{}) (string, error) {
	var opt string
	switch t := in.(type) {
	case string:
		opt = t
	case []interface{}:
		opts := make([]string, len(t))
		for i, elem := range t {
			s, ok := elem.(string)
			if !ok {
				return "", fmt.Errorf("could not convert element %T to a string", elem)
			}
			opts[i] = s
		}
		opt = strings.Join(opts, ",")
	case nil:
		// If the variable is omitted, the value is nil
	default:
		return "", fmt.Errorf("unknown type %T for opt", t)
	}
	return opt, nil
}
