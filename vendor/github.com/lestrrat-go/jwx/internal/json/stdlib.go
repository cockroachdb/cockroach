//go:build !jwx_goccy
// +build !jwx_goccy

package json

import (
	"encoding/json"
	"io"
)

type Decoder = json.Decoder
type Delim = json.Delim
type Encoder = json.Encoder
type Marshaler = json.Marshaler
type Number = json.Number
type RawMessage = json.RawMessage
type Unmarshaler = json.Unmarshaler

func Engine() string {
	return "encoding/json"
}

// NewDecoder respects the values specified in DecoderSettings,
// and creates a Decoder that has certain features turned on/off
func NewDecoder(r io.Reader) *json.Decoder {
	dec := json.NewDecoder(r)

	muGlobalConfig.RLock()
	if useNumber {
		dec.UseNumber()
	}
	muGlobalConfig.RUnlock()

	return dec
}

func NewEncoder(w io.Writer) *json.Encoder {
	return json.NewEncoder(w)
}

// Marshal is just a proxy for "encoding/json".Marshal
func Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// MarshalIndent is just a proxy for "encoding/json".MarshalIndent
func MarshalIndent(v interface{}, prefix, indent string) ([]byte, error) {
	return json.MarshalIndent(v, prefix, indent)
}
