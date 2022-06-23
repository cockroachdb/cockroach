package pgproto3

import (
	"encoding/hex"
	"encoding/json"

	"github.com/jackc/pgio"
)

type SASLResponse struct {
	Data []byte
}

// Frontend identifies this message as sendable by a PostgreSQL frontend.
func (*SASLResponse) Frontend() {}

// Decode decodes src into dst. src must contain the complete message with the exception of the initial 1 byte message
// type identifier and 4 byte message length.
func (dst *SASLResponse) Decode(src []byte) error {
	*dst = SASLResponse{Data: src}
	return nil
}

// Encode encodes src into dst. dst will include the 1 byte message type identifier and the 4 byte message length.
func (src *SASLResponse) Encode(dst []byte) []byte {
	dst = append(dst, 'p')
	dst = pgio.AppendInt32(dst, int32(4+len(src.Data)))

	dst = append(dst, src.Data...)

	return dst
}

// MarshalJSON implements encoding/json.Marshaler.
func (src SASLResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
		Data string
	}{
		Type: "SASLResponse",
		Data: string(src.Data),
	})
}

// UnmarshalJSON implements encoding/json.Unmarshaler.
func (dst *SASLResponse) UnmarshalJSON(data []byte) error {
	var msg struct {
		Data string
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		return err
	}
	if msg.Data != "" {
		decoded, err := hex.DecodeString(msg.Data)
		if err != nil {
			return err
		}
		dst.Data = decoded
	}
	return nil
}
