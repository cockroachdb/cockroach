// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descpb

import "github.com/cockroachdb/errors"

// Proto wire types.
const (
	wireVarint = 0
	wireBytes  = 2
)

// Proto field numbers for Descriptor wrapper (oneof union).
const (
	descriptorTableField    = 1
	descriptorDatabaseField = 2
)

// Proto field number for TableDescriptor.parent_id.
const tableParentIDField = 4

// ExtractParentDatabaseID extracts the parent database ID from raw
// descriptor bytes without full unmarshaling. This is an optimization
// for filtering descriptors by parent database before expensive unmarshal.
//
// Returns:
//   - parentID: the parent database ID (0 for databases or non-tables)
//   - isTable: true if this is a TableDescriptor
//   - err: parsing error (returns false, 0, nil for unknown/non-table types)
func ExtractParentDatabaseID(descBytes []byte) (parentID ID, isTable bool, err error) {
	if len(descBytes) == 0 {
		return 0, false, nil
	}

	// Parse the Descriptor wrapper to find which oneof field is present.
	data := descBytes
	for len(data) > 0 {
		// Read the tag (field number and wire type).
		tag, n := decodeVarint(data)
		if n == 0 {
			return 0, false, errors.New("failed to decode tag")
		}
		data = data[n:]

		fieldNum := tag >> 3
		wireType := tag & 0x7

		switch fieldNum {
		case descriptorTableField:
			// This is a TableDescriptor.
			if wireType != wireBytes {
				return 0, false, errors.Newf("unexpected wire type %d for table field", wireType)
			}
			// Read the length-prefixed TableDescriptor bytes.
			length, n := decodeVarint(data)
			if n == 0 {
				return 0, false, errors.New("failed to decode table length")
			}
			data = data[n:]
			if uint64(len(data)) < length {
				return 0, false, errors.New("table bytes truncated")
			}
			tableBytes := data[:length]

			// Extract parent_id from TableDescriptor.
			parentID, err := extractTableParentID(tableBytes)
			if err != nil {
				return 0, false, err
			}
			return parentID, true, nil

		case descriptorDatabaseField:
			// This is a DatabaseDescriptor - databases have no parent.
			return 0, false, nil

		default:
			// Skip this field and continue looking.
			data, err = skipField(data, int(wireType))
			if err != nil {
				return 0, false, err
			}
		}
	}

	// No recognized descriptor type found.
	return 0, false, nil
}

// extractTableParentID extracts the parent_id field from TableDescriptor bytes.
func extractTableParentID(tableBytes []byte) (ID, error) {
	data := tableBytes
	for len(data) > 0 {
		tag, n := decodeVarint(data)
		if n == 0 {
			return 0, errors.New("failed to decode tag in table")
		}
		data = data[n:]

		fieldNum := tag >> 3
		wireType := tag & 0x7

		if fieldNum == tableParentIDField {
			// Found parent_id field.
			if wireType != wireVarint {
				return 0, errors.Newf("unexpected wire type %d for parent_id", wireType)
			}
			value, n := decodeVarint(data)
			if n == 0 {
				return 0, errors.New("failed to decode parent_id value")
			}
			return ID(value), nil
		}

		// Skip this field.
		var err error
		data, err = skipField(data, int(wireType))
		if err != nil {
			return 0, err
		}
	}

	// parent_id not found, default to 0.
	return 0, nil
}

// decodeVarint decodes a varint from the front of data.
// Returns the value and the number of bytes consumed.
// Returns 0, 0 if the buffer is too small or the varint is malformed.
func decodeVarint(data []byte) (uint64, int) {
	var value uint64
	var shift uint
	for i := 0; i < len(data) && i < 10; i++ {
		b := data[i]
		value |= uint64(b&0x7F) << shift
		if b < 0x80 {
			return value, i + 1
		}
		shift += 7
	}
	return 0, 0
}

// skipField skips over a field value based on its wire type.
// Returns the remaining data after the field.
func skipField(data []byte, wireType int) ([]byte, error) {
	switch wireType {
	case 0: // Varint
		_, n := decodeVarint(data)
		if n == 0 {
			return nil, errors.New("failed to skip varint")
		}
		return data[n:], nil
	case 1: // 64-bit fixed
		if len(data) < 8 {
			return nil, errors.New("data too short for 64-bit field")
		}
		return data[8:], nil
	case 2: // Length-delimited (bytes, string, embedded message)
		length, n := decodeVarint(data)
		if n == 0 {
			return nil, errors.New("failed to decode length")
		}
		data = data[n:]
		if uint64(len(data)) < length {
			return nil, errors.New("data too short for bytes field")
		}
		return data[length:], nil
	case 5: // 32-bit fixed
		if len(data) < 4 {
			return nil, errors.New("data too short for 32-bit field")
		}
		return data[4:], nil
	default:
		return nil, errors.Newf("unknown wire type %d", wireType)
	}
}
