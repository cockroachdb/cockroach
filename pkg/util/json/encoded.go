package json

import (
	"bytes"
	"fmt"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

type jsonEncoded struct {
	// containerLength is only set if this is an object or an array.
	containerLen int
	typ          Type
	// value contains the encoding of this JSON value. In the case of
	// arrays and objects, value contains the container header, but it never
	// contains a scalar container header.
	value []byte
}

func (j jsonEncoded) Type() Type {
	return j.typ
}

func newEncodedFromRoot(v []byte) (jsonEncoded, error) {
	v, typ, err := jsonTypeFromRootBuffer(v)
	if err != nil {
		return jsonEncoded{}, err
	}

	containerLen := -1
	if typ == ArrayJSONType || typ == ObjectJSONType {
		containerHeader, err := getUint32At(v, 0)
		if err != nil {
			return jsonEncoded{}, err
		}
		containerLen = int(containerHeader & containerHeaderLenMask)
	}

	return jsonEncoded{
		typ:          typ,
		containerLen: containerLen,
		value:        v,
	}, nil
}

func jsonTypeFromRootBuffer(v []byte) ([]byte, Type, error) {
	// Root buffers always have a container header.
	containerHeader, err := getUint32At(v, 0)
	if err != nil {
		return v, 0, err
	}
	switch containerHeader & containerHeaderTypeMask {
	case arrayContainerTag:
		return v, ArrayJSONType, nil
	case objectContainerTag:
		return v, ObjectJSONType, nil
	case scalarContainerTag:
		jEntry, err := getUint32At(v, 4)
		if err != nil {
			return v, 0, err
		}
		switch jEntry & jEntryTypeMask {
		case nullTag:
			return v[8:], NullJSONType, nil
		case trueTag:
			return v[8:], TrueJSONType, nil
		case falseTag:
			return v[8:], FalseJSONType, nil
		case numberTag:
			return v[8:], NumberJSONType, nil
		case stringTag:
			return v[8:], StringJSONType, nil
		}
	}
	return nil, 0, pgerror.NewErrorf(pgerror.CodeInternalError, "unknown type %d", containerHeader&containerHeaderTypeMask)
}

func newEncoded(jentry uint32, v []byte) (JSON, error) {
	var typ Type
	var containerLen int
	switch jentry & jEntryTypeMask {
	case stringTag:
		typ = StringJSONType
	case numberTag:
		typ = NumberJSONType

	case nullTag: // Don't bother with returning a jsonEncoded for the singleton types.
		return NullJSONValue, nil
	case falseTag:
		return FalseJSONValue, nil
	case trueTag:
		return TrueJSONValue, nil
	case containerTag:
		containerHeader, err := getUint32At(v, 0)
		if err != nil {
			return nil, err
		}
		switch containerHeader & containerHeaderTypeMask {
		case arrayContainerTag:
			typ = ArrayJSONType
		case objectContainerTag:
			typ = ObjectJSONType
		}
		containerLen = int(containerHeader & containerHeaderLenMask)
	}

	return jsonEncoded{
		typ:          typ,
		containerLen: containerLen,
		value:        v,
	}, nil
}

func getUint32At(v []byte, idx int) (uint32, error) {
	if idx+4 > len(v) {
		return 0, pgerror.NewError(pgerror.CodeInternalError, "insufficient bytes to decode uint32 int value")

	}
	return uint32(v[idx])<<24 |
		uint32(v[idx+1])<<16 |
		uint32(v[idx+2])<<8 |
		uint32(v[idx+3]), nil
}

func (j jsonEncoded) getUint32At(idx int) (uint32, error) {
	return getUint32At(j.value, idx)
}

func (j jsonEncoded) FetchValIdx(idx int) (JSON, error) {
	decoded, err := j.decode()
	if err != nil {
		return nil, err
	}
	return decoded.FetchValIdx(idx)
}

func (j jsonEncoded) FetchValKey(key string) (JSON, error) {
	decoded, err := j.decode()
	if err != nil {
		return nil, err
	}
	return decoded.FetchValKey(key)
}

// decode should be used in cases where you will definitely have to use the
// entire decoded JSON structure, like printing it out to a string.
func (j jsonEncoded) decode() (JSON, error) {
	switch j.typ {
	case NumberJSONType:
		_, j, err := decodeJSONNumber(j.value)
		return j, err
	case StringJSONType:
		return jsonString(j.value), nil
	case TrueJSONType:
		return TrueJSONValue, nil
	case FalseJSONType:
		return FalseJSONValue, nil
	case NullJSONType:
		return NullJSONValue, nil
	}
	_, decoded, err := DecodeJSON(j.value)
	return decoded, err
}

func (j jsonEncoded) TypeAsText() string {
	switch j.typ {
	case StringJSONType:
		return "string"
	case NullJSONType:
		return "null"
	case TrueJSONType:
		return "boolean"
	case FalseJSONType:
		return "boolean"
	case NumberJSONType:
		return "number"
	case ArrayJSONType:
		return "array"
	default:
		return "object"
	}
}

func (j jsonEncoded) AsText() (*string, error) {
	decoded, err := j.decode()
	if err != nil {
		return nil, err
	}
	return decoded.AsText()
}

func (j jsonEncoded) Compare(other JSON) (int, error) {
	decoded, err := j.decode()
	if err != nil {
		return 0, err
	}
	return decoded.Compare(other)
}

func (j jsonEncoded) Exists(key string) (bool, error) {
	decoded, err := j.decode()
	if err != nil {
		return false, err
	}
	return decoded.Exists(key)
}

func (j jsonEncoded) FetchValKeyOrIdx(key string) (JSON, error) {
	decoded, err := j.decode()
	if err != nil {
		return nil, err
	}
	return decoded.FetchValKeyOrIdx(key)
}

func (j jsonEncoded) Format(buf *bytes.Buffer) {
	decoded, err := j.decode()
	if err != nil {
		buf.WriteString(`<corrupt JSON data: `)
		buf.WriteString(err.Error())
		buf.WriteString(`>`)
	} else {
		decoded.Format(buf)
	}
}

func (j jsonEncoded) RemoveIndex(idx int) (JSON, error) {
	decoded, err := j.decode()
	if err != nil {
		return nil, err
	}
	return decoded.RemoveIndex(idx)
}

func (j jsonEncoded) RemoveKey(key string) (JSON, error) {
	decoded, err := j.decode()
	if err != nil {
		return nil, err
	}
	return decoded.RemoveKey(key)
}

func (j jsonEncoded) Size() uintptr {
	return unsafe.Sizeof(j) + uintptr(len(j.value))
}

func (j jsonEncoded) String() string {
	var buf bytes.Buffer
	j.Format(&buf)
	return buf.String()
}

func (j jsonEncoded) isScalar() bool {
	return j.typ != ArrayJSONType && j.typ != ObjectJSONType
}

func (j jsonEncoded) EncodeInvertedIndexKeys(b []byte) ([][]byte, error) {
	decoded, err := j.decode()
	if err != nil {
		return nil, err
	}
	return decoded.EncodeInvertedIndexKeys(b)
}

func (j jsonEncoded) preprocessForContains() (containsable, error) {
	decoded, err := j.decode()
	if err != nil {
		return nil, err
	}
	return decoded.preprocessForContains()
}

func (j jsonEncoded) jEntry() uint32 {
	var typeTag uint32
	switch j.typ {
	case NullJSONType:
		typeTag = nullTag
	case TrueJSONType:
		typeTag = trueTag
	case FalseJSONType:
		typeTag = falseTag
	case StringJSONType:
		typeTag = stringTag
	case NumberJSONType:
		typeTag = numberTag
	case ObjectJSONType, ArrayJSONType:
		typeTag = containerTag
	}
	byteLen := uint32(len(j.value))
	return typeTag | byteLen
}

func (j jsonEncoded) encode(appendTo []byte) (jEntry uint32, b []byte, err error) {
	return j.jEntry(), append(appendTo, j.value...), nil
}

func (j jsonEncoded) mustDecode() JSON {
	decoded, err := j.decode()
	if err != nil {
		panic(fmt.Sprintf("invalid JSON data: %s, %v", err.Error(), j.value))
	}
	return decoded
}

// maybeDecode implements the JSON interface.
func (j jsonEncoded) maybeDecode() JSON {
	return j.mustDecode()
}
