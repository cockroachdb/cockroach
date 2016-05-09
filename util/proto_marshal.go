package util

import (
	"io"
	"io/ioutil"

	gwruntime "github.com/gengo/grpc-gateway/runtime"
	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/util/protoutil"
)

type ProtoMarshaler struct{}

func (*ProtoMarshaler) ContentType() string {
	return ProtoContentType
}

func (*ProtoMarshaler) Marshal(v interface{}) ([]byte, error) {
	if p, ok := v.(proto.Message); ok {
		return protoutil.Marshal(p)
	}
	return nil, Errorf("unexpected type %T does not implement %s", v, typeProtoMessage)
}

func (*ProtoMarshaler) Unmarshal(data []byte, v interface{}) error {
	if p, ok := v.(proto.Message); ok {
		return proto.Unmarshal(data, p)
	}
	return Errorf("unexpected type %T does not implement %s", v, typeProtoMessage)
}

type protoDecoder struct {
	r io.Reader
}

func (*ProtoMarshaler) NewDecoder(r io.Reader) gwruntime.Decoder {
	return &protoDecoder{r: r}
}

func (d *protoDecoder) Decode(v interface{}) error {
	if p, ok := v.(proto.Message); ok {
		bytes, err := ioutil.ReadAll(d.r)
		if err == nil {
			err = proto.Unmarshal(bytes, p)
		}
		return err
	}
	return Errorf("unexpected type %T does not implement %s", v, typeProtoMessage)
}

type protoEncoder struct {
	w io.Writer
}

func (*ProtoMarshaler) NewEncoder(w io.Writer) gwruntime.Encoder {
	return &protoEncoder{w: w}
}

func (e *protoEncoder) Encode(v interface{}) error {
	if p, ok := v.(proto.Message); ok {
		bytes, err := protoutil.Marshal(p)
		if err == nil {
			_, err = e.w.Write(bytes)
		}
		return err
	}
	return Errorf("unexpected type %T does not implement %s", v, typeProtoMessage)
}
