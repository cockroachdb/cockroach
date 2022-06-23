package internal

import (
	"github.com/golang/protobuf/proto"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/internal/codec"
	"reflect"
	"strings"

	"github.com/jhump/protoreflect/internal"
)

// NB: We use reflection or unknown fields in case we are linked against an older
// version of the proto runtime which does not know about the proto3_optional field.
// We don't require linking with newer version (which would greatly simplify this)
// because that means pulling in v1.4+ of the protobuf runtime, which has some
// compatibility issues. (We'll be nice to users and not require they upgrade to
// that latest runtime to upgrade to newer protoreflect.)

func GetProto3Optional(fd *dpb.FieldDescriptorProto) bool {
	type newerFieldDesc interface {
		GetProto3Optional() bool
	}
	var pm proto.Message = fd
	if fd, ok := pm.(newerFieldDesc); ok {
		return fd.GetProto3Optional()
	}

	// Field does not exist, so we have to examine unknown fields
	// (we just silently bail if we have problems parsing them)
	unk := internal.GetUnrecognized(pm)
	buf := codec.NewBuffer(unk)
	for {
		tag, wt, err := buf.DecodeTagAndWireType()
		if err != nil {
			return false
		}
		if tag == Field_proto3OptionalTag && wt == proto.WireVarint {
			v, _ := buf.DecodeVarint()
			return v != 0
		}
		if err := buf.SkipField(wt); err != nil {
			return false
		}
	}
}

func SetProto3Optional(fd *dpb.FieldDescriptorProto) {
	rv := reflect.ValueOf(fd).Elem()
	fld := rv.FieldByName("Proto3Optional")
	if fld.IsValid() {
		fld.Set(reflect.ValueOf(proto.Bool(true)))
		return
	}

	// Field does not exist, so we have to store as unknown field.
	var buf codec.Buffer
	if err := buf.EncodeTagAndWireType(Field_proto3OptionalTag, proto.WireVarint); err != nil {
		// TODO: panic? log?
		return
	}
	if err := buf.EncodeVarint(1); err != nil {
		// TODO: panic? log?
		return
	}
	internal.SetUnrecognized(fd, buf.Bytes())
}

// ProcessProto3OptionalFields adds synthetic oneofs to the given message descriptor
// for each proto3 optional field. It also updates the fields to have the correct
// oneof index reference.
func ProcessProto3OptionalFields(msgd *dpb.DescriptorProto) {
	var allNames map[string]struct{}
	for _, fd := range msgd.Field {
		if GetProto3Optional(fd) {
			// lazy init the set of all names
			if allNames == nil {
				allNames = map[string]struct{}{}
				for _, fd := range msgd.Field {
					allNames[fd.GetName()] = struct{}{}
				}
				for _, fd := range msgd.Extension {
					allNames[fd.GetName()] = struct{}{}
				}
				for _, ed := range msgd.EnumType {
					allNames[ed.GetName()] = struct{}{}
					for _, evd := range ed.Value {
						allNames[evd.GetName()] = struct{}{}
					}
				}
				for _, fd := range msgd.NestedType {
					allNames[fd.GetName()] = struct{}{}
				}
				for _, n := range msgd.ReservedName {
					allNames[n] = struct{}{}
				}
			}

			// Compute a name for the synthetic oneof. This uses the same
			// algorithm as used in protoc:
			//  https://github.com/protocolbuffers/protobuf/blob/74ad62759e0a9b5a21094f3fb9bb4ebfaa0d1ab8/src/google/protobuf/compiler/parser.cc#L785-L803
			ooName := fd.GetName()
			if !strings.HasPrefix(ooName, "_") {
				ooName = "_" + ooName
			}
			for {
				_, ok := allNames[ooName]
				if !ok {
					// found a unique name
					allNames[ooName] = struct{}{}
					break
				}
				ooName = "X" + ooName
			}

			fd.OneofIndex = proto.Int32(int32(len(msgd.OneofDecl)))
			msgd.OneofDecl = append(msgd.OneofDecl, &dpb.OneofDescriptorProto{Name: proto.String(ooName)})
		}
	}
}
