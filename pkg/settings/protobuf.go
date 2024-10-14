// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settings

import (
	"context"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
)

// ProtobufSetting is the interface of a setting variable that will be updated
// automatically when the corresponding cluster-wide setting of type "protobuf"
// is updated. The proto message is held in memory, the byte representation
// stored in system.settings, and JSON presentation used when accepting input
// and rendering (just through SHOW CLUSTER SETTING <setting-name>, the raw form
// is visible when looking directly at system.settings).
type ProtobufSetting struct {
	defaultValue protoutil.Message
	validateFn   func(*Values, protoutil.Message) error
	common
}

var _ internalSetting = &ProtobufSetting{}

// String returns the string representation of the setting's current value.
func (s *ProtobufSetting) String(sv *Values) string {
	p := s.Get(sv)
	json, err := s.MarshalToJSON(p)
	if err != nil {
		panic(errors.Wrapf(err, "marshaling %s: %+v", proto.MessageName(p), p))
	}
	return json
}

// DefaultString returns the default value for the setting as a string.
func (s *ProtobufSetting) DefaultString() string {
	json, err := s.MarshalToJSON(s.defaultValue)
	if err != nil {
		panic(errors.Wrapf(err, "marshaling %s: %+v", proto.MessageName(s.defaultValue), s.defaultValue))
	}
	return json
}

// Encoded returns the encoded value of the current value of the setting.
func (s *ProtobufSetting) Encoded(sv *Values) string {
	p := s.Get(sv)
	return EncodeProtobuf(p)
}

// EncodedDefault returns the encoded value of the default value of the setting.
func (s *ProtobufSetting) EncodedDefault() string {
	return EncodeProtobuf(s.defaultValue)
}

// DecodeToString decodes and renders an encoded value.
func (s *ProtobufSetting) DecodeToString(encoded string) (string, error) {
	message, err := s.DecodeValue(encoded)
	if err != nil {
		return "", err
	}
	return s.MarshalToJSON(message)
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*ProtobufSetting) Typ() string {
	return "p"
}

// DecodeValue decodes the value into a protobuf.
func (s *ProtobufSetting) DecodeValue(encoded string) (protoutil.Message, error) {
	p, err := newProtoMessage(proto.MessageName(s.defaultValue))
	if err != nil {
		return nil, err
	}

	if err := protoutil.Unmarshal([]byte(encoded), p); err != nil {
		return nil, err
	}
	return p, nil
}

// Default returns default value for setting.
func (s *ProtobufSetting) Default() protoutil.Message {
	return s.defaultValue
}

// Get retrieves the protobuf value in the setting.
func (s *ProtobufSetting) Get(sv *Values) protoutil.Message {
	loaded := sv.getGeneric(s.slot)
	if loaded == nil {
		return s.defaultValue
	}
	return loaded.(protoutil.Message)
}

// Validate that a value conforms with the validation function.
func (s *ProtobufSetting) Validate(sv *Values, p protoutil.Message) error {
	if s.validateFn == nil {
		return nil // nothing to do
	}
	return s.validateFn(sv, p)
}

// Override sets the setting to the given value, assuming it passes validation.
func (s *ProtobufSetting) Override(ctx context.Context, sv *Values, p protoutil.Message) {
	sv.setValueOrigin(ctx, s.slot, OriginOverride)
	_ = s.set(ctx, sv, p)
	sv.setDefaultOverride(s.slot, p)
}

func (s *ProtobufSetting) decodeAndSet(ctx context.Context, sv *Values, encoded string) error {
	p, err := s.DecodeValue(encoded)
	if err != nil {
		return err
	}
	return s.set(ctx, sv, p)
}

func (s *ProtobufSetting) decodeAndSetDefaultOverride(
	ctx context.Context, sv *Values, encoded string,
) error {
	p, err := s.DecodeValue(encoded)
	if err != nil {
		return err
	}
	sv.setDefaultOverride(s.slot, p)
	return nil
}

func (s *ProtobufSetting) set(ctx context.Context, sv *Values, p protoutil.Message) error {
	if err := s.Validate(sv, p); err != nil {
		return err
	}
	if s.Get(sv) != p {
		sv.setGeneric(ctx, s.slot, p)
	}
	return nil
}

func (s *ProtobufSetting) setToDefault(ctx context.Context, sv *Values) {
	// See if the default value was overridden.
	if val := sv.getDefaultOverride(s.slot); val != nil {
		// As per the semantics of override, these values don't go through
		// validation.
		_ = s.set(ctx, sv, val.(protoutil.Message))
		return
	}
	if err := s.set(ctx, sv, s.defaultValue); err != nil {
		panic(err)
	}
}

// MarshalToJSON returns a JSON representation of the protobuf.
func (s *ProtobufSetting) MarshalToJSON(p protoutil.Message) (string, error) {
	jsonEncoder := jsonpb.Marshaler{EmitDefaults: false}
	return jsonEncoder.MarshalToString(p)
}

// UnmarshalFromJSON unmarshals a protobuf from a json representation.
func (s *ProtobufSetting) UnmarshalFromJSON(jsonEncoded string) (protoutil.Message, error) {
	p, err := newProtoMessage(proto.MessageName(s.defaultValue))
	if err != nil {
		return nil, err
	}

	json := &jsonpb.Unmarshaler{}
	if err := json.Unmarshal(strings.NewReader(jsonEncoded), p); err != nil {
		return nil, errors.Wrapf(err, "unmarshaling json to %s", proto.MessageName(p))
	}
	return p, nil
}

// RegisterProtobufSetting defines a new setting with type protobuf.
func RegisterProtobufSetting(
	class Class, key InternalKey, desc string, defaultValue protoutil.Message, opts ...SettingOption,
) *ProtobufSetting {
	validateFn := func(sv *Values, p protoutil.Message) error {
		for _, opt := range opts {
			switch {
			case opt.commonOpt != nil:
				continue
			case opt.validateProtoFn != nil:
			default:
				panic(errors.AssertionFailedf("wrong validator type"))
			}
			if err := opt.validateProtoFn(sv, p); err != nil {
				return err
			}
		}
		return nil
	}
	setting := &ProtobufSetting{
		defaultValue: defaultValue,
		validateFn:   validateFn,
	}

	// By default, all protobuf settings are considered to contain PII and are
	// thus non-reportable (to exclude them from telemetry reports).
	setting.setReportable(false)
	register(class, key, desc, setting)
	setting.apply(opts)
	return setting
}

// Defeat the unused linter.
var _ = (*ProtobufSetting).Default

// newProtoMessage creates a new protocol message object, given its fully
// qualified name.
func newProtoMessage(name string) (protoutil.Message, error) {
	// Get the reflected type of the protocol message.
	rt := proto.MessageType(name)
	if rt == nil {
		return nil, errors.Newf("unknown proto message type %s", name)
	}

	// If the message is known, we should get the pointer to our message.
	if rt.Kind() != reflect.Ptr {
		return nil, errors.AssertionFailedf(
			"expected ptr to message, got %s instead", rt.Kind().String())
	}

	// Construct message of appropriate type, through reflection.
	rv := reflect.New(rt.Elem())
	msg, ok := rv.Interface().(protoutil.Message)
	if !ok {
		// Just to be safe.
		return nil, errors.AssertionFailedf(
			"unexpected proto type for %s; expected protoutil.Message, got %T",
			name, rv.Interface())
	}
	return msg, nil
}
