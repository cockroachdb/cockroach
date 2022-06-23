package internal

import (
	"reflect"

	"github.com/golang/protobuf/proto"
)

var typeOfBytes = reflect.TypeOf([]byte(nil))

// GetUnrecognized fetches the bytes of unrecognized fields for the given message.
func GetUnrecognized(msg proto.Message) []byte {
	val := reflect.Indirect(reflect.ValueOf(msg))
	u := val.FieldByName("XXX_unrecognized")
	if u.IsValid() && u.Type() == typeOfBytes {
		return u.Interface().([]byte)
	}

	// Fallback to reflection for API v2 messages
	get, _, _, ok := unrecognizedGetSetMethods(val)
	if !ok {
		return nil
	}

	return get.Call([]reflect.Value(nil))[0].Convert(typeOfBytes).Interface().([]byte)
}

// SetUnrecognized adds the given bytes to the unrecognized fields for the given message.
func SetUnrecognized(msg proto.Message, data []byte) {
	val := reflect.Indirect(reflect.ValueOf(msg))
	u := val.FieldByName("XXX_unrecognized")
	if u.IsValid() && u.Type() == typeOfBytes {
		// Just store the bytes in the unrecognized field
		ub := u.Interface().([]byte)
		ub = append(ub, data...)
		u.Set(reflect.ValueOf(ub))
		return
	}

	// Fallback to reflection for API v2 messages
	get, set, argType, ok := unrecognizedGetSetMethods(val)
	if !ok {
		return
	}

	existing := get.Call([]reflect.Value(nil))[0].Convert(typeOfBytes).Interface().([]byte)
	if len(existing) > 0 {
		data = append(existing, data...)
	}
	set.Call([]reflect.Value{reflect.ValueOf(data).Convert(argType)})
}

func unrecognizedGetSetMethods(val reflect.Value) (get reflect.Value, set reflect.Value, argType reflect.Type, ok bool) {
	// val could be an APIv2 message. We use reflection to interact with
	// this message so that we don't have a hard dependency on the new
	// version of the protobuf package.
	refMethod := val.MethodByName("ProtoReflect")
	if !refMethod.IsValid() {
		if val.CanAddr() {
			refMethod = val.Addr().MethodByName("ProtoReflect")
		}
		if !refMethod.IsValid() {
			return
		}
	}
	refType := refMethod.Type()
	if refType.NumIn() != 0 || refType.NumOut() != 1 {
		return
	}
	ref := refMethod.Call([]reflect.Value(nil))
	getMethod, setMethod := ref[0].MethodByName("GetUnknown"), ref[0].MethodByName("SetUnknown")
	if !getMethod.IsValid() || !setMethod.IsValid() {
		return
	}
	getType := getMethod.Type()
	setType := setMethod.Type()
	if getType.NumIn() != 0 || getType.NumOut() != 1 || setType.NumIn() != 1 || setType.NumOut() != 0 {
		return
	}
	arg := setType.In(0)
	if !arg.ConvertibleTo(typeOfBytes) || getType.Out(0) != arg {
		return
	}

	return getMethod, setMethod, arg, true
}
