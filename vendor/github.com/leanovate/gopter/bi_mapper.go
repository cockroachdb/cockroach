package gopter

import (
	"fmt"
	"reflect"
)

// BiMapper is a bi-directional (or bijective) mapper of a tuple of values (up)
// to another tuple of values (down).
type BiMapper struct {
	UpTypes    []reflect.Type
	DownTypes  []reflect.Type
	Downstream reflect.Value
	Upstream   reflect.Value
}

// NewBiMapper creates a BiMapper of two functions `downstream` and its
// inverse `upstream`.
// That is: The return values of `downstream` must match the parameters of
// `upstream` and vice versa.
func NewBiMapper(downstream interface{}, upstream interface{}) *BiMapper {
	downstreamVal := reflect.ValueOf(downstream)
	if downstreamVal.Kind() != reflect.Func {
		panic("downstream has to be a function")
	}
	upstreamVal := reflect.ValueOf(upstream)
	if upstreamVal.Kind() != reflect.Func {
		panic("upstream has to be a function")
	}

	downstreamType := downstreamVal.Type()
	upTypes := make([]reflect.Type, downstreamType.NumIn())
	for i := 0; i < len(upTypes); i++ {
		upTypes[i] = downstreamType.In(i)
	}
	downTypes := make([]reflect.Type, downstreamType.NumOut())
	for i := 0; i < len(downTypes); i++ {
		downTypes[i] = downstreamType.Out(i)
	}

	upstreamType := upstreamVal.Type()
	if len(upTypes) != upstreamType.NumOut() {
		panic(fmt.Sprintf("upstream is expected to have %d return values", len(upTypes)))
	}
	for i, upType := range upTypes {
		if upstreamType.Out(i) != upType {
			panic(fmt.Sprintf("upstream has wrong return type %d: %v != %v", i, upstreamType.Out(i), upType))
		}
	}
	if len(downTypes) != upstreamType.NumIn() {
		panic(fmt.Sprintf("upstream is expected to have %d parameters", len(downTypes)))
	}
	for i, downType := range downTypes {
		if upstreamType.In(i) != downType {
			panic(fmt.Sprintf("upstream has wrong parameter type %d: %v != %v", i, upstreamType.In(i), downType))
		}
	}

	return &BiMapper{
		UpTypes:    upTypes,
		DownTypes:  downTypes,
		Downstream: downstreamVal,
		Upstream:   upstreamVal,
	}
}

// ConvertUp calls the Upstream function on the arguments in the down array
// and returns the results.
func (b *BiMapper) ConvertUp(down []interface{}) []interface{} {
	if len(down) != len(b.DownTypes) {
		panic(fmt.Sprintf("Expected %d values != %d", len(b.DownTypes), len(down)))
	}
	downVals := make([]reflect.Value, len(b.DownTypes))
	for i, val := range down {
		if val == nil {
			downVals[i] = reflect.Zero(b.DownTypes[i])
		} else {
			downVals[i] = reflect.ValueOf(val)
		}
	}
	upVals := b.Upstream.Call(downVals)
	up := make([]interface{}, len(upVals))
	for i, upVal := range upVals {
		up[i] = upVal.Interface()
	}

	return up
}

// ConvertDown calls the Downstream function on the elements of the up array
// and returns the results.
func (b *BiMapper) ConvertDown(up []interface{}) []interface{} {
	if len(up) != len(b.UpTypes) {
		panic(fmt.Sprintf("Expected %d values != %d", len(b.UpTypes), len(up)))
	}
	upVals := make([]reflect.Value, len(b.UpTypes))
	for i, val := range up {
		if val == nil {
			upVals[i] = reflect.Zero(b.UpTypes[i])
		} else {
			upVals[i] = reflect.ValueOf(val)
		}
	}
	downVals := b.Downstream.Call(upVals)
	down := make([]interface{}, len(downVals))
	for i, downVal := range downVals {
		down[i] = downVal.Interface()
	}

	return down
}
