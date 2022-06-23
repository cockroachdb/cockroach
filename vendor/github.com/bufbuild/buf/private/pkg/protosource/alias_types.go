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

package protosource

import (
	"fmt"
	"strconv"

	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	// FieldDescriptorProtoTypeDouble is an alias.
	FieldDescriptorProtoTypeDouble FieldDescriptorProtoType = 1
	// FieldDescriptorProtoTypeFloat is an alias.
	FieldDescriptorProtoTypeFloat FieldDescriptorProtoType = 2
	// FieldDescriptorProtoTypeInt64 is an alias.
	FieldDescriptorProtoTypeInt64 FieldDescriptorProtoType = 3
	// FieldDescriptorProtoTypeUint64 is an alias.
	FieldDescriptorProtoTypeUint64 FieldDescriptorProtoType = 4
	// FieldDescriptorProtoTypeInt32 is an alias.
	FieldDescriptorProtoTypeInt32 FieldDescriptorProtoType = 5
	// FieldDescriptorProtoTypeFixed64 is an alias.
	FieldDescriptorProtoTypeFixed64 FieldDescriptorProtoType = 6
	// FieldDescriptorProtoTypeFixed32 is an alias.
	FieldDescriptorProtoTypeFixed32 FieldDescriptorProtoType = 7
	// FieldDescriptorProtoTypeBool is an alias.
	FieldDescriptorProtoTypeBool FieldDescriptorProtoType = 8
	// FieldDescriptorProtoTypeString is an alias.
	FieldDescriptorProtoTypeString FieldDescriptorProtoType = 9
	// FieldDescriptorProtoTypeGroup is an alias.
	FieldDescriptorProtoTypeGroup FieldDescriptorProtoType = 10
	// FieldDescriptorProtoTypeMessage is an alias.
	FieldDescriptorProtoTypeMessage FieldDescriptorProtoType = 11
	// FieldDescriptorProtoTypeBytes is an alias.
	FieldDescriptorProtoTypeBytes FieldDescriptorProtoType = 12
	// FieldDescriptorProtoTypeUint32 is an alias.
	FieldDescriptorProtoTypeUint32 FieldDescriptorProtoType = 13
	// FieldDescriptorProtoTypeEnum is an alias.
	FieldDescriptorProtoTypeEnum FieldDescriptorProtoType = 14
	// FieldDescriptorProtoTypeSfixed32 is an alias.
	FieldDescriptorProtoTypeSfixed32 FieldDescriptorProtoType = 15
	// FieldDescriptorProtoTypeSfixed64 is an alias.
	FieldDescriptorProtoTypeSfixed64 FieldDescriptorProtoType = 16
	// FieldDescriptorProtoTypeSint32 is an alias.
	FieldDescriptorProtoTypeSint32 FieldDescriptorProtoType = 17
	// FieldDescriptorProtoTypeSint64 is an alias.
	FieldDescriptorProtoTypeSint64 FieldDescriptorProtoType = 18

	// FieldDescriptorProtoLabelOptional is an alias.
	FieldDescriptorProtoLabelOptional FieldDescriptorProtoLabel = 1
	// FieldDescriptorProtoLabelRequired is an alias.
	FieldDescriptorProtoLabelRequired FieldDescriptorProtoLabel = 2
	// FieldDescriptorProtoLabelRepeated is an alias.
	FieldDescriptorProtoLabelRepeated FieldDescriptorProtoLabel = 3

	// FieldOptionsCTypeString is an alias.
	FieldOptionsCTypeString FieldOptionsCType = 0
	// FieldOptionsCTypeCord is an alias.
	FieldOptionsCTypeCord FieldOptionsCType = 1
	// FieldOptionsCTypeStringPiece is an alias.
	FieldOptionsCTypeStringPiece FieldOptionsCType = 2

	// FieldOptionsJSTypeJSNormal is an alias.
	FieldOptionsJSTypeJSNormal FieldOptionsJSType = 0
	// FieldOptionsJSTypeJSString is an alias.
	FieldOptionsJSTypeJSString FieldOptionsJSType = 1
	// FieldOptionsJSTypeJSNumber is an alias.
	FieldOptionsJSTypeJSNumber FieldOptionsJSType = 2

	// FileOptionsOptimizeModeSpeed is an alias.
	FileOptionsOptimizeModeSpeed FileOptionsOptimizeMode = 1
	// FileOptionsOptimizeModeCodeSize is an alias.
	FileOptionsOptimizeModeCodeSize FileOptionsOptimizeMode = 2
	// FileOptionsOptimizeModeLiteRuntime is an alias.
	FileOptionsOptimizeModeLiteRuntime FileOptionsOptimizeMode = 3

	// MethodOptionsIdempotencyLevelIdempotencyUnknown is an alias.
	MethodOptionsIdempotencyLevelIdempotencyUnknown = 0
	// MethodOptionsIdempotencyLevelNoSideEffects is an alias.
	MethodOptionsIdempotencyLevelNoSideEffects = 1
	// MethodOptionsIdempotencyLevelIdempotent is an alias.
	MethodOptionsIdempotencyLevelIdempotent = 2
)

var (
	numberToFieldDescriptorProtoType = map[int32]FieldDescriptorProtoType{
		1:  FieldDescriptorProtoTypeDouble,
		2:  FieldDescriptorProtoTypeFloat,
		3:  FieldDescriptorProtoTypeInt64,
		4:  FieldDescriptorProtoTypeUint64,
		5:  FieldDescriptorProtoTypeInt32,
		6:  FieldDescriptorProtoTypeFixed64,
		7:  FieldDescriptorProtoTypeFixed32,
		8:  FieldDescriptorProtoTypeBool,
		9:  FieldDescriptorProtoTypeString,
		10: FieldDescriptorProtoTypeGroup,
		11: FieldDescriptorProtoTypeMessage,
		12: FieldDescriptorProtoTypeBytes,
		13: FieldDescriptorProtoTypeUint32,
		14: FieldDescriptorProtoTypeEnum,
		15: FieldDescriptorProtoTypeSfixed32,
		16: FieldDescriptorProtoTypeSfixed64,
		17: FieldDescriptorProtoTypeSint32,
		18: FieldDescriptorProtoTypeSint64,
	}
	fieldDescriptorProtoTypeToString = map[FieldDescriptorProtoType]string{
		FieldDescriptorProtoTypeDouble:   "double",
		FieldDescriptorProtoTypeFloat:    "float",
		FieldDescriptorProtoTypeInt64:    "int64",
		FieldDescriptorProtoTypeUint64:   "uint64",
		FieldDescriptorProtoTypeInt32:    "int32",
		FieldDescriptorProtoTypeFixed64:  "fixed64",
		FieldDescriptorProtoTypeFixed32:  "fixed32",
		FieldDescriptorProtoTypeBool:     "bool",
		FieldDescriptorProtoTypeString:   "string",
		FieldDescriptorProtoTypeGroup:    "group",
		FieldDescriptorProtoTypeMessage:  "message",
		FieldDescriptorProtoTypeBytes:    "bytes",
		FieldDescriptorProtoTypeUint32:   "uint32",
		FieldDescriptorProtoTypeEnum:     "enum",
		FieldDescriptorProtoTypeSfixed32: "sfixed32",
		FieldDescriptorProtoTypeSfixed64: "sfixed64",
		FieldDescriptorProtoTypeSint32:   "sint32",
		FieldDescriptorProtoTypeSint64:   "sint64",
	}

	numberToFieldDescriptorProtoLabel = map[int32]FieldDescriptorProtoLabel{
		1: FieldDescriptorProtoLabelOptional,
		2: FieldDescriptorProtoLabelRequired,
		3: FieldDescriptorProtoLabelRepeated,
	}
	fieldDescriptorProtoLabelToString = map[FieldDescriptorProtoLabel]string{
		FieldDescriptorProtoLabelOptional: "optional",
		FieldDescriptorProtoLabelRequired: "required",
		FieldDescriptorProtoLabelRepeated: "repeated",
	}

	numberToFieldOptionsCType = map[int32]FieldOptionsCType{
		0: FieldOptionsCTypeString,
		1: FieldOptionsCTypeCord,
		2: FieldOptionsCTypeStringPiece,
	}
	fieldOptionsCTypeToString = map[FieldOptionsCType]string{
		FieldOptionsCTypeString:      "STRING",
		FieldOptionsCTypeCord:        "CORD",
		FieldOptionsCTypeStringPiece: "STRING_PIECE",
	}

	numberToFieldOptionsJSType = map[int32]FieldOptionsJSType{
		0: FieldOptionsJSTypeJSNormal,
		1: FieldOptionsJSTypeJSString,
		2: FieldOptionsJSTypeJSNumber,
	}
	fieldOptionsJSTypeToString = map[FieldOptionsJSType]string{
		FieldOptionsJSTypeJSNormal: "JS_NORMAL",
		FieldOptionsJSTypeJSString: "JS_STRING",
		FieldOptionsJSTypeJSNumber: "JS_NUMBER",
	}

	numberToFileOptionsOptimizeMode = map[int32]FileOptionsOptimizeMode{
		1: FileOptionsOptimizeModeSpeed,
		2: FileOptionsOptimizeModeCodeSize,
		3: FileOptionsOptimizeModeLiteRuntime,
	}
	fileOptionsOptimizeModeToString = map[FileOptionsOptimizeMode]string{
		FileOptionsOptimizeModeSpeed:       "SPEED",
		FileOptionsOptimizeModeCodeSize:    "CODE_SIZE",
		FileOptionsOptimizeModeLiteRuntime: "LITE_RUNTIME",
	}

	numberToMethodOptionsIdempotencyLevel = map[int32]MethodOptionsIdempotencyLevel{
		0: MethodOptionsIdempotencyLevelIdempotencyUnknown,
		1: MethodOptionsIdempotencyLevelNoSideEffects,
		2: MethodOptionsIdempotencyLevelIdempotent,
	}
	methodOptionsIdempotencyLevelToString = map[MethodOptionsIdempotencyLevel]string{
		MethodOptionsIdempotencyLevelIdempotencyUnknown: "IDEMPOTENCY_UNKNOWN",
		MethodOptionsIdempotencyLevelNoSideEffects:      "NO_SIDE_EFFECTS",
		MethodOptionsIdempotencyLevelIdempotent:         "IDEMPOTENT",
	}
)

// FieldDescriptorProtoType aliases descriptorpb.FieldDescriptorProto_Type.
type FieldDescriptorProtoType int32

// String returns the string representation of the FieldDescriptorProtoType.
func (t FieldDescriptorProtoType) String() string {
	s, ok := fieldDescriptorProtoTypeToString[t]
	if !ok {
		return strconv.Itoa(int(t))
	}
	return s
}

func getFieldDescriptorProtoType(v descriptorpb.FieldDescriptorProto_Type) (FieldDescriptorProtoType, error) {
	t, ok := numberToFieldDescriptorProtoType[int32(v)]
	if !ok {
		return 0, fmt.Errorf("no FieldDescriptorProtoType for %v", v)
	}
	return t, nil
}

// FieldDescriptorProtoLabel aliases descriptorpb.FieldDescriptorProto_Label.
type FieldDescriptorProtoLabel int32

// String returns the string representation of the FieldDescriptorProtoLabel.
func (l FieldDescriptorProtoLabel) String() string {
	s, ok := fieldDescriptorProtoLabelToString[l]
	if !ok {
		return strconv.Itoa(int(l))
	}
	return s
}

func getFieldDescriptorProtoLabel(v descriptorpb.FieldDescriptorProto_Label) (FieldDescriptorProtoLabel, error) {
	t, ok := numberToFieldDescriptorProtoLabel[int32(v)]
	if !ok {
		return 0, fmt.Errorf("no FieldDescriptorProtoLabel for %v", v)
	}
	return t, nil
}

// FieldOptionsCType aliases descriptorpb.FieldOptions_CType.
type FieldOptionsCType int32

// String returns the string representation of the FieldOptionsCType.
func (c FieldOptionsCType) String() string {
	s, ok := fieldOptionsCTypeToString[c]
	if !ok {
		return strconv.Itoa(int(c))
	}
	return s
}

func getFieldOptionsCType(v descriptorpb.FieldOptions_CType) (FieldOptionsCType, error) {
	t, ok := numberToFieldOptionsCType[int32(v)]
	if !ok {
		return 0, fmt.Errorf("no FieldOptionsCType for %v", v)
	}
	return t, nil
}

// FieldOptionsJSType aliases descriptorpb.FieldOptions_JSType.
type FieldOptionsJSType int32

// String returns the string representation of the FieldOptionsJSType.
func (j FieldOptionsJSType) String() string {
	s, ok := fieldOptionsJSTypeToString[j]
	if !ok {
		return strconv.Itoa(int(j))
	}
	return s
}

func getFieldOptionsJSType(v descriptorpb.FieldOptions_JSType) (FieldOptionsJSType, error) {
	t, ok := numberToFieldOptionsJSType[int32(v)]
	if !ok {
		return 0, fmt.Errorf("no FieldOptionsJSType for %v", v)
	}
	return t, nil
}

// FileOptionsOptimizeMode aliases descriptorpb.FileOptions_OptimizeMode.
type FileOptionsOptimizeMode int32

// String returns the string representation of the FileOptionsOptimizeMode.
func (t FileOptionsOptimizeMode) String() string {
	s, ok := fileOptionsOptimizeModeToString[t]
	if !ok {
		return strconv.Itoa(int(t))
	}
	return s
}

func getFileOptionsOptimizeMode(v descriptorpb.FileOptions_OptimizeMode) (FileOptionsOptimizeMode, error) {
	t, ok := numberToFileOptionsOptimizeMode[int32(v)]
	if !ok {
		return 0, fmt.Errorf("no FileOptionsOptimizeMode for %v", v)
	}
	return t, nil
}

// MethodOptionsIdempotencyLevel aliases descriptorpb.MethodOptions_IdempotencyLevel.
type MethodOptionsIdempotencyLevel int32

// String returns the string representation of the MethodOptionsIdempotencyLevel.
func (t MethodOptionsIdempotencyLevel) String() string {
	s, ok := methodOptionsIdempotencyLevelToString[t]
	if !ok {
		return strconv.Itoa(int(t))
	}
	return s
}

func getMethodOptionsIdempotencyLevel(v descriptorpb.MethodOptions_IdempotencyLevel) (MethodOptionsIdempotencyLevel, error) {
	t, ok := numberToMethodOptionsIdempotencyLevel[int32(v)]
	if !ok {
		return 0, fmt.Errorf("no MethodOptionsIdempotencyLevel for %v", v)
	}
	return t, nil
}
