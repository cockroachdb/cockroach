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

var (
	csharpNamespacePathKey      = getPathKey([]int32{8, 37})
	goPackagePathKey            = getPathKey([]int32{8, 11})
	javaMultipleFilesPathKey    = getPathKey([]int32{8, 10})
	javaOuterClassnamePathKey   = getPathKey([]int32{8, 8})
	javaPackagePathKey          = getPathKey([]int32{8, 1})
	javaStringCheckUtf8PathKey  = getPathKey([]int32{8, 27})
	objcClassPrefixPathKey      = getPathKey([]int32{8, 36})
	packagePathKey              = getPathKey([]int32{2})
	phpClassPrefixPathKey       = getPathKey([]int32{8, 40})
	phpNamespacePathKey         = getPathKey([]int32{8, 41})
	phpMetadataNamespacePathKey = getPathKey([]int32{8, 44})
	rubyPackagePathKey          = getPathKey([]int32{8, 45})
	swiftPrefixPathKey          = getPathKey([]int32{8, 39})
	optimizeForPathKey          = getPathKey([]int32{8, 9})
	ccGenericServicesPathKey    = getPathKey([]int32{8, 16})
	javaGenericServicesPathKey  = getPathKey([]int32{8, 17})
	pyGenericServicesPathKey    = getPathKey([]int32{8, 18})
	phpGenericServicesPathKey   = getPathKey([]int32{8, 42})
	ccEnableArenasPathKey       = getPathKey([]int32{8, 31})
	syntaxPathKey               = getPathKey([]int32{12})
)

func getDependencyPath(dependencyIndex int) []int32 {
	return []int32{3, int32(dependencyIndex)}
}

func getMessagePath(topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	path := []int32{4, int32(topLevelMessageIndex)}
	for _, nestedMessageIndex := range nestedMessageIndexes {
		path = append(path, 3, int32(nestedMessageIndex))
	}
	return path
}

func getMessageNamePath(messageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessagePath(messageIndex, nestedMessageIndexes...), 1)
}

func getMessageMessageSetWireFormatPath(messageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessagePath(messageIndex, nestedMessageIndexes...), 7, 1)
}

func getMessageNoStandardDescriptorAccessorPath(messageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessagePath(messageIndex, nestedMessageIndexes...), 7, 2)
}

func getMessageFieldPath(fieldIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessagePath(topLevelMessageIndex, nestedMessageIndexes...), 2, int32(fieldIndex))
}

func getMessageFieldNamePath(fieldIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessageFieldPath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...), 1)
}

func getMessageFieldNumberPath(fieldIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessageFieldPath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...), 3)
}

func getMessageFieldTypePath(fieldIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessageFieldPath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...), 5)
}

func getMessageFieldTypeNamePath(fieldIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessageFieldPath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...), 6)
}

func getMessageFieldJSONNamePath(fieldIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessageFieldPath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...), 10)
}

func getMessageFieldJSTypePath(fieldIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessageFieldPath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...), 8, 6)
}

func getMessageFieldCTypePath(fieldIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessageFieldPath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...), 8, 1)
}

func getMessageFieldPackedPath(fieldIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessageFieldPath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...), 8, 2)
}

func getMessageFieldExtendeePath(fieldIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessageFieldPath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...), 2)
}

func getMessageExtensionPath(extensionIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessagePath(topLevelMessageIndex, nestedMessageIndexes...), 6, int32(extensionIndex))
}

func getMessageExtensionNamePath(extensionIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessageExtensionPath(extensionIndex, topLevelMessageIndex, nestedMessageIndexes...), 1)
}

func getMessageExtensionNumberPath(extensionIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessageExtensionPath(extensionIndex, topLevelMessageIndex, nestedMessageIndexes...), 3)
}

func getMessageExtensionTypePath(extensionIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessageExtensionPath(extensionIndex, topLevelMessageIndex, nestedMessageIndexes...), 5)
}

func getMessageExtensionTypeNamePath(extensionIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessageExtensionPath(extensionIndex, topLevelMessageIndex, nestedMessageIndexes...), 6)
}

func getMessageExtensionJSONNamePath(extensionIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessageExtensionPath(extensionIndex, topLevelMessageIndex, nestedMessageIndexes...), 10)
}

func getMessageExtensionJSTypePath(extensionIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessageExtensionPath(extensionIndex, topLevelMessageIndex, nestedMessageIndexes...), 8, 6)
}

func getMessageExtensionCTypePath(extensionIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessageExtensionPath(extensionIndex, topLevelMessageIndex, nestedMessageIndexes...), 8, 1)
}

func getMessageExtensionPackedPath(extensionIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessageExtensionPath(extensionIndex, topLevelMessageIndex, nestedMessageIndexes...), 8, 2)
}

func getMessageExtensionExtendeePath(extensionIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessageExtensionPath(extensionIndex, topLevelMessageIndex, nestedMessageIndexes...), 2)
}

func getMessageOneofPath(oneofIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessagePath(topLevelMessageIndex, nestedMessageIndexes...), 8, int32(oneofIndex))
}

func getMessageOneofNamePath(oneofIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessageOneofPath(oneofIndex, topLevelMessageIndex, nestedMessageIndexes...), 1)
}

func getMessageReservedRangePath(reservedRangeIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessagePath(topLevelMessageIndex, nestedMessageIndexes...), 9, int32(reservedRangeIndex))
}

func getMessageReservedNamePath(reservedNameIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessagePath(topLevelMessageIndex, nestedMessageIndexes...), 10, int32(reservedNameIndex))
}

func getMessageExtensionRangePath(extensionRangeIndex int, topLevelMessageIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getMessagePath(topLevelMessageIndex, nestedMessageIndexes...), 5, int32(extensionRangeIndex))
}

func getEnumPath(enumIndex int, nestedMessageIndexes ...int) []int32 {
	if len(nestedMessageIndexes) == 0 {
		return []int32{5, int32(enumIndex)}
	}
	path := []int32{4, int32(nestedMessageIndexes[0])}
	for _, nestedMessageIndex := range nestedMessageIndexes[1:] {
		path = append(path, 3, int32(nestedMessageIndex))
	}
	return append(path, 4, int32(enumIndex))
}
func getEnumNamePath(enumIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getEnumPath(enumIndex, nestedMessageIndexes...), 1)
}

func getEnumAllowAliasPath(enumIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getEnumPath(enumIndex, nestedMessageIndexes...), 3, 2)
}

func getEnumValuePath(enumIndex int, enumValueIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getEnumPath(enumIndex, nestedMessageIndexes...), 2, int32(enumValueIndex))
}

func getEnumValueNamePath(enumIndex int, enumValueIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getEnumValuePath(enumIndex, enumValueIndex, nestedMessageIndexes...), 1)
}

func getEnumValueNumberPath(enumIndex int, enumValueIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getEnumValuePath(enumIndex, enumValueIndex, nestedMessageIndexes...), 2)
}

func getEnumReservedRangePath(enumIndex int, reservedRangeIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getEnumPath(enumIndex, nestedMessageIndexes...), 4, int32(reservedRangeIndex))
}

func getEnumReservedNamePath(enumIndex int, reservedNameIndex int, nestedMessageIndexes ...int) []int32 {
	return append(getEnumPath(enumIndex, nestedMessageIndexes...), 5, int32(reservedNameIndex))
}

func getServicePath(serviceIndex int) []int32 {
	return []int32{6, int32(serviceIndex)}
}

func getServiceNamePath(serviceIndex int) []int32 {
	return append(getServicePath(serviceIndex), 1)
}

func getMethodPath(serviceIndex int, methodIndex int) []int32 {
	return []int32{6, int32(serviceIndex), 2, int32(methodIndex)}
}

func getMethodNamePath(serviceIndex int, methodIndex int) []int32 {
	return append(getMethodPath(serviceIndex, methodIndex), 1)
}

func getMethodInputTypePath(serviceIndex int, methodIndex int) []int32 {
	return append(getMethodPath(serviceIndex, methodIndex), 2)
}

func getMethodOutputTypePath(serviceIndex int, methodIndex int) []int32 {
	return append(getMethodPath(serviceIndex, methodIndex), 3)
}

func getMethodIdempotencyLevelPath(serviceIndex int, methodIndex int) []int32 {
	return append(getMethodPath(serviceIndex, methodIndex), 4, 34)
}

func getFileExtensionPath(fieldIndex int) []int32 {
	return []int32{7, int32(fieldIndex)}
}

func getFileExtensionNamePath(fieldIndex int) []int32 {
	return append(getFileExtensionPath(fieldIndex), 1)
}

func getFileExtensionNumberPath(fieldIndex int) []int32 {
	return append(getFileExtensionPath(fieldIndex), 3)
}

func getFileExtensionTypePath(fieldIndex int) []int32 {
	return append(getFileExtensionPath(fieldIndex), 5)
}

func getFileExtensionTypeNamePath(fieldIndex int) []int32 {
	return append(getFileExtensionPath(fieldIndex), 6)
}

func getFileExtensionJSONNamePath(fieldIndex int) []int32 {
	return append(getFileExtensionPath(fieldIndex), 10)
}

func getFileExtensionJSTypePath(fieldIndex int) []int32 {
	return append(getFileExtensionPath(fieldIndex), 8, 6)
}

func getFileExtensionCTypePath(fieldIndex int) []int32 {
	return append(getFileExtensionPath(fieldIndex), 8, 1)
}

func getFileExtensionPackedPath(fieldIndex int) []int32 {
	return append(getFileExtensionPath(fieldIndex), 8, 2)
}

func getFileExtensionExtendeePath(fieldIndex int) []int32 {
	return append(getFileExtensionPath(fieldIndex), 2)
}
