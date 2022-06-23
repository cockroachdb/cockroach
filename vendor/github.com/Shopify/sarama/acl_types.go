package sarama

import (
	"fmt"
	"strings"
)

type (
	AclOperation int

	AclPermissionType int

	AclResourceType int

	AclResourcePatternType int
)

// ref: https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/acl/AclOperation.java
const (
	AclOperationUnknown AclOperation = iota
	AclOperationAny
	AclOperationAll
	AclOperationRead
	AclOperationWrite
	AclOperationCreate
	AclOperationDelete
	AclOperationAlter
	AclOperationDescribe
	AclOperationClusterAction
	AclOperationDescribeConfigs
	AclOperationAlterConfigs
	AclOperationIdempotentWrite
)

func (a *AclOperation) String() string {
	mapping := map[AclOperation]string{
		AclOperationUnknown:         "Unknown",
		AclOperationAny:             "Any",
		AclOperationAll:             "All",
		AclOperationRead:            "Read",
		AclOperationWrite:           "Write",
		AclOperationCreate:          "Create",
		AclOperationDelete:          "Delete",
		AclOperationAlter:           "Alter",
		AclOperationDescribe:        "Describe",
		AclOperationClusterAction:   "ClusterAction",
		AclOperationDescribeConfigs: "DescribeConfigs",
		AclOperationAlterConfigs:    "AlterConfigs",
		AclOperationIdempotentWrite: "IdempotentWrite",
	}
	s, ok := mapping[*a]
	if !ok {
		s = mapping[AclOperationUnknown]
	}
	return s
}

// MarshalText returns the text form of the AclOperation (name without prefix)
func (a *AclOperation) MarshalText() ([]byte, error) {
	return []byte(a.String()), nil
}

// UnmarshalText takes a text reprentation of the operation and converts it to an AclOperation
func (a *AclOperation) UnmarshalText(text []byte) error {
	normalized := strings.ToLower(string(text))
	mapping := map[string]AclOperation{
		"unknown":         AclOperationUnknown,
		"any":             AclOperationAny,
		"all":             AclOperationAll,
		"read":            AclOperationRead,
		"write":           AclOperationWrite,
		"create":          AclOperationCreate,
		"delete":          AclOperationDelete,
		"alter":           AclOperationAlter,
		"describe":        AclOperationDescribe,
		"clusteraction":   AclOperationClusterAction,
		"describeconfigs": AclOperationDescribeConfigs,
		"alterconfigs":    AclOperationAlterConfigs,
		"idempotentwrite": AclOperationIdempotentWrite,
	}
	ao, ok := mapping[normalized]
	if !ok {
		*a = AclOperationUnknown
		return fmt.Errorf("no acl operation with name %s", normalized)
	}
	*a = ao
	return nil
}

// ref: https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/acl/AclPermissionType.java
const (
	AclPermissionUnknown AclPermissionType = iota
	AclPermissionAny
	AclPermissionDeny
	AclPermissionAllow
)

func (a *AclPermissionType) String() string {
	mapping := map[AclPermissionType]string{
		AclPermissionUnknown: "Unknown",
		AclPermissionAny:     "Any",
		AclPermissionDeny:    "Deny",
		AclPermissionAllow:   "Allow",
	}
	s, ok := mapping[*a]
	if !ok {
		s = mapping[AclPermissionUnknown]
	}
	return s
}

// MarshalText returns the text form of the AclPermissionType (name without prefix)
func (a *AclPermissionType) MarshalText() ([]byte, error) {
	return []byte(a.String()), nil
}

// UnmarshalText takes a text reprentation of the permission type and converts it to an AclPermissionType
func (a *AclPermissionType) UnmarshalText(text []byte) error {
	normalized := strings.ToLower(string(text))
	mapping := map[string]AclPermissionType{
		"unknown": AclPermissionUnknown,
		"any":     AclPermissionAny,
		"deny":    AclPermissionDeny,
		"allow":   AclPermissionAllow,
	}

	apt, ok := mapping[normalized]
	if !ok {
		*a = AclPermissionUnknown
		return fmt.Errorf("no acl permission with name %s", normalized)
	}
	*a = apt
	return nil
}

// ref: https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/resource/ResourceType.java
const (
	AclResourceUnknown AclResourceType = iota
	AclResourceAny
	AclResourceTopic
	AclResourceGroup
	AclResourceCluster
	AclResourceTransactionalID
)

func (a *AclResourceType) String() string {
	mapping := map[AclResourceType]string{
		AclResourceUnknown:         "Unknown",
		AclResourceAny:             "Any",
		AclResourceTopic:           "Topic",
		AclResourceGroup:           "Group",
		AclResourceCluster:         "Cluster",
		AclResourceTransactionalID: "TransactionalID",
	}
	s, ok := mapping[*a]
	if !ok {
		s = mapping[AclResourceUnknown]
	}
	return s
}

// MarshalText returns the text form of the AclResourceType (name without prefix)
func (a *AclResourceType) MarshalText() ([]byte, error) {
	return []byte(a.String()), nil
}

// UnmarshalText takes a text reprentation of the resource type and converts it to an AclResourceType
func (a *AclResourceType) UnmarshalText(text []byte) error {
	normalized := strings.ToLower(string(text))
	mapping := map[string]AclResourceType{
		"unknown":         AclResourceUnknown,
		"any":             AclResourceAny,
		"topic":           AclResourceTopic,
		"group":           AclResourceGroup,
		"cluster":         AclResourceCluster,
		"transactionalid": AclResourceTransactionalID,
	}

	art, ok := mapping[normalized]
	if !ok {
		*a = AclResourceUnknown
		return fmt.Errorf("no acl resource with name %s", normalized)
	}
	*a = art
	return nil
}

// ref: https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/resource/PatternType.java
const (
	AclPatternUnknown AclResourcePatternType = iota
	AclPatternAny
	AclPatternMatch
	AclPatternLiteral
	AclPatternPrefixed
)

func (a *AclResourcePatternType) String() string {
	mapping := map[AclResourcePatternType]string{
		AclPatternUnknown:  "Unknown",
		AclPatternAny:      "Any",
		AclPatternMatch:    "Match",
		AclPatternLiteral:  "Literal",
		AclPatternPrefixed: "Prefixed",
	}
	s, ok := mapping[*a]
	if !ok {
		s = mapping[AclPatternUnknown]
	}
	return s
}

// MarshalText returns the text form of the AclResourcePatternType (name without prefix)
func (a *AclResourcePatternType) MarshalText() ([]byte, error) {
	return []byte(a.String()), nil
}

// UnmarshalText takes a text reprentation of the resource pattern type and converts it to an AclResourcePatternType
func (a *AclResourcePatternType) UnmarshalText(text []byte) error {
	normalized := strings.ToLower(string(text))
	mapping := map[string]AclResourcePatternType{
		"unknown":  AclPatternUnknown,
		"any":      AclPatternAny,
		"match":    AclPatternMatch,
		"literal":  AclPatternLiteral,
		"prefixed": AclPatternPrefixed,
	}

	arpt, ok := mapping[normalized]
	if !ok {
		*a = AclPatternUnknown
		return fmt.Errorf("no acl resource pattern with name %s", normalized)
	}
	*a = arpt
	return nil
}
