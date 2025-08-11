// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mixedversion

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachprod/roachprodutil/serde"
	"gopkg.in/yaml.v3"
)

type stepProtocolTypes struct {
	InstallFixturesStep                    installFixturesStep
	StartStep                              startStep
	StartSharedProcessVirtualClusterStep   startSharedProcessVirtualClusterStep
	StartSeparateProcessVirtualClusterStep startSeparateProcessVirtualClusterStep
	RestartVirtualClusterStep              restartVirtualClusterStep
	WaitForStableClusterVersionStep        waitForStableClusterVersionStep
	PreserveDowngradeOptionStep            preserveDowngradeOptionStep
	RestartWithNewBinaryStep               restartWithNewBinaryStep
	AllowUpgradeStep                       allowUpgradeStep
	WaitStep                               waitStep
	RunHookStep                            runHookStep
	SetClusterSettingStep                  setClusterSettingStep
	SetClusterVersionStep                  setClusterVersionStep
	ResetClusterSettingStep                resetClusterSettingStep
	DeleteAllTenantsVersionOverrideStep    deleteAllTenantsVersionOverrideStep
	DisableRateLimitersStep                disableRateLimitersStep
	PanicNodeStep                          panicNodeStep
	RestartNodeStep                        restartNodeStep
	NetworkPartitionInjectStep             networkPartitionInjectStep
	NetworkPartitionRecoveryStep           networkPartitionRecoveryStep
}

type protocolStepTypeWrapper serde.TypeWrapper

var stepProtocolTypesMap = (&stepProtocolTypes{}).toTypeMap()

func (t *stepProtocolTypes) toTypeMap() serde.TypeMap {
	return serde.ToTypeMap(t)
}

func (t *stepProtocolTypes) getTypeName(
	dynamicType singleStepProtocol, validationRef singleStepProtocol,
) (string, error) {
	return serde.GetTypeName(t, dynamicType, validationRef)
}

func (t singleStepContainer) MarshalYAML() (any, error) {
	typeName, err := t.Val.getTypeName(&stepProtocolTypes{})
	if err != nil {
		return nil, fmt.Errorf("failed to get type name for %T: %w", t.Val, err)
	}
	return protocolStepTypeWrapper{
		Type: typeName,
		Val:  t.Val,
	}, nil
}

func (t *singleStepContainer) UnmarshalYAML(value *yaml.Node) error {
	var w protocolStepTypeWrapper
	if err := value.Decode(&w); err != nil {
		return fmt.Errorf("failed to decode testStep: %w", err)
	}
	*t = singleStepContainer{
		Val: w.Val.(singleStepProtocol),
	}
	return nil
}

func (t *protocolStepTypeWrapper) UnmarshalYAML(value *yaml.Node) error {
	return (*serde.TypeWrapper)(t).UnmarshalYAML(stepProtocolTypesMap, value)
}
