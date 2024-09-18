// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useMemo } from "react";
import Select, { OptionsType } from "react-select";

import { useNodeStatuses } from "src/api";
import { getRegionFromLocality } from "src/store/nodes";
import { NodeID, StoreID } from "src/types/clusterTypes";
import {
  GroupedReactSelectOption,
  ReactSelectOption,
} from "src/types/selectTypes";

type NodeRegionsSelectorProps = {
  value: StoreID[];
  onChange: (selected: StoreID[]) => void;
};

export const NodeRegionsSelector: React.FC<NodeRegionsSelectorProps> = ({
  value,
  onChange,
}) => {
  const nodesResp = useNodeStatuses();

  const nodeOptions: GroupedReactSelectOption<StoreID[]>[] = useMemo(() => {
    const optionsMap: Record<string, { nid: NodeID; sids: StoreID[] }[]> = {};
    if (nodesResp.isLoading && !nodesResp.data?.nodes) {
      return [];
    }

    nodesResp.data.nodes.forEach(node => {
      const region = getRegionFromLocality(node.desc.locality);
      if (optionsMap[region] == null) {
        optionsMap[region] = [];
      }
      optionsMap[region].push({
        nid: node.desc.node_id as NodeID,
        sids: node.store_statuses.map(s => s.desc.store_id as StoreID),
      });
    });

    return Object.entries(optionsMap).map(([region, nodes]) => {
      return {
        label: region,
        options: nodes.map(n => ({
          label: `n${n.nid}`,
          value: n.sids,
        })),
      };
    });
  }, [nodesResp]);

  const onSelectChange = (
    selected: OptionsType<ReactSelectOption<StoreID[]>>,
  ) => {
    onChange(selected.map(s => s.value).reduce((acc, v) => acc.concat(v), []));
  };

  const selectValue: OptionsType<ReactSelectOption<StoreID[]>> =
    nodeOptions.reduce((acc, region) => {
      const nodes = region.options.filter(n =>
        value.some(v => n.value.includes(v)),
      );
      return [...acc, ...nodes];
    }, []);

  return (
    <Select
      placeholder={"Nodes"}
      name="nodeRegions"
      options={nodeOptions}
      clearable={true}
      isMulti
      value={selectValue}
      onChange={onSelectChange}
    />
  );
};
