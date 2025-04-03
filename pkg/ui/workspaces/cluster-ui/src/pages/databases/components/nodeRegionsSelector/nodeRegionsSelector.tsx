// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useMemo } from "react";
import Select, { OptionsType } from "react-select";

import { useNodeStatuses } from "src/api";
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
  const { nodeStatusByID, isLoading } = useNodeStatuses();

  const nodeOptions: GroupedReactSelectOption<StoreID[]>[] = useMemo(() => {
    const optionsMap: Record<string, { nid: NodeID; sids: StoreID[] }[]> = {};
    const nodes = Object.keys(nodeStatusByID ?? {});
    if (isLoading && !nodes.length) {
      return [];
    }

    nodes.forEach(node => {
      const nid = parseInt(node) as NodeID;
      const region = nodeStatusByID[nid].region;
      if (optionsMap[region] == null) {
        optionsMap[region] = [];
      }
      optionsMap[region].push({
        nid,
        sids: nodeStatusByID[nid].stores,
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
  }, [nodeStatusByID, isLoading]);

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
