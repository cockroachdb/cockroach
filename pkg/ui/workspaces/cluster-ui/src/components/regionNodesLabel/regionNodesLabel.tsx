// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Skeleton } from "antd";
import React from "react";

import { NodeID } from "src/types/clusterTypes";

import { NodesList } from "./components/nodesList";
import { RegionLabel } from "./components/regionLabel";

type RegionNodesLabelProps = {
  nodesByRegion: Record<string, NodeID[]>;
  loading?: boolean;
};

export const RegionNodesLabel: React.FC<RegionNodesLabelProps> = ({
  nodesByRegion = {},
  loading,
}) => {
  if (loading) {
    return (
      <Skeleton paragraph={false} title={{ width: 100 }} loading={loading} />
    );
  }

  if (Object.keys(nodesByRegion).length === 1) {
    return <NodesList nodes={Object.values(nodesByRegion)[0]} />;
  }

  return (
    <div>
      {Object.entries(nodesByRegion).map(([region, nodes]) => (
        <RegionLabel
          key={region}
          region={{
            code: region,
            label: region,
          }}
          nodes={nodes}
        />
      ))}
    </div>
  );
};
