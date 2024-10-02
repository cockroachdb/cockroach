// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tooltip, Badge, Typography } from "antd";
import React from "react";

import { NodeID, Region } from "src/types/clusterTypes";

import styles from "./regionNodesLabel.module.scss";

const { Text } = Typography;

type RegionNodesLabelProps = {
  nodes: NodeID[];
  region: Region;
  showCode?: boolean;
};

// TODO(xinhaoz): We may also be unable to show a flag for regions in SH.
export const RegionNodesLabel: React.FC<RegionNodesLabelProps> = ({
  nodes = [],
  region,
  // TODO (xinhaoz): Investigate if we can determine labels for regions in SH.
  showCode = false,
}) => {
  return (
    <div className={styles.container}>
      <Tooltip placement="top" title={nodes.map(nid => "n" + nid).join(", ")}>
        <div className={styles["label-body"]}>
          <Text strong>{region.label || "Unknown Region"}</Text>
          {showCode && <Text>({region.code})</Text>}
          <div>
            <Badge count={nodes.length} className={styles.badge} />
          </div>
        </div>
      </Tooltip>
    </div>
  );
};
