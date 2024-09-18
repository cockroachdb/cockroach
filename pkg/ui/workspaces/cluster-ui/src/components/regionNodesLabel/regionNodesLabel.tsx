// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
          <Text strong>{region.label}</Text>
          {showCode && <Text>({region.code})</Text>}
          <div>
            <Badge count={nodes.length} className={styles.badge} />
          </div>
        </div>
      </Tooltip>
    </div>
  );
};
