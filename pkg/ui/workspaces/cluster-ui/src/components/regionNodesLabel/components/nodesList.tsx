// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tag } from "antd";
import React from "react";

import { Tooltip } from "src/components/tooltip";
import { NodeID } from "src/types/clusterTypes";

import styles from "./nodesList.module.scss";

type Props = {
  nodes: NodeID[];
};

export const NodesList: React.FC<Props> = ({ nodes = [] }) => {
  if (!nodes.length) {
    return null;
  }

  const displayedNodes = nodes.slice(0, 4);
  const hiddenNodes = nodes.length > 4 ? nodes.slice(4) : [];
  return (
    <div>
      {displayedNodes.map(nid => (
        <Tag className={styles["label-body"]} key={nid}>
          N{nid}
        </Tag>
      ))}
      {hiddenNodes?.length > 0 && (
        <Tooltip title={hiddenNodes.map(nid => `n${nid}`).join(", ")}>
          +{hiddenNodes.length}
        </Tooltip>
      )}
    </div>
  );
};
