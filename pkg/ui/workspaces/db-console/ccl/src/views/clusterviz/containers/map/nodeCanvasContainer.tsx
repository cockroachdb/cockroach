// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  Loading,
  api as clusterUiApi,
  buildLocalityTree,
  useNodesSummary,
  LocalityTier,
} from "@cockroachlabs/cluster-ui";
import isNil from "lodash/isNil";
import React, { useMemo } from "react";
import { useHistory } from "react-router-dom";

import { cockroach } from "src/js/protos";
import { CLUSTERVIZ_ROOT } from "src/routes/visualization";
import { getLocality } from "src/util/localities";

import { NodeCanvas } from "./nodeCanvas";

export interface NodeCanvasContainerOwnProps {
  tiers: LocalityTier[];
}

import LivenessStatus = cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus;

const NodeCanvasContainer: React.FC<NodeCanvasContainerOwnProps> = ({
  tiers,
}) => {
  const history = useHistory();

  const {
    nodeStatuses,
    livenessStatusByNodeID,
    livenessByNodeID,
    isLoading: nodesSummaryLoading,
    error: nodesSummaryError,
  } = useNodesSummary();

  const {
    locationTree,
    isLoading: locationsLoading,
    error: locationsError,
  } = clusterUiApi.useLocations();

  const isLoading = nodesSummaryLoading || locationsLoading;
  const dataExists = !isLoading && !!nodeStatuses.length;
  const errors = [nodesSummaryError, locationsError].filter(Boolean);

  const localityTree = useMemo(() => {
    const commissioned = nodeStatuses.filter(node => {
      const status = livenessStatusByNodeID[`${node.desc.node_id}`];
      return (
        isNil(status) || status !== LivenessStatus.NODE_STATUS_DECOMMISSIONED
      );
    });
    return buildLocalityTree(commissioned);
  }, [nodeStatuses, livenessStatusByNodeID]);

  const currentLocality = getLocality(localityTree, tiers);
  if (!isLoading && dataExists && isNil(currentLocality)) {
    history.replace(CLUSTERVIZ_ROOT);
  }

  return (
    <Loading
      loading={!dataExists}
      page={"node canvas container"}
      error={errors}
      render={() => (
        <NodeCanvas
          localityTree={currentLocality}
          locationTree={locationTree}
          tiers={tiers}
          livenessStatuses={livenessStatusByNodeID}
          livenesses={livenessByNodeID}
        />
      )}
    />
  );
};

export default NodeCanvasContainer;
