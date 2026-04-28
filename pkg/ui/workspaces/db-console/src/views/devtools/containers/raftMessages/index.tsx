// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { useNodesSummary } from "@cockroachlabs/cluster-ui";
import isString from "lodash/isString";
import map from "lodash/map";
import React, { useCallback, useMemo } from "react";
import { useDispatch, useSelector } from "react-redux";
import { useHistory, useRouteMatch } from "react-router-dom";

import {
  hoverOff as hoverOffAction,
  hoverOn as hoverOnAction,
  hoverStateSelector,
} from "src/redux/hover";
import { AppDispatch } from "src/redux/state";
import { setGlobalTimeScaleAction } from "src/redux/statements";
import { setMetricsFixedWindow } from "src/redux/timeScale";
import { nodeIDAttr } from "src/util/constants";
import { getMatchParamByName } from "src/util/query";
import {
  GraphDashboardProps,
  storeIDsForNode,
} from "src/views/cluster/containers/nodeGraphs/dashboards/dashboardUtils";
import TimeScaleDropdown from "src/views/cluster/containers/timeScaleDropdownWithSearchParams";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import {
  PageConfig,
  PageConfigItem,
} from "src/views/shared/components/pageconfig";
import { MetricsDataProvider } from "src/views/shared/containers/metricDataProvider";

import messagesDashboard from "./messages";

const RaftMessages: React.FC = () => {
  const dispatch: AppDispatch = useDispatch();
  const history = useHistory();
  const match = useRouteMatch();

  const {
    nodeIDs: nodeIdNumbers,
    nodeDisplayNameByID,
    storeIDsByNodeID,
  } = useNodesSummary();
  const hoverState = useSelector(hoverStateSelector);

  const nodeIds = nodeIdNumbers;

  const nodeDropdownOptions: DropdownOption[] = useMemo(() => {
    const base = [{ value: "", label: "Cluster" }];
    return base.concat(
      map(nodeIdNumbers, id => ({
        value: id.toString(),
        label: nodeDisplayNameByID[id],
      })),
    );
  }, [nodeIdNumbers, nodeDisplayNameByID]);

  const nodeChange = useCallback(
    (selected: DropdownOption) => {
      if (!isString(selected.value) || selected.value === "") {
        history.push("/raft/messages/all/");
      } else {
        history.push(`/raft/messages/node/${selected.value}`);
      }
    },
    [history],
  );

  const selectedNode = getMatchParamByName(match, nodeIDAttr) || "";
  const nodeSources = selectedNode !== "" ? [selectedNode] : null;

  // When "all" is the selected source, some graphs display a line for every
  // node in the cluster using the nodeIDs collection. However, if a specific
  // node is already selected, these per-node graphs should only display data
  // only for the selected node.
  const nodeIDs = nodeSources ? nodeSources : nodeIds;

  // If a single node is selected, we need to restrict the set of stores
  // queried for per-store metrics (only stores that belong to that node will
  // be queried).
  const storeSources = nodeSources
    ? storeIDsForNode(storeIDsByNodeID, nodeSources[0])
    : null;

  // tooltipSelection is a string used in tooltips to reference the currently
  // selected nodes. This is a prepositional phrase, currently either "across
  // all nodes" or "on node X".
  const tooltipSelection =
    nodeSources && nodeSources.length === 1
      ? `on node ${nodeSources[0]}`
      : "across all nodes";

  const dashboardProps: GraphDashboardProps = {
    nodeIDs,
    nodeSources,
    storeSources,
    tooltipSelection,
    nodeDisplayNameByID,
    storeIDsByNodeID,
  };

  // Generate graphs for the current dashboard, wrapping each one in a
  // MetricsDataProvider with a unique key.
  const graphs = messagesDashboard(dashboardProps);
  const graphComponents = map(graphs, (graph, idx) => {
    const key = `nodes.raftMessages.${idx}`;
    return (
      <div key={key}>
        <MetricsDataProvider
          id={key}
          key={key}
          setMetricsFixedWindow={tw => dispatch(setMetricsFixedWindow(tw))}
          setTimeScale={ts => dispatch(setGlobalTimeScaleAction(ts))}
          history={history}
        >
          {React.cloneElement(graph, {
            hoverOn: (...args: Parameters<typeof hoverOnAction>) =>
              dispatch(hoverOnAction(...args)),
            hoverOff: () => dispatch(hoverOffAction()),
            hoverState,
          })}
        </MetricsDataProvider>
      </div>
    );
  });

  return (
    <div>
      <PageConfig>
        <PageConfigItem>
          <Dropdown
            title="Graph"
            options={nodeDropdownOptions}
            selected={selectedNode}
            onChange={nodeChange}
          />
        </PageConfigItem>
        <PageConfigItem>
          <TimeScaleDropdown />
        </PageConfigItem>
      </PageConfig>
      <div className="section l-columns">
        <div className="chart-group l-columns__left">{graphComponents}</div>
      </div>
    </div>
  );
};

export default RaftMessages;
