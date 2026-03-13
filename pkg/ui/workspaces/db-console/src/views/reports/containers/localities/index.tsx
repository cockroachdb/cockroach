// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  Loading,
  api as clusterUiApi,
  useNodesSummary,
} from "@cockroachlabs/cluster-ui";
import isNil from "lodash/isNil";
import React, { useMemo } from "react";
import { Helmet } from "react-helmet";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { cockroach } from "src/js/protos";
import {
  LocalityTier,
  LocalityTree,
  selectLocalityTree,
} from "src/redux/localities";
import { LocationTree } from "src/redux/locations";
import { getNodeLocalityTiers } from "src/util/localities";
import { findMostSpecificLocation, hasLocation } from "src/util/locations";
import "./localities.scss";

import { BackToAdvanceDebug } from "../util";

import LivenessStatus = cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus;

function formatCoord(coordinate: number) {
  return coordinate.toFixed(4);
}

function renderLocation(locations: LocationTree, tiers: LocalityTier[]) {
  const location = findMostSpecificLocation(locations, tiers);

  if (isNil(location)) {
    return "";
  }

  return `${formatCoord(location.latitude)}, ${formatCoord(
    location.longitude,
  )}`;
}

function renderLocalityTree(locations: LocationTree, tree: LocalityTree) {
  let rows: React.ReactNode[] = [];
  const leftIndentStyle = {
    paddingLeft: `${20 * tree.tiers.length}px`,
  };

  tree.nodes.forEach(node => {
    const tiers = getNodeLocalityTiers(node);

    rows.push(
      <tr>
        <td></td>
        <td>
          n{node.desc.node_id} @ {node.desc.address.address_field}
        </td>
        <td className="parent-location">{renderLocation(locations, tiers)}</td>
      </tr>,
    );
  });

  Object.keys(tree.localities).forEach(key => {
    Object.keys(tree.localities[key]).forEach(value => {
      const child = tree.localities[key][value];

      rows.push(
        <tr>
          <td>
            <span style={leftIndentStyle}>
              {key}={value}
            </span>
          </td>
          <td></td>
          <td
            className={
              hasLocation(locations, { key, value })
                ? "own-location"
                : "parent-location"
            }
          >
            {renderLocation(locations, child.tiers)}
          </td>
        </tr>,
      );

      rows = rows.concat(renderLocalityTree(locations, child));
    });
  });

  return rows;
}

function buildLocalityTree(
  nodeStatuses: cockroach.server.status.statuspb.INodeStatus[],
  livenessStatusByNodeID: Record<string, number>,
): LocalityTree {
  const commissioned = nodeStatuses.filter(node => {
    const status = livenessStatusByNodeID[`${node.desc.node_id}`];
    return (
      isNil(status) || status !== LivenessStatus.NODE_STATUS_DECOMMISSIONED
    );
  });
  return selectLocalityTree.resultFunc(commissioned);
}

export function Localities({
  history,
}: RouteComponentProps): React.ReactElement {
  const {
    nodeStatuses,
    livenessStatusByNodeID,
    isLoading: nodesLoading,
    error: nodesError,
  } = useNodesSummary();

  const {
    locationTree,
    isLoading: locationsLoading,
    error: locationsError,
  } = clusterUiApi.useLocations();

  const isLoading = nodesLoading || locationsLoading;
  const errors = [nodesError, locationsError].filter(Boolean);

  const localityTree = useMemo(
    () => buildLocalityTree(nodeStatuses, livenessStatusByNodeID),
    [nodeStatuses, livenessStatusByNodeID],
  );

  return (
    <div>
      <Helmet title="Localities | Debug" />
      <BackToAdvanceDebug history={history} />
      <section className="section">
        <h1 className="base-heading">Localities</h1>
      </section>
      <Loading
        loading={isLoading}
        page={"localities"}
        error={errors}
        render={() => (
          <section className="section">
            <table className="locality-table">
              <thead>
                <tr>
                  <th>Localities</th>
                  <th>Nodes</th>
                  <th>Location</th>
                </tr>
              </thead>
              <tbody>{renderLocalityTree(locationTree, localityTree)}</tbody>
            </table>
          </section>
        )}
      />
    </div>
  );
}

export default withRouter(Localities);
