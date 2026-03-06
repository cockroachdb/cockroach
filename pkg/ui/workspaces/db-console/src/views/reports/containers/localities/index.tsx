// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Loading, useNodesSummary } from "@cockroachlabs/cluster-ui";
import isNil from "lodash/isNil";
import React, { useEffect, useMemo } from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { refreshLocations } from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import {
  buildLocalityTree,
  LocalityTier,
  LocalityTree,
} from "src/redux/localities";
import {
  LocationTree,
  selectLocationsRequestStatus,
  selectLocationTree,
} from "src/redux/locations";
import { LivenessStatus } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { getNodeLocalityTiers } from "src/util/localities";
import { findMostSpecificLocation, hasLocation } from "src/util/locations";
import "./localities.scss";

import { BackToAdvanceDebug } from "../util";

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

interface LocalitiesProps {
  locationTree: LocationTree;
  locationStatus: CachedDataReducerState<any>;
  refreshLocations: typeof refreshLocations;
}

export function Localities({
  locationTree,
  locationStatus,
  refreshLocations: refreshLocationsAction,
  history,
}: LocalitiesProps & RouteComponentProps): React.ReactElement {
  const {
    nodeStatuses,
    livenessStatusByNodeID,
    isLoading: nodesLoading,
  } = useNodesSummary();

  const localityTree = useMemo(() => {
    const commissionedNodes = nodeStatuses.filter(node => {
      const status = livenessStatusByNodeID[`${node.desc.node_id}`];
      return status !== LivenessStatus.NODE_STATUS_DECOMMISSIONED;
    });
    return buildLocalityTree(commissionedNodes);
  }, [nodeStatuses, livenessStatusByNodeID]);

  useEffect(() => {
    refreshLocationsAction();
  }, [refreshLocationsAction]);

  return (
    <div>
      <Helmet title="Localities | Debug" />
      <BackToAdvanceDebug history={history} />
      <section className="section">
        <h1 className="base-heading">Localities</h1>
      </section>
      <Loading
        loading={nodesLoading || !locationStatus.data}
        page={"localities"}
        error={[locationStatus.lastError]}
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

const mapStateToProps = (state: AdminUIState) => ({
  locationTree: selectLocationTree(state),
  locationStatus: selectLocationsRequestStatus(state),
});

const mapDispatchToProps = {
  refreshLocations,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(Localities),
);
