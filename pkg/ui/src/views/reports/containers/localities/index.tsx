import _ from "lodash";
import React from "react";
import { connect } from "react-redux";

import { refreshNodes, refreshLocations } from "src/redux/apiReducers";
import { selectLocalityTree, LocalityTier, LocalityTree } from "src/redux/localities";
import { selectLocationTree, LocationTree } from "src/redux/locations";
import { AdminUIState } from "src/redux/state";
import { findMostSpecificLocation, hasLocation } from "src/util/locations";

import "./localities.styl";

function formatCoord(coordinate: number) {
  return coordinate.toFixed(4);
}

function renderLocation(locations: LocationTree, tiers: LocalityTier[]) {
  const location = findMostSpecificLocation(locations, tiers);

  if (_.isNil(location)) {
    return "";
  }

  return `${formatCoord(location.latitude)}, ${formatCoord(location.longitude)}`;
}

function renderLocalityTree(locations: LocationTree, tree: LocalityTree) {
  let rows: React.ReactNode[] = [];

  tree.nodes.forEach((node) => {
    rows.push(
      <tr>
        <td></td>
        <td>n{ node.desc.node_id } @ { node.desc.address.address_field }</td>
        <td className="parent-location">{ renderLocation(locations, node.desc.locality.tiers) }</td>
      </tr>,
    );
  });

  Object.keys(tree.localities).forEach((key) => {
    Object.keys(tree.localities[key]).forEach((value) => {
      const child = tree.localities[key][value];

      rows.push(
        <tr>
          <td className={`depth-${tree.tiers.length}`}>{ key }={ value }</td>
          <td></td>
          <td className={hasLocation(locations, { key, value }) ? "own-location" : "parent-location"}>
            { renderLocation(locations, child.tiers) }
          </td>
        </tr>,
      );

      rows = rows.concat(renderLocalityTree(locations, child));
    });
  });

  return rows;
}

interface LocalitiesProps {
  localityTree: LocalityTree;
  locationTree: LocationTree;
  refreshLocations: typeof refreshLocations;
  refreshNodes: typeof refreshNodes;
}

class Localities extends React.Component<LocalitiesProps, {}> {
  static title() {
    return <h1>Localities</h1>;
  }

  componentWillMount() {
    this.props.refreshLocations();
    this.props.refreshNodes();
  }

  componentWillReceiveProps(props: LocalitiesProps) {
    props.refreshLocations();
    props.refreshNodes();
  }

  render() {
    return (
      <section className="section">
        <table className="locality-table">
          <thead>
            <tr>
              <th>Localities</th>
              <th>Nodes</th>
              <th>Location</th>
            </tr>
          </thead>
          <tbody>
            { renderLocalityTree(this.props.locationTree, this.props.localityTree) }
          </tbody>
        </table>
      </section>
    );
  }
}

function mapStateToProps(state: AdminUIState) {
  return {
    localityTree: selectLocalityTree(state),
    locationTree: selectLocationTree(state),
  };
}

const actions = {
  refreshLocations,
  refreshNodes,
};

export default connect(mapStateToProps, actions)(Localities);
