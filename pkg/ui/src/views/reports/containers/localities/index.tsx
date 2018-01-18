import _ from "lodash";
import React from "react";
import { connect } from "react-redux";

import { refreshNodes, refreshLocations } from "src/redux/apiReducers";
import { selectLocalityTree, LocalityTree } from "src/redux/localities";
import { selectLocationTree, LocationTree } from "src/redux/locations";
import { AdminUIState } from "src/redux/state";

import "./localities.styl";

export function getLocation(locations: LocationTree, key: string, value: string) {
  if (!locations[key]) {
    return null;
  }

  return locations[key][value];
}

function formatCoord(coordinate: number) {
  return coordinate.toFixed(4);
}

function renderLocation(locations: LocationTree, key: string, value: string) {
  const location = getLocation(locations, key, value);

  if (_.isNil(location)) {
    return "";
  }

  return `${formatCoord(location.latitude)}, ${formatCoord(location.longitude)}`;
}

function renderLocalityTree(locations: LocationTree, tree: LocalityTree, depth = 0) {
  let rows: React.ReactNode[] = [];

  tree.nodes.forEach((node) => {
    rows.push(
      <tr>
        <td></td>
        <td>n{ node.desc.node_id } @ { node.desc.address.address_field }</td>
        <td></td>
      </tr>,
    );
  });

  Object.keys(tree.localities).forEach((key) => {
    Object.keys(tree.localities[key]).forEach((value) => {
      rows.push(
        <tr>
          <td className={`depth-${depth}`}>{ key }={ value }</td>
          <td></td>
          <td>{ renderLocation(locations, key, value) }</td>
        </tr>,
      );

      rows = rows.concat(renderLocalityTree(locations, tree.localities[key][value], depth + 1));
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
