import React from "react";

import { LocalityTree } from "src/redux/localities";
import { LocationTree } from "src/redux/locations";
import { getConfigStatus } from "src/util/locations";
import { pluralize } from "oss/src/util/pluralize";
import "./instructionsBox.styl";

interface InstructionsBoxProps {
  localityTree: LocalityTree;
  locationTree: LocationTree;
}

export class InstructionsBox extends React.Component<InstructionsBoxProps> {
  constructor() {
    super();
    this.state = {
      collapsed: false,
    };
  }

  render() {
    const status = getConfigStatus(this.props.localityTree, this.props.locationTree);

    const todos: string[] = [];
    if (status.withoutLocality > 0) {
      todos.push(
        `add localities to ${status.withoutLocality} ` +
        pluralize(status.withoutLocality, "node", "nodes"),
      );
    }
    // TODO: you add location to localities, not nodes.
    if (status.withoutLocation) {
      todos.push(
        `add location info to ${status.withoutLocation} ` +
        pluralize(status.withoutLocation, "node", "nodes"),
      );
    }

    return (
      <div className="instructions-box">
        To see a map, {todos.join(" and ")}.
        <br />
        <a href="http://cockroachlabs.com/docs">Docs</a>
      </div>
    );
  }
}
