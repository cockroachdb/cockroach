import React from "react";

import { LocalityTree } from "src/redux/localities";
import { LocationTree } from "src/redux/locations";
import { getConfigStatus } from "src/util/locations";
import { pluralize } from "src/util/pluralize";
import nodeMapScreenshot from "assets/nodeMapScreenshot.png";
import "./instructionsBox.styl";

interface InstructionsBoxProps {
  localityTree: LocalityTree;
  locationTree: LocationTree;
}

interface InstructionBoxState {
  expanded: boolean;
}

const LOCAL_STORAGE_KEY = "clusterviz-instruction-box-expanded";

export class InstructionsBox extends React.Component<InstructionsBoxProps, InstructionBoxState> {
  constructor() {
    super();
    const valFromStorage = localStorage.getItem(LOCAL_STORAGE_KEY);
    let expanded = true;
    switch (valFromStorage) {
      case "false":
        expanded = false;
    }
    this.state = {
      expanded,
    };
  }

  handleToggle = () => {
    const expanded = !this.state.expanded;
    const val = `${expanded}`;
    localStorage.setItem(LOCAL_STORAGE_KEY, val);
    this.setState({
      expanded,
    });
  }

  renderExpanded() {
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
      <div style={{ maxWidth: 220 }}>
        <p>
          To see a map, {todos.join(" and ")}.{" "}
          (<a href="http://cockroachlabs.com/docs">See Docs</a>)
        </p>
        <img src={nodeMapScreenshot} width={220} />
      </div>
    );
  }

  renderCollapsed() {
    return "?";
  }

  render() {
    let content = null;
    if (this.state.expanded) {
      content = this.renderExpanded();
    } else {
      content = this.renderCollapsed();
    }

    return (
      <div onClick={() => this.handleToggle()} className="instructions-box">
        {content}
      </div>
    );
  }
}
