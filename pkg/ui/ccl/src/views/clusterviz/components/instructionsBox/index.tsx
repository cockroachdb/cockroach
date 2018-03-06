import React from "react";

import { numNodesWithoutLocality } from "src/util/localities";
import { NodeStatus$Properties } from "src/util/proto";
import nodeMapScreenshot from "assets/nodeMapScreenshot.png";
import "./instructionsBox.styl";

interface InstructionsBoxProps {
  allNodes: NodeStatus$Properties[];
}

interface InstructionBoxState {
  expanded: boolean;
}

const DOCS_LINK = "http://cockroach-docs-review.s3-website-us-east-1.amazonaws.com/50893285dcb2e74c603d34103597b83f715a4c31/dev/admin-ui-node-map.html#configure-and-navigate-the-node-map";

const LOCAL_STORAGE_KEY = "clusterviz-instruction-box-expanded";

interface Step {
  num: number;
  text: React.ReactNode;
}

export class InstructionsBox extends React.Component<InstructionsBoxProps, InstructionBoxState> {
  constructor() {
    super();
    const valFromStorage = localStorage.getItem(LOCAL_STORAGE_KEY);
    let expanded = true;
    if (valFromStorage === "false") {
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

  getNextTodo(): Step {
    const nodesWithoutLocality = numNodesWithoutLocality(this.props.allNodes);

    if (nodesWithoutLocality > 0) {
      return {
        num: 1,
        text: (
          <span>
            Start every node in the cluster with a <code>--locality</code> flag.
            <br />
            ({nodesWithoutLocality} currently missing it)
          </span>
        ),
      };
    }
    return {
      num: 2,
      text: (
        <span>
          Add locations to the <code>system.locations</code> table corresponding to
          your localities.
        </span>
      ),
    };
  }

  renderExpanded() {
    const nextTodo = this.getNextTodo();

    return (
      <div className="instructions-box instructions-box--expanded">
        <div className="instructions-box-top-bar">
          <div>
            <span className="instructions-box-top-bar__see_nodes">
              See your nodes on a map!
            </span>{" "}
            <a
              href={DOCS_LINK}
              className="instructions-box-top-bar__setup_link"
            >
              Follow our setup guide
            </a>
          </div>
          <span
            className="instructions-box-top-bar__x_out"
            onClick={this.handleToggle.bind(this)}
          >
            ✕
          </span>
        </div>
        <div className="instructions-box-content">
          <p className="instructions-box-content__instruction">
            <span className={"instructions-box-content__step_num"}>Step {nextTodo.num}:</span>{" "}
            {nextTodo.text}
          </p>
          <img className="instructions-box-content__screenshot" src={nodeMapScreenshot} />
        </div>
      </div>
    );
  }

  renderCollapsed() {
    return (
      <div
        className="instructions-box instructions-box--collapsed"
        onClick={this.handleToggle.bind(this)}
      >
        ?
      </div>
    );
  }

  render() {
    if (this.state.expanded) {
      return this.renderExpanded();
    } else {
      return this.renderCollapsed();
    }
  }
}
