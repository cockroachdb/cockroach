import React from "react";
import { connect, Dispatch } from "react-redux";

import { numNodesWithoutLocality } from "src/util/localities";
import {
  INSTRUCTION_BOX_EXPANDED_KEY,
  instructionBoxExpandedPersistentSelector,
} from "src/redux/alerts";
import { AdminUIState } from "src/redux/state";
import { nodeStatusesSelector } from "src/redux/nodes";
import { NodeStatus$Properties } from "src/util/proto";
import { saveUIData } from "oss/src/redux/uiData";
import nodeMapScreenshot from "assets/nodeMapScreenshot.png";
import "./instructionsBox.styl";

interface InstructionsBoxProps {
  allNodes: NodeStatus$Properties[];
  expanded: boolean;
  setExpanded: (expanded: boolean) => void;
}

const DOCS_LINK = "http://cockroach-docs-review.s3-website-us-east-1.amazonaws.com/50893285dcb2e74c603d34103597b83f715a4c31/dev/admin-ui-node-map.html#configure-and-navigate-the-node-map";

interface Step {
  num: number;
  text: React.ReactNode;
}

class InstructionsBox extends React.Component<InstructionsBoxProps> {
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
            onClick={() => this.props.setExpanded(false)}
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
        onClick={() => this.props.setExpanded(true)}
      >
        ?
      </div>
    );
  }

  render() {
    if (this.props.expanded) {
      return this.renderExpanded();
    } else {
      return this.renderCollapsed();
    }
  }
}

function mapStateToProps(state: AdminUIState) {
  return {
    expanded: instructionBoxExpandedPersistentSelector(state),
    allNodes: nodeStatusesSelector(state),
  };
}

function mapDispatchToProps(dispatch: Dispatch<AdminUIState>) {
  return {
    setExpanded: (expanded: boolean) => {
      const action = saveUIData({
        key: INSTRUCTION_BOX_EXPANDED_KEY,
        value: expanded,
      });
      console.log("ACTION", expanded, action);
      dispatch(action);
      return;
    },
  };
}

export default connect(mapStateToProps, mapDispatchToProps)(InstructionsBox);
