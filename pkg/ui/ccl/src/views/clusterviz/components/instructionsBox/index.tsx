import React from "react";
import { connect, Dispatch } from "react-redux";
import _ from "lodash";

import { numNodesWithoutLocality } from "src/util/localities";
import {
  instructionsBoxExpandedSetting,
  instructionsBoxExpandedSelector,
} from "src/redux/alerts";
import { INSTRUCTIONS_BOX_EXPANDED_KEY, saveUIData } from "src/redux/uiData";
import { AdminUIState } from "src/redux/state";
import { nodeStatusesSelector } from "src/redux/nodes";
import { NodeStatus$Properties } from "src/util/proto";
import { LocalityTier, LocalityTree } from "src/redux/localities";
import nodeMapScreenshot from "assets/nodeMapSteps/3-seeMap.png";
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
  renderExpanded() {
    const nextTodo = getNextTodo(this.props.allNodes);

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
          <div className="instructions-box-content__screenshot">
            <img src={nodeMapScreenshot} />
          </div>
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
    expanded: instructionsBoxExpandedSelector(state),
    allNodes: nodeStatusesSelector(state),
  };
}

function mapDispatchToProps(dispatch: Dispatch<AdminUIState>) {
  return {
    setExpanded: (expanded: boolean) => {
      dispatch(instructionsBoxExpandedSetting.set(expanded));
      dispatch(saveUIData({
        key: INSTRUCTIONS_BOX_EXPANDED_KEY,
        value: expanded,
      }));
    },
  };
}

export default connect(mapStateToProps, mapDispatchToProps)(InstructionsBox);

// Helper functions.

interface Step {
  num: number;
  text: React.ReactNode;
}

/**
 * showInstructionBox decides whether to show the instructionBox.
 * Exported for testing.
 *
 * It returns true if, with more configuration, the user could see
 * a map here. This is false if the map is already being shown.
 * It's also false if we're down a level and looking at all nodes.
 */
export function showInstructionsBox(
  showMap: boolean, tiers: LocalityTier[], localityTree: LocalityTree,
): boolean {
  if (showMap) {
    return false;
  }
  const downALevel = tiers.length > 0;
  const justNodes = _.size(localityTree.localities) === 0;
  return !(downALevel && justNodes);
}

export function getNextTodo(allNodes: NodeStatus$Properties[]): Step {
  const nodesWithoutLocality = numNodesWithoutLocality(allNodes);

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
