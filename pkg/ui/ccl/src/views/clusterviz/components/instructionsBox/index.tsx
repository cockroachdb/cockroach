import React from "react";
import { connect, Dispatch } from "react-redux";
import { Link } from "react-router";
import classNames from "classnames";

import { allNodesHaveLocality } from "src/util/localities";
import {
  instructionsBoxExpandedSelector, setInstructionsBoxExpanded,
} from "src/redux/alerts";
import { AdminUIState } from "src/redux/state";
import { nodeStatusesSelector } from "src/redux/nodes";
import { LocalityTier } from "src/redux/localities";
import docsURL from "src/util/docs";
import nodeMapScreenshot from "assets/nodeMapSteps/3-seeMap.png";
import "./instructionsBox.styl";

interface InstructionsBoxProps {
  allNodesHaveLocality: boolean;
  expanded: boolean;
  expand: () => void;
  collapse: () => void;
}

const DOCS_LINK = docsURL("enable-node-map.html");

class InstructionsBox extends React.Component<InstructionsBoxProps> {
  renderExpanded() {
    const firstTodoDone = this.props.allNodesHaveLocality;

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
            className="instructions-box-top-bar__x-out"
            onClick={this.props.collapse}
          >
            ✕
          </span>
        </div>
        <div className="instructions-box-content">
          <ol>
            <li
              className={classNames(
                "instructions-box-content__todo-item",
                { "instructions-box-content__todo-item--done": firstTodoDone },
              )}
            >
              Ensure every node in your cluster was started with a <code>--locality</code> flag.
              (<Link to={"/reports/localities"}>See current locality tree</Link>)
            </li>
            <li>
              Add locations to the <code>system.locations</code> table corresponding to
              your locality flags.
            </li>
          </ol>
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
        onClick={this.props.expand}
      >
        ?
      </div>
    );
  }

  render() {
    if (this.props.expanded) {
      try {
        return this.renderExpanded();
      } catch (e) {
        console.error(e);
      }
    } else {
      return this.renderCollapsed();
    }
  }
}

function mapStateToProps(state: AdminUIState) {
  return {
    expanded: instructionsBoxExpandedSelector(state),
    allNodesHaveLocality: allNodesHaveLocality(nodeStatusesSelector(state)),
  };
}

function mapDispatchToProps(dispatch: Dispatch<AdminUIState>) {
  return {
    expand: () => dispatch(setInstructionsBoxExpanded(true)),
    collapse: () => dispatch(setInstructionsBoxExpanded(false)),
  };
}

export default connect(mapStateToProps, mapDispatchToProps)(InstructionsBox);

// Helper functions.

/**
 * showInstructionBox decides whether to show the instructionBox.
 */
export function showInstructionsBox(showMap: boolean, tiers: LocalityTier[]): boolean {
  const atTopLevel = tiers.length === 0;
  return atTopLevel && !showMap;
}
