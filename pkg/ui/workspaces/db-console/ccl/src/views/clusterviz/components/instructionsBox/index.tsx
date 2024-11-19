// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames";
import React from "react";
import { connect } from "react-redux";
import { Link } from "react-router-dom";

import nodeMapScreenshot from "assets/nodeMapSteps/3-seeMap.png";
import questionMap from "assets/questionMap.svg";
import {
  instructionsBoxCollapsedSelector,
  setInstructionsBoxCollapsed,
} from "src/redux/alerts";
import { LocalityTier } from "src/redux/localities";
import { nodeStatusesSelector } from "src/redux/nodes";
import { AdminUIState, AppDispatch } from "src/redux/state";
import * as docsURL from "src/util/docs";
import { allNodesHaveLocality } from "src/util/localities";
import "./instructionsBox.styl";

interface InstructionsBoxProps {
  allNodesHaveLocality: boolean;
  collapsed: boolean;
  expand: () => void;
  collapse: () => void;
}

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
              href={docsURL.enableNodeMap}
              target="_blank"
              rel="noreferrer"
              className="instructions-box-top-bar__setup_link"
            >
              Follow our configuration guide
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
              className={classNames("instructions-box-content__todo-item", {
                "instructions-box-content__todo-item--done": firstTodoDone,
              })}
            >
              {firstTodoDone ? (
                <div className="instructions-box-content__todo-check">
                  {"\u2714"}
                </div>
              ) : null}
              Ensure every node in your cluster was started with a{" "}
              <code>--locality</code> flag. (
              <Link to={"/reports/localities"}>See current locality tree</Link>)
            </li>
            <li>
              Add locations to the <code>system.locations</code> table
              corresponding to your locality flags.
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
        <img src={questionMap} />
      </div>
    );
  }

  render() {
    if (this.props.collapsed) {
      return this.renderCollapsed();
    } else {
      return this.renderExpanded();
    }
  }
}

function mapStateToProps(state: AdminUIState) {
  return {
    collapsed: instructionsBoxCollapsedSelector(state),
    allNodesHaveLocality: allNodesHaveLocality(nodeStatusesSelector(state)),
  };
}

function mapDispatchToProps(dispatch: AppDispatch) {
  return {
    expand: () => dispatch(setInstructionsBoxCollapsed(false)),
    collapse: () => dispatch(setInstructionsBoxCollapsed(true)),
  };
}

export default connect(mapStateToProps, mapDispatchToProps)(InstructionsBox);

// Helper functions.

/**
 * showInstructionBox decides whether to show the instructionBox.
 */
export function showInstructionsBox(
  showMap: boolean,
  tiers: LocalityTier[],
): boolean {
  const atTopLevel = tiers.length === 0;
  return atTopLevel && !showMap;
}
