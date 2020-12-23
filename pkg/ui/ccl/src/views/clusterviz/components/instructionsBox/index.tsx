// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";
import { Dispatch, Action } from "redux";
import { connect } from "react-redux";
import { Link } from "react-router-dom";
import classNames from "classnames";

import { allNodesHaveLocality } from "src/util/localities";
import {
  instructionsBoxCollapsedSelector,
  setInstructionsBoxCollapsed,
} from "src/redux/alerts";
import { AdminUIState } from "src/redux/state";
import { nodeStatusesSelector } from "src/redux/nodes";
import { LocalityTier } from "src/redux/localities";
import * as docsURL from "src/util/docs";
import nodeMapScreenshot from "assets/nodeMapSteps/3-seeMap.png";
import questionMap from "assets/questionMap.svg";
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

function mapDispatchToProps(dispatch: Dispatch<Action, AdminUIState>) {
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
