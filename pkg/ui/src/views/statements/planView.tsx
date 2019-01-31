// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import _ from "lodash";
import React from "react";

import { cockroach } from "src/js/protos";
import IExplainTreePlanNode = cockroach.sql.IExplainTreePlanNode;
import IAttr = cockroach.sql.ExplainTreePlanNode.IAttr;
import {ToolTipWrapper} from "oss/src/views/shared/components/toolTip";

interface IPlanNode {
  name: string;
  attrs: IAttr[];
  children: IPlanNode[][];
}

/* ************************* HELPER FUNCTIONS ************************* */

function planNode(name: string, attrs: any, children: IPlanNode[][]): IPlanNode {
  return {
    "name": name,
    "attrs": attrs,
    "children": children,
  };
}

// TODO(celia): explain.
function flattenTree(treePlan: IExplainTreePlanNode): IPlanNode[] {
  const flattenedPlan = [ planNode(
    treePlan.name,
    treePlan.attrs,
    []),
  ];

  if (treePlan.children.length === 0) {
    return flattenedPlan;
  }
  const flattenedChildren =
    treePlan.children
      .map( (child) => flattenTree(child));
  if (treePlan.children.length === 1) {
    flattenedPlan.push(...flattenedChildren[0]);
  } else {
    flattenedPlan[0].children = flattenedChildren;
  }
  return flattenedPlan;
}

interface IPlanNodeHeaderProps {
  title: string;
  subtitle: string;
  warn: boolean;
}

// TODO(celia): explain.
function planNodeHeaderProps(node: IPlanNode): IPlanNodeHeaderProps {
  let title = _.capitalize(node.name);
  let subtitle = null;
  let warn = false;

  switch (node.name) {
    case "scan":
      title = "Table scan";
      if (_.find(node.attrs, {key: "spans", value: "ALL"})) {
        warn = true;
        const tableAttr = _.filter(node.attrs, (attr) => ( attr.key === "table"));
        subtitle = tableAttr[0].value;
        const ampersandPosition = subtitle.indexOf("@");
        if (ampersandPosition > 0) {
          subtitle = subtitle.substring(0, ampersandPosition - 1);
        }
      }
      break;
    case "join":
      const typeAttr = _.filter(node.attrs, (attr) => ( attr.key === "type"));
      title = _.capitalize(typeAttr[0].value + " " + node.name);
      break;
    default:
      break;
  }

  return {
    title, subtitle, warn,
  };
}

/* ************************* PLAN NODES ************************* */

function PlanNodeDescription (props: {
  header: IPlanNodeHeaderProps;
}) {
  return <span>
    {props.header.warn &&
    <span>&#x26A0; </span>
    }
    {!props.header.warn &&
    <span>&#x26AC; </span>
    }
    {props.header.title}
    {props.header.subtitle &&
    <span>: <b>{props.header.subtitle}</b></span>
    }
  </span>;
}

interface IPlanNodeDetailProps {
  node: IPlanNode;
  expanded: boolean;
  toggleExpanded: () => void;
}

class PlanNodeDetails extends React.Component<IPlanNodeDetailProps> {

  renderArrow() {
    const node = this.props.node;
    if (node.attrs && node.attrs.length > 0) {
      if (this.props.expanded) {
        // up arrow
        return <div className="arrow-expanded">&uarr;</div>;
      }
      // down arrow
      return <div className="arrow">&darr;</div>;
    }
  }

  renderMaybeExpandedDetail() {
    const node = this.props.node;
    if (node.attrs && node.attrs.length > 0) {
      if (this.props.expanded) {
        return (
          <div className="nodeAttributes">
            {node.attrs.map( (attr) => (
              <div key={attr.key} className="attr">
                {attr.key}
                {attr.value &&
                  <span>={attr.value}</span>
                }
              </div>
            ))}
          </div>
        );
      }
    }
  }

  renderClassName(warn: boolean) {
    const node = this.props.node;
    if (node.attrs && node.attrs.length > 0 && this.props.expanded) {
      if (warn) {
        return "nodeDetails warn expanded";
      }
      return "nodeDetails expanded";
    } else if (warn) {
      return "nodeDetails warn";
    }
    return "nodeDetails";
  }

  render() {
    const node = this.props.node;
    const header = planNodeHeaderProps(node);
    return (
      <div
        className={this.renderClassName(header.warn)}
        onClick={() => {
          this.props.toggleExpanded();
        }}
      >
        <PlanNodeDescription
          header={header}
        /> {this.renderArrow()}
        {this.renderMaybeExpandedDetail()}
      </div>
    );
  }
}

interface IPlanNodeProps {
  node: IPlanNode;
}

interface IPlanNodeState {
  expanded: boolean;
}

class PlanNode extends React.Component<IPlanNodeProps, IPlanNodeState> {
  constructor(props: IPlanNodeProps) {
    super(props);
    this.state = {
      expanded: false,
    };

    // This binding is necessary to make `this` work in the callback
    this.toggleExpanded = this.toggleExpanded.bind(this);
  }

  // TODO(celia): toggleExpanded() was originally inside <PlanNodeDetails/>,
  // but I've moved this here for now, because the outer <li> may need
  // to know state.expanded in order to finish/fix up the styling of the
  // node connecting lines. Should move this back into <PlanNodeDetails/>
  // if <li> ends up not needing to know about expanded.
  toggleExpanded() {
    this.setState(state => ({
      expanded: !state.expanded,
    }));
  }

  renderClassName() {
    const node = this.props.node;
    const expanded = false; /* TODO */
    if (node.attrs && node.attrs.length > 0 && expanded) {
      return "expanded";
    }
  }

  render() {
    const node = this.props.node;
    const expanded = this.state.expanded;
    return <li className={this.renderClassName()}>
      <PlanNodeDetails
        node={node}
        expanded={expanded}
        toggleExpanded={this.toggleExpanded}
      />
      {node.children && node.children.map( (child) => (
        <PlanNodes
          nodes={child}
        />
      ))
      }
    </li>;
  }
}

function PlanNodes(props: {
  nodes: IPlanNode[],
}): React.ReactElement<{}> {
  const nodes = props.nodes;
  return <ul>
    {nodes.map( (node) => {
      return <PlanNode node={node}/>;
    })}
  </ul>;
}

export function PlanView(props: {
  title: string,
  plan: IExplainTreePlanNode,
}) {
  const flattenedPlanNodes = flattenTree(props.plan);
  return <table className="plan-view-table">
    <thead>
    <tr className="plan-view-table__row--header">
      <th className="plan-view-table__cell">
        {props.title}
        <div className="plan-view-table__tooltip">
          <ToolTipWrapper
            text="This is the most recently sampled plan. Plans are sampled every five minutes.">
            <div className="plan-view-table__tooltip-hover-area">
              <div className="plan-view-table__info-icon">i</div>
            </div>
          </ToolTipWrapper>
        </div>
      </th>
    </tr>
    </thead>
    <tbody>
    <tr className="plan-view-table__row--body">
      <td className="plan-view plan-view-table__cell" style={{ textAlign: "left" }}>
        <PlanNodes
          nodes={flattenedPlanNodes}
        />
      </td>
    </tr>
    </tbody>
  </table>;
}
