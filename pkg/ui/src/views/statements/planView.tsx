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
import IAttr = cockroach.sql.ExplainTreePlanNode.IAttr;
import IExplainTreePlanNode = cockroach.sql.IExplainTreePlanNode;
import { ToolTipWrapper } from "src/views/shared/components/toolTip";

const WARNING_ICON = (
  <svg className="warning-icon" width="17" height="17" viewBox="0 0 24 22" xmlns="http://www.w3.org/2000/svg">
    <path fill-rule="evenodd" clip-rule="evenodd" d="M15.7798 2.18656L23.4186 15.5005C25.0821 18.4005 22.9761 21.9972 19.6387 21.9972H4.3619C1.02395 21.9972 -1.08272 18.4009 0.582041 15.5005M0.582041 15.5005L8.21987 2.18656C9.89189 -0.728869 14.1077 -0.728837 15.7798 2.18656M13.4002 7.07075C13.4002 6.47901 12.863 5.99932 12.2002 5.99932C11.5375 5.99932 11.0002 6.47901 11.0002 7.07075V13.4993C11.0002 14.0911 11.5375 14.5707 12.2002 14.5707C12.863 14.5707 13.4002 14.0911 13.4002 13.4993V7.07075ZM13.5717 17.2774C13.5717 16.5709 12.996 15.9981 12.286 15.9981C11.5759 15.9981 11.0002 16.5709 11.0002 17.2774V17.2902C11.0002 17.9967 11.5759 18.5695 12.286 18.5695C12.996 18.5695 13.5717 17.9967 13.5717 17.2902V17.2774Z"/>
  </svg>
);
const NODE_ICON = (
  <span className="node-icon">&#x26AC;</span>
);
const UP_ARROW_ICON = (
  <svg className="arrow-icon" viewBox="0 0 20 11" width="10" height="10">
    <polyline
      stroke-linecap="round"
      points="2,10 10,1 18,10"
    />
  </svg>
);
const DOWN_ARROW_ICON = (
  <svg className="arrow-icon" viewBox="0 0 20 11" width="10" height="10">
    <polyline
      stroke-linecap="round"
      points="2,1 10,10 18,1"
    />
  </svg>
);

// FlatPlanNode contains details for the flattened representation of
// IExplainTreePlanNode.
//
// Note that the function that flattens IExplainTreePlanNode returns
// an array of FlatPlanNode (not a single FlatPlanNode). E.g.:
//
//  flattenTree(IExplainTreePlanNode) => FlatPlanNode[]
//
export interface FlatPlanNode {
  name: string;
  attrs: IAttr[];
  children: FlatPlanNode[][];
}

/* ************************* HELPER FUNCTIONS ************************* */

// flattenTree takes a tree representation of a logical plan
// (IExplainTreePlanNode) and flattens any single child paths.
// For example, if an IExplainTreePlanNode was visually displayed
// as:
//
//    root
//       |
//       |___ single_grandparent
//                  |
//                  |____ parent_1
//                  |         |
//                  |         |______ single_child
//                  |
//                  |____ parent_2
//
// Then its FlatPlanNode[] equivalent would be visually displayed
// as:
//
//    root
//       |
//    single_grandparent
//       |
//       |____ parent_1
//       |          |
//       |     single_child
//       |
//       |____ parent_2
//
export function flattenTree(treePlan: IExplainTreePlanNode): FlatPlanNode[] {
  const flattenedPlan: FlatPlanNode[] = [
    {
      "name": treePlan.name,
      "attrs": treePlan.attrs,
      "children": [],
    },
  ];

  if (treePlan.children.length === 0) {
    return flattenedPlan;
  }
  const flattenedChildren =
    treePlan.children
      .map( (child) => flattenTree(child));
  if (treePlan.children.length === 1) {
    // Append single child into same list that contains parent node.
    flattenedPlan.push(...flattenedChildren[0]);
  } else {
    // Only add to children property if there are multiple children.
    flattenedPlan[0].children = flattenedChildren;
  }
  return flattenedPlan;
}

interface PlanNodeHeaderProps {
  title: string;
  subtitle: string;
  warn: boolean;
}

// planNodeHeaderProps looks at a node's name and attributes and determines if:
// 1) We should warn the user about this particular node,
// 2) We should include any attribute details in the collapsed node view.
export function planNodeHeaderProps(node: FlatPlanNode): PlanNodeHeaderProps {
  let title = node.name;
  let subtitle = null;
  let warn = false;

  switch (node.name) {
    case "scan":
      if (_.find(node.attrs, {key: "spans", value: "ALL"})) {
        const tableAttr = _.filter(node.attrs, (attr) => ( attr.key === "table"));
        if (tableAttr.length >= 1) {
          title = "table scan";
          warn = true;
          subtitle = tableAttr[0].value;
          const ampersandPosition = subtitle.indexOf("@");
          if (ampersandPosition > 0) {
            subtitle = subtitle.substring(0, ampersandPosition);
          }
        }
      }
      break;
    case "join":
      const typeAttr = _.filter(node.attrs, (attr) => ( attr.key === "type"));
      title = typeAttr[0].value + " " + node.name;
      break;
    default:
      break;
  }

  return {
    title, subtitle, warn,
  };
}

/* ************************* PLAN NODES ************************* */

interface PlanNodeDetailProps {
  node: FlatPlanNode;
}

interface PlanNodeDetailState {
  expanded: boolean;
}

class PlanNodeDetails extends React.Component<PlanNodeDetailProps, PlanNodeDetailState> {
  constructor(props: PlanNodeDetailProps) {
    super(props);
    this.state = {
      expanded: false,
    };
  }

  toggleExpanded = () => {
    this.setState(state => ({
      expanded: !state.expanded,
    }));
  }

  renderPlanNodeHeader(props: PlanNodeHeaderProps) {
    return (
      <span>
        {props.warn && WARNING_ICON}
          {!props.warn && NODE_ICON}
          {_.capitalize(props.title)}
          {props.subtitle &&
          <span>: <b>{props.subtitle}</b></span>
          }
      </span>
    );
  }

  renderArrow() {
    const node = this.props.node;
    if (node.attrs && node.attrs.length > 0) {
      if (this.state.expanded) {
        return <div className="arrow-expanded">{UP_ARROW_ICON}</div>;
      }
      return <div className="arrow">{DOWN_ARROW_ICON}</div>;
    }
  }

  renderMaybeExpandedDetail() {
    const node = this.props.node;
    if (node.attrs && node.attrs.length > 0) {
      if (this.state.expanded) {
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
    const hasAttributes = node.attrs && node.attrs.length > 0;
    const warnClassName = (warn && " warn" || "");
    if (!hasAttributes) {
      return "nodeDetails" + warnClassName;
    }
    if (this.state.expanded) {
      return "nodeDetails hasAttributes expanded" + warnClassName;
    }
    return "nodeDetails hasAttributes" + warnClassName;
  }

  render() {
    const node = this.props.node;
    const headerProps = planNodeHeaderProps(node);
    return (
      <div
        className={this.renderClassName(headerProps.warn)}
        onClick={() => this.toggleExpanded()}
      >
        {this.renderPlanNodeHeader(headerProps)} {this.renderArrow()}
        {this.renderMaybeExpandedDetail()}
      </div>
    );
  }
}

interface PlanNodeProps {
  node: FlatPlanNode;
}

class PlanNode extends React.Component<PlanNodeProps> {
  render() {
    const node = this.props.node;
    return (
      <li>
        <PlanNodeDetails
          node={node}
        />
        {node.children && node.children.map( (child) => (
          <PlanNodes
            nodes={child}
          />
        ))
        }
      </li>
    );
  }
}

function PlanNodes(props: {
  nodes: FlatPlanNode[],
}): React.ReactElement<{}> {
  const nodes = props.nodes;
  return (
    <ul>
      {nodes.map( (node) => {
        return <PlanNode node={node}/>;
      })}
    </ul>
  );
}

export function PlanView(props: {
  title: string,
  plan: IExplainTreePlanNode,
}) {
  const flattenedPlanNodes = flattenTree(props.plan);

  const lastSampledHelpText = (
    <React.Fragment>
      If the time from the last sample is greater than 5 minutes, a new
      plan will be sampled. This frequency can be configured
      with the cluster setting{" "}
      <code>
        <pre style={{ display: "inline-block" }}>
          sql.metrics.statement_details.plan_collection.period
        </pre>
      </code>.
    </React.Fragment>
  );

  return (
    <table className="plan-view-table">
      <thead>
      <tr className="plan-view-table__row--header">
        <th className="plan-view-table__cell">
          {props.title}
          <div className="plan-view-table__tooltip">
            <ToolTipWrapper
              text={lastSampledHelpText}>
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
    </table>
  );
}
