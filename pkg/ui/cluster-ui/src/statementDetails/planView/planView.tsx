// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import React, { Fragment } from "react";
import classNames from "classnames/bind";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { Tooltip } from "@cockroachlabs/ui-components";
import styles from "./planView.module.scss";

type IAttr = cockroach.sql.ExplainTreePlanNode.IAttr;
type IExplainTreePlanNode = cockroach.sql.IExplainTreePlanNode;

const cx = classNames.bind(styles);

const WARNING_ICON = (
  <svg
    className={cx("warning-icon")}
    width="17"
    height="17"
    viewBox="0 0 24 22"
    xmlns="http://www.w3.org/2000/svg"
  >
    <path
      fillRule="evenodd"
      clipRule="evenodd"
      d="M15.7798 2.18656L23.4186 15.5005C25.0821 18.4005 22.9761 21.9972 19.6387 21.9972H4.3619C1.02395 21.9972 -1.08272 18.4009 0.582041 15.5005M0.582041 15.5005L8.21987 2.18656C9.89189 -0.728869 14.1077 -0.728837 15.7798 2.18656M13.4002 7.07075C13.4002 6.47901 12.863 5.99932 12.2002 5.99932C11.5375 5.99932 11.0002 6.47901 11.0002 7.07075V13.4993C11.0002 14.0911 11.5375 14.5707 12.2002 14.5707C12.863 14.5707 13.4002 14.0911 13.4002 13.4993V7.07075ZM13.5717 17.2774C13.5717 16.5709 12.996 15.9981 12.286 15.9981C11.5759 15.9981 11.0002 16.5709 11.0002 17.2774V17.2902C11.0002 17.9967 11.5759 18.5695 12.286 18.5695C12.996 18.5695 13.5717 17.9967 13.5717 17.2902V17.2774Z"
    />
  </svg>
);
const NODE_ICON = <span className={cx("node-icon")}>&#x26AC;</span>;

// FlatPlanNodeAttribute contains a flattened representation of IAttr[].
export interface FlatPlanNodeAttribute {
  key: string;
  values: string[];
  warn: boolean;
}

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
  attrs: FlatPlanNodeAttribute[];
  children: FlatPlanNode[][];
}

function warnForAttribute(attr: IAttr): boolean {
  // TODO(yuzefovich): 'spans ALL' is pre-20.1 attribute (and it might show up
  // during an upgrade), so we should remove the check for it after 20.2
  // release.
  if (
    attr.key === "spans" &&
    (attr.value === "FULL SCAN" || attr.value === "ALL")
  ) {
    return true;
  }
  return false;
}

// flattenAttributes takes a list of attrs (IAttr[]) and collapses
// all the values for the same key (FlatPlanNodeAttribute). For example,
// if attrs was:
//
// attrs: IAttr[] = [
//  {
//    key: "render",
//    value: "name",
//  },
//  {
//    key: "render",
//    value: "title",
//  },
// ];
//
// The returned FlatPlanNodeAttribute would be:
//
// flattenedAttr: FlatPlanNodeAttribute = {
//  key: "render",
//  value: ["name", "title"],
// };
//
export function flattenAttributes(
  attrs: IAttr[] | null,
): FlatPlanNodeAttribute[] {
  if (attrs === null) {
    return [];
  }
  const flattenedAttrsMap: { [key: string]: FlatPlanNodeAttribute } = {};
  attrs.forEach(attr => {
    const existingAttr = flattenedAttrsMap[attr.key];
    const warn = warnForAttribute(attr);
    if (!existingAttr) {
      flattenedAttrsMap[attr.key] = {
        key: attr.key,
        values: [attr.value],
        warn: warn,
      };
    } else {
      existingAttr.values.push(attr.value);
      if (warn) {
        existingAttr.warn = true;
      }
    }
  });
  const flattenedAttrs = _.values(flattenedAttrsMap);
  return _.sortBy(flattenedAttrs, attr =>
    attr.key === "table" ? "table" : "z" + attr.key,
  );
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
      name: treePlan.name,
      attrs: flattenAttributes(treePlan.attrs),
      children: [],
    },
  ];

  if (treePlan.children.length === 0) {
    return flattenedPlan;
  }
  const flattenedChildren = treePlan.children.map(child => flattenTree(child));
  if (treePlan.children.length === 1) {
    // Append single child into same list that contains parent node.
    flattenedPlan.push(...flattenedChildren[0]);
  } else {
    // Only add to children property if there are multiple children.
    flattenedPlan[0].children = flattenedChildren;
  }
  return flattenedPlan;
}

// shouldHideNode looks at node name to determine whether we should hide
// node from logical plan tree.
//
// Currently we're hiding `row source to planNode`, which is a node
// generated during execution (e.g. this is an internal implementation
// detail that will add more confusion than help to user). See #34594
// for details.
function shouldHideNode(nodeName: string): boolean {
  return nodeName === "row source to plan node";
}

/* ************************* PLAN NODES ************************* */

interface PlanNodeDetailProps {
  node: FlatPlanNode;
}

class PlanNodeDetails extends React.Component<PlanNodeDetailProps> {
  constructor(props: PlanNodeDetailProps) {
    super(props);
  }

  renderAttributeValues(values: string[]) {
    if (!values.length || !values[0].length) {
      return;
    }
    if (values.length === 1) {
      return <span> = {values[0]}</span>;
    }
    return <span> = [{values.join(", ")}]</span>;
  }

  renderAttribute(attr: FlatPlanNodeAttribute) {
    let attrClassName = "";
    let keyClassName = "nodeAttributeKey";
    if (attr.warn) {
      attrClassName = "warn";
      keyClassName = "";
    }
    return (
      <div key={attr.key} className={cx(attrClassName)}>
        {attr.warn && WARNING_ICON}
        <span className={cx(keyClassName)}>{attr.key}</span>
        {this.renderAttributeValues(attr.values)}
      </div>
    );
  }

  renderNodeDetails() {
    const node = this.props.node;
    if (node.attrs && node.attrs.length > 0) {
      return (
        <div className={cx("nodeAttributes")}>
          {node.attrs.map(attr => this.renderAttribute(attr))}
        </div>
      );
    }
  }

  render() {
    const node = this.props.node;
    return (
      <div className={cx("nodeDetails")}>
        {NODE_ICON} <b>{_.capitalize(node.name)}</b>
        {this.renderNodeDetails()}
      </div>
    );
  }
}

function PlanNodes(props: { nodes: FlatPlanNode[] }): React.ReactElement<{}> {
  const nodes = props.nodes;
  return (
    <ul>
      {nodes.map((node, idx) => {
        return <PlanNode node={node} key={idx} />;
      })}
    </ul>
  );
}

interface PlanNodeProps {
  node: FlatPlanNode;
}

class PlanNode extends React.Component<PlanNodeProps> {
  render() {
    if (shouldHideNode(this.props.node.name)) {
      return null;
    }
    const node = this.props.node;
    return (
      <li>
        <PlanNodeDetails node={node} />
        {node.children &&
          node.children.map((child, idx) => (
            <PlanNodes nodes={child} key={idx} />
          ))}
      </li>
    );
  }
}

interface PlanViewProps {
  title: string;
  plan: IExplainTreePlanNode;
}

interface PlanViewState {
  expanded: boolean;
  showExpandDirections: boolean;
}

export class PlanView extends React.Component<PlanViewProps, PlanViewState> {
  private innerContainer: React.RefObject<HTMLDivElement>;
  constructor(props: PlanViewProps) {
    super(props);
    this.state = {
      expanded: false,
      showExpandDirections: true,
    };
    this.innerContainer = React.createRef();
  }

  toggleExpanded = () => {
    this.setState(state => ({
      expanded: !state.expanded,
    }));
  };

  showExpandDirections() {
    // Only show directions to show/hide the full plan if content is longer than its max-height.
    const containerObj = this.innerContainer.current;
    return containerObj.scrollHeight > containerObj.clientHeight;
  }

  componentDidMount() {
    this.setState({ showExpandDirections: this.showExpandDirections() });
  }

  render() {
    const flattenedPlanNodes = flattenTree(this.props.plan);

    const lastSampledHelpText = (
      <Fragment>
        If the time from the last sample is greater than 5 minutes, a new plan
        will be sampled. This frequency can be configured with the cluster
        setting{" "}
        <code>
          <pre style={{ display: "inline-block" }}>
            sql.metrics.statement_details.plan_collection.period
          </pre>
        </code>
        .
      </Fragment>
    );

    return (
      <table className={cx("plan-view-table")}>
        <thead>
          <tr>
            <th className={cx("plan-view-table__cell")}>
              <h2 className={cx("base-heading", "summary--card__title")}>
                {this.props.title}
              </h2>
              <div className={cx("plan-view-table__tooltip")}>
                <Tooltip content={lastSampledHelpText}>
                  <div className={cx("plan-view-table__tooltip-hover-area")}>
                    <div className={cx("plan-view-table__info-icon")}>i</div>
                  </div>
                </Tooltip>
              </div>
            </th>
          </tr>
        </thead>
        <tbody>
          <tr className={cx("plan-view-table__row--body")}>
            <td
              className={cx("plan-view", "plan-view-table__cell")}
              style={{ textAlign: "left" }}
            >
              <div className={cx("plan-view-container")}>
                <div id="plan-view-inner-container" ref={this.innerContainer}>
                  <PlanNodes nodes={flattenedPlanNodes} />
                </div>
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    );
  }
}
