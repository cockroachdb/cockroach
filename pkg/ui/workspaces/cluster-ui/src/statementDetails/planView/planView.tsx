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
import { Fraction } from "../statementDetails";

type IAttr = cockroach.sql.ExplainTreePlanNode.IAttr;
type IExplainTreePlanNode = cockroach.sql.IExplainTreePlanNode;

const cx = classNames.bind(styles);

const NODE_ICON = <span className={cx("node-icon")}>&#8226;</span>;

// FlatPlanNodeAttribute contains a flattened representation of IAttr[].
export interface FlatPlanNodeAttribute {
  key: string;
  values: string[];
  warn: boolean;
}

// FlatPlanNode contains details for the flattened representation of
// IExplainTreePlanNode.
export interface FlatPlanNode {
  name: string;
  attrs: FlatPlanNodeAttribute[];
  children: FlatPlanNode[];
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

function fractionToString(fraction: Fraction): string {
  // The fraction denominator is the number of times the statement
  // has been executed.

  if (Number.isNaN(fraction.numerator)) {
    return "unknown";
  }

  return (fraction.numerator > 0).toString();
}

// flattenTreeAttributes takes a tree representation of a logical
// plan (IExplainTreePlanNode) and flattens the attributes in each node.
// (see flattenAttributes)
export function flattenTreeAttributes(
  treePlan: IExplainTreePlanNode,
): FlatPlanNode {
  return {
    name: treePlan.name,
    attrs: flattenAttributes(treePlan.attrs),
    children: treePlan.children.map(child => flattenTreeAttributes(child)),
  };
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

function NodeAttribute({
  attribute,
}: {
  attribute: FlatPlanNodeAttribute;
}): React.ReactElement {
  if (!attribute.values.length || !attribute.values[0].length) return null;

  const attrClassName = attribute.warn ? "warn" : "";

  const values =
    attribute.values.length > 1
      ? `[${attribute.values.join(", ")}]`
      : attribute.values[0];

  return (
    <div key={attribute.key}>
      <span className={cx("nodeAttributeKey")}>{attribute.key}:</span>{" "}
      <span className={cx(attrClassName)}>{values}</span>
    </div>
  );
}

interface PlanNodeDetailProps {
  node: FlatPlanNode;
}

class PlanNodeDetails extends React.Component<PlanNodeDetailProps> {
  constructor(props: PlanNodeDetailProps) {
    super(props);
  }

  renderNodeDetails() {
    const node = this.props.node;
    if (node.attrs && node.attrs.length > 0) {
      return (
        <div className={cx("nodeAttributes")}>
          {node.attrs.map((attr, idx) => (
            <NodeAttribute key={idx} attribute={attr} />
          ))}
        </div>
      );
    }
  }

  render() {
    const node = this.props.node;
    return (
      <div className={cx("nodeDetails")}>
        {NODE_ICON} <b>{node.name}</b>
        {this.renderNodeDetails()}
      </div>
    );
  }
}

type PlanNodeProps = { node: FlatPlanNode };

function PlanNode({ node }: PlanNodeProps): React.ReactElement {
  if (shouldHideNode(node.name)) {
    return null;
  }

  return (
    <ul>
      <PlanNodeDetails node={node} />
      {node.children &&
        node.children.map((child, idx) => (
          <li key={idx}>
            <PlanNode node={child} />
          </li>
        ))}
    </ul>
  );
}

export type GlobalPropertiesType = {
  distribution: Fraction;
  vectorized: Fraction;
};

interface PlanViewProps {
  title: string;
  plan: IExplainTreePlanNode;
  globalProperties: GlobalPropertiesType;
}

interface PlanViewState {
  expanded: boolean;
  showExpandDirections: boolean;
}

export class PlanView extends React.Component<PlanViewProps, PlanViewState> {
  private readonly innerContainer: React.RefObject<HTMLDivElement>;
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
    const { plan, globalProperties } = this.props;
    const flattenedPlanNodeRoot = flattenTreeAttributes(plan);

    const globalAttrs: FlatPlanNodeAttribute[] = [
      {
        key: "distribution",
        values: [fractionToString(globalProperties.distribution)],
        warn: false, // distribution is never warned
      },
      {
        key: "vectorized",
        values: [fractionToString(globalProperties.vectorized)],
        warn: false, // vectorized is never warned
      },
    ];

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
                <div
                  className={cx("plan-view-inner-container")}
                  ref={this.innerContainer}
                >
                  <div className={cx("nodeAttributes", "globalAttributes")}>
                    {globalAttrs.map((attr, idx) => (
                      <NodeAttribute key={idx} attribute={attr} />
                    ))}
                  </div>
                  <PlanNode node={flattenedPlanNodeRoot} />
                </div>
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    );
  }
}
