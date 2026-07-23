// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { Tooltip } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import sortBy from "lodash/sortBy";
import values from "lodash/values";
import React, { Fragment } from "react";

import {
  getAttributeTooltip,
  getOperatorTooltip,
  getAttributeValueTooltip,
} from "./planTooltips";
import styles from "./planView.module.scss";

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
  return attr.key === "spans" && attr.value === "FULL SCAN";
}

// planNodeAttrsToString converts an array of FlatPlanNodeAttribute[] into a string.
export function planNodeAttrsToString(attrs: FlatPlanNodeAttribute[]): string {
  return attrs.map(attr => `${attr.key} ${attr.values.join(" ")}`).join(" ");
}

// planNodeToString recursively converts a FlatPlanNode into a string.
export function planNodeToString(plan: FlatPlanNode): string {
  const str = `${plan.name} ${planNodeAttrsToString(plan.attrs)}`;

  if (plan.children.length > 0) {
    return `${str} ${plan.children
      .map(child => planNodeToString(child))
      .join(" ")}`;
  }

  return str;
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
  const flattenedAttrs = values(flattenedAttrsMap);
  return sortBy(flattenedAttrs, attr =>
    attr.key === "table" ? "table" : "z" + attr.key,
  );
}

/* ************************* HELPER FUNCTIONS ************************* */

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

// standardizeKey converts strings separated by whitespace and/or
// hyphens to camel case. '(anti)' is also removed from the resulting string.
export function standardizeKey(str: string): string {
  return str
    .toLowerCase()
    .split(/[ -]+/)
    .filter(str => str !== "(anti)")
    .map((str, i) =>
      i === 0 ? str : str.charAt(0).toUpperCase().concat(str.substring(1)),
    )
    .join("");
}

/* ************************* PLAN NODES ************************* */

function NodeAttribute({
  attribute,
}: {
  attribute: FlatPlanNodeAttribute;
}): React.ReactElement {
  if (!attribute.values.length || !attribute.values[0].length) return null;

  const values =
    attribute.values.length > 1
      ? `[${attribute.values.join(", ")}]`
      : attribute.values[0];

  const tooltipContent = getAttributeTooltip(standardizeKey(attribute.key));
  const attributeValTooltip =
    attribute.values.length > 1
      ? null
      : getAttributeValueTooltip(standardizeKey(attribute.values[0]));

  return (
    <div>
      <span
        className={cx(
          "node-attribute-key",
          tooltipContent && "underline-tooltip",
        )}
      >
        <Tooltip content={tooltipContent} placement="bottom">
          {`${attribute.key}:`}
        </Tooltip>
      </span>{" "}
      <span
        className={cx(
          "node-attribute",
          attribute.warn && "warn",
          attributeValTooltip && "underline-tooltip",
        )}
      >
        <Tooltip placement="bottom" content={attributeValTooltip}>
          {values}
        </Tooltip>
      </span>
    </div>
  );
}

interface PlanNodeDetailProps {
  node: FlatPlanNode;
}

function PlanNodeDetails({ node }: PlanNodeDetailProps): React.ReactElement {
  const tooltipContent = getOperatorTooltip(standardizeKey(node.name));

  return (
    <div className={cx("node-details")}>
      {NODE_ICON}{" "}
      <b className={tooltipContent && cx("underline-tooltip")}>
        <Tooltip placement="bottom" content={tooltipContent}>
          {node.name}
        </Tooltip>
      </b>
      {node.attrs && node.attrs.length > 0 && (
        <div className={cx("node-attributes")}>
          {node.attrs.map((attr, idx) => (
            <NodeAttribute key={idx} attribute={attr} />
          ))}
        </div>
      )}
    </div>
  );
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
  distribution: boolean;
  vectorized: boolean;
};

interface PlanViewProps {
  title: string;
  plan: IExplainTreePlanNode;
  globalProperties: GlobalPropertiesType;
}

export function PlanView({
  title,
  plan,
  globalProperties,
}: PlanViewProps): React.ReactElement {
  const flattenedPlanNodeRoot = flattenTreeAttributes(plan);

  const globalAttrs: FlatPlanNodeAttribute[] = [
    {
      key: "distribution",
      values: [globalProperties.distribution.toString()],
      warn: false, // distribution is never warned
    },
    {
      key: "vectorized",
      values: [globalProperties.vectorized.toString()],
      warn: false, // vectorized is never warned
    },
  ];

  const lastSampledHelpText = (
    <Fragment>
      If the time from the last sample is greater than 5 minutes, a new plan
      will be sampled. This frequency can be configured with the cluster setting{" "}
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
              {title}
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
              <div className={cx("plan-view-inner-container")}>
                <div className={cx("node-attributes", "global-attributes")}>
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
