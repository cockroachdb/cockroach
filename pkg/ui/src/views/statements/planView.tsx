import _ from "lodash";
import React from "react";

import { cockroach } from "src/js/protos";
import { intersperse } from "src/util/intersperse";
import PlanNode$Properties = cockroach.sql.IPlanNode;
import Attr$Properties = cockroach.sql.PlanNode.IAttr;

export function PlanView(props: { plan: PlanNode$Properties }) {
  return (
    <div className="plan-view">
      <ul>
        <li>
          <PlanNode node={props.plan} />
        </li>
      </ul>
    </div>
  );
}

interface PlanNodeProps {
  node: PlanNode$Properties;
}

function PlanNode(props: PlanNodeProps): React.ReactElement<PlanNodeProps> {
  const node = props.node;
  node.children = node.children || [];
  node.attrs = node.attrs || [];
  const collapsedAttrs = collapseRepeatedAttrs(node.attrs);
  return (
    <div className="plan-node">
      <span className="plan-node__name">{node.name}</span>
      <span className="plan-node__attrs">
        {collapsedAttrs.map((attr) => (
          <span className="plan-node__attr" key={attr.key}>
            <span className="plan-node__attr-key">{attr.key}</span>
            <span className="plan-node__attr-eq">=</span>
            {typeof attr.value === "string"
              ? <span className="plan-node__attr-value">{attr.value}</span>
              : renderAttrValueList(attr.value as string[])}
          </span>
        ))}
      </span>
      <ul>
        {node.children.map((child, idx) => (
          <li>
            <PlanNode key={idx} node={child} />
          </li>
        ))}
      </ul>
    </div>
  );
}

function renderAttrValueList(values: string[]) {
  return (
    <span className="plan-node__attr-value-list">
      [
      {intersperse(
        values.map((value, idx) => (
          <span className="plan-node__attr-value" key={idx}>{value}</span>
        )),
        <span>, </span>,
      )}
      ]
    </span>
  );
}

interface CollapsedPlanAttr {
  // TODO(vilterp): figure out a way to make these not optional and appease the typechecker
  key?: string;
  value?: string | string[];
}

function collapseRepeatedAttrs(attrs: Attr$Properties[]): CollapsedPlanAttr[] {
  const collapsed: { [key: string]: CollapsedPlanAttr } = {};

  attrs.forEach((attr) => {
    const existingAttr = collapsed[attr.key];
    if (!existingAttr) {
      collapsed[attr.key] = attr;
      return;
    }
    if (typeof existingAttr.value === "string") {
      collapsed[attr.key] = {
        key: attr.key,
        value: [existingAttr.value, attr.value],
      };
      return;
    }
    // TODO(vilterp): type switch?
    (collapsed[attr.key].value as string[]).push(attr.value);
  });

  const collapsedAttrs = _.values(collapsed);
  return _.sortBy(collapsedAttrs, (ca) => (
    ca.key === "table" ? "table" : "z" + ca.key
  ));
}
