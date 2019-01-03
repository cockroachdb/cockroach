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
import { intersperse } from "src/util/intersperse";
import IExplainTreePlanNode = cockroach.sql.IExplainTreePlanNode;
import ExplainTreePlanNode_IAttr = cockroach.sql.ExplainTreePlanNode.IAttr;

export function PlanView(props: {
  title: string,
  plan: IExplainTreePlanNode,
}) {
  if (!props.plan) {
    return <div className="plan-view">
      <h3>{props.title}</h3>
      <p>No plan captured yet.</p>
    </div>;
  }
  return <div className="plan-view">
    <h3>{props.title}</h3>
    <div className="plan-node">
      <ul>
        <li>
          <PlanNode node={props.plan} />
        </li>
      </ul>
    </div>
  </div>;
}

interface PlanNodeProps {
  node: IExplainTreePlanNode;
}

function PlanNode(props: PlanNodeProps) {
  const node = props.node;
  const collapsedAttrs = collapseRepeatedAttrs(node.attrs);
  return (
    <div>
      <span className="plan-node__name">{node.name}</span>
      <span className="plan-node__attrs">
        {collapsedAttrs.map((attr) => (
          <span className="plan-node__attr" key={attr.key}>
            <span className="plan-node__attr-key">{attr.key}</span>
            <span className="plan-node__attr-eq">=</span>
            { renderAttrValueList(attr.value) }
          </span>
        ))}
      </span>
      { renderChildren(node.children) }
    </div>
  );
}

function renderChildren(children: cockroach.sql.IExplainTreePlanNode[]|null): React.ReactElement<PlanNodeProps> {
  if (!children) {
    return null;
  }
  return (
    <ul>
      {children.map((child, idx) => (
        <li>
          <PlanNode key={idx} node={child} />
        </li>
      ))}
    </ul>
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
  key: string;
  value: string[];
}

export function collapseRepeatedAttrs(attrs: ExplainTreePlanNode_IAttr[]): CollapsedPlanAttr[] {
  if (!attrs) {
    return [];
  }
  const collapsed: { [key: string]: CollapsedPlanAttr } = {};

  attrs.forEach((attr) => {
    if (attr.key === null || attr.value === null) {
      return;
    }
    const existingAttr = collapsed[attr.key];
    if (!existingAttr) {
      collapsed[attr.key] = {
        key: attr.key,
        value: [attr.value],
      };
      return;
    } else {
      collapsed[attr.key].value.push(attr.value);
    }
  });

  const collapsedAttrs = _.values(collapsed);
  return _.sortBy(collapsedAttrs, (ca) => (
    ca.key === "table" ? "table" : "z" + ca.key
  ));
}
