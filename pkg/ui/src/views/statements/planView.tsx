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
import PlanNode$Properties = cockroach.sql.IExplainTreePlanNode;
import Attr$Properties = cockroach.sql.ExplainTreePlanNode.IAttr;

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
  key: string;
  value: string[];
}

function collapseRepeatedAttrs(attrs: Attr$Properties[]): CollapsedPlanAttr[] {
  const collapsed: { [key: string]: CollapsedPlanAttr } = {};

  attrs.forEach((attr) => {
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
