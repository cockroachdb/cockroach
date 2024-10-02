// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

/**
 * Predicate function to determine if a react child is a ReactElement.
 */
function isReactElement(child: React.ReactNode): child is React.ReactElement {
  return (child as React.ReactElement).type !== undefined;
}

/**
 * findChildrenOfType performs a DFS of the supplied React children collection,
 * returning all children which are ReactElements of the supplied type.
 */
export function findChildrenOfType<P>(
  children: React.ReactNode,
  type: string | React.ComponentClass<P> | React.FC<P>,
): React.ReactElement<P>[] {
  const matchingChildren: React.ReactElement<P>[] = [];
  const childrenToSearch = React.Children.toArray(children);
  while (childrenToSearch.length > 0) {
    const child: React.ReactNode = childrenToSearch.shift();
    if (!isReactElement(child)) {
      continue;
    } else {
      if (child.type === type) {
        matchingChildren.push(child);
      }
      const { props } = child;
      if (props.children) {
        // Children added to front of search array for DFS.
        childrenToSearch.unshift(...React.Children.toArray(props.children));
      }
    }
  }

  return matchingChildren;
}
