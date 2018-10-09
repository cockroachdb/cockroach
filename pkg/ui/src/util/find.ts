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

import React from "react";

/**
 * findChildrenOfType performs a DFS of the supplied React children collection,
 * returning all children which are ReactElements of the supplied type.
 */
export function findChildrenOfType<P>(children: React.ReactNode, type: string | React.ComponentClass<P> | React.SFC<P>): React.ReactElement<P>[] {
  const matchingChildren: React.ReactElement<P>[] = [];
  const childrenToSearch = React.Children.toArray(children);
  while (childrenToSearch.length > 0) {
    const child = childrenToSearch.shift();
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

/**
 * Predicate function to determine if a react child is a ReactElement.
 */
function isReactElement(child: React.ReactChild): child is React.ReactElement<any> {
  return (<React.ReactElement<any>>child).type !== undefined;
}
