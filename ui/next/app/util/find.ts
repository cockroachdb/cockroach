/// <reference path="../../typings/main.d.ts" />
import * as React from "react";

/**
 * findChildrenOfType performs a DFS of the supplied React children collection,
 * returning all children which are ReactElements of the supplied type.
 */
export function findChildrenOfType(children: React.ReactNode, type: any) {
  let matchingChildren: React.ReactElement<any>[] = [];
  let childrenToSearch = React.Children.toArray(children);
  while (childrenToSearch.length > 0) {
    let child = childrenToSearch.shift();
    if (!IsReactElement(child)) {
      continue;
    } else {
      if (child.type === type) {
        matchingChildren.push(child);
      }
      let { props } = child;
      if (props.children) {
        childrenToSearch = React.Children.toArray(props.children).concat(childrenToSearch);
      }
    }
  }

  return matchingChildren;
}

/**
 * Predicate function to determine if a react child is a ReactElement (as
 * opposed to a string or number).
 */
function IsReactElement(child: any): child is React.ReactElement<any> {
  return (child && child.type);
}
