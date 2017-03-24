import * as React from "react";

/**
 * findChildrenOfType performs a DFS of the supplied React children collection,
 * returning all children which are ReactElements of the supplied type.
 */
export function findChildrenOfType<P>(children: React.ReactNode, type: string | React.ComponentClass<P> | React.SFC<P>): React.ReactElement<P>[] {
  let matchingChildren: React.ReactElement<P>[] = [];
  let childrenToSearch = React.Children.toArray(children);
  while (childrenToSearch.length > 0) {
    let child = childrenToSearch.shift();
    if (!isReactElement(child)) {
      continue;
    } else {
      if (child.type === type) {
        matchingChildren.push(child);
      }
      let { props } = child;
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
