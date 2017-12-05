import _ from "lodash";

export interface TreeNode<T> {
  name: string;
  children?: TreeNode<T>[];
  data: T;
}

export type TreePath = string[];

export function isLeaf<T>(t: TreeNode<T>): boolean {
  return !_.has(t, "children");
}

export function getLeaves<T>(tree: TreeNode<T>) {
  const output: TreeNode<T>[] = [];
  let maxDepth = 0;
  function recur(node: TreeNode<T>, depth: number) {
    if (isLeaf(node)) {
      output.push(node);
      return;
    }
    if (depth > maxDepth) {
      maxDepth = depth;
    }
    for (const child of node.children) {
      recur(child, 0);
    }
  }
  recur(tree, 0);
  return output;
}

interface LayoutNode<T> {
  width: number;
  depth: number;
  path: TreePath;
  data: T;
}

export function layoutTree<T>(tree: TreeNode<T>): LayoutNode<T>[][] {
  function recur(node: TreeNode<T>, pathToThis: TreePath): LayoutNode<T>[][] {
    if (!node.children) {
      return [
        [
          {
            width: 1,
            depth: 1,
            path: pathToThis,
            data: node.data,
          },
        ],
      ];
    }

    const childLayouts = node.children.map((childNode) => (
      recur(childNode, [...pathToThis, childNode.name])
    ));
    const maxDepth = _.maxBy(childLayouts, (cl) => cl[0][0].depth)[0][0].depth;
    const transposedChildLayouts = _.range(maxDepth).map(() => ([]));

    _.forEach(childLayouts, (childLayout) => {
      _.forEach(childLayout, (row, rowIdx) => {
        _.forEach(row, (col) => {
          transposedChildLayouts[rowIdx].push(col);
        });
      });
    });

    return [
      [
        {
          width: _.sumBy(childLayouts, (cl) => cl[0][0].width),
          depth: maxDepth + 1,
          name: node.name,
          path: pathToThis,
        },
      ],
      ...transposedChildLayouts,
    ];
  }

  return recur(tree, []);
}

export interface FlattenedNode<T> {
  depth: number;
  isLeaf: boolean;
  data: T;
  path: TreePath;
}

export function flatten<T>(tree: TreeNode<T>, collapsedPaths: TreePath[]): FlattenedNode<T>[] {
  const output: FlattenedNode<T>[] = [];
  function recur(node: TreeNode<T>, depth: number, pathSoFar: TreePath) {
    const pathToThis = [...pathSoFar, node.name];
    output.push({
      depth,
      isLeaf: isLeaf(node),
      data: node.data,
      path: pathToThis,
    });
    const isExpanded = _.filter(collapsedPaths, (p) => _.isEqual(p, pathToThis)).length === 0;
    if (!isLeaf(node) && isExpanded) {
      node.children.forEach((child) => {
        recur(child, depth + 1, pathToThis);
      });
    }
  }
  recur(tree, 0, []);
  return output;
}

// mutates `tree`.
export function setAtPath<T>(tree: TreeNode<T>, path: TreePath, node: TreeNode<T>) {
  if (path.length === 0) {
    tree.children.push(node);
    return;
  }
  const nextPathSegment = path[0];
  let nextChild = _.find(tree.children, (child) => _.isEqual(child.name, nextPathSegment));
  if (!nextChild) {
    nextChild = {
      name: nextPathSegment,
      children: [],
      data: null,
    };
    tree.children.push(nextChild);
  }
  setAtPath(nextChild, path.slice(1), node);
}
