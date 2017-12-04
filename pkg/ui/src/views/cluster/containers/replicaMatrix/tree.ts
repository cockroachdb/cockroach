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

interface LayoutNode<T> {
  width: number;
  depth: number;
  path: TreePath;
  isCollapsed: boolean;
  isPlaceholder: boolean;
  data: T;
}

export function layoutTree<T>(root: TreeNode<T>, collapsedPaths: TreePath[]): LayoutNode<T>[][] {
  const depth = getDepth(root);
  function recur(node: TreeNode<T>, pathToThis: TreePath): LayoutNode<T>[][] {
    if (isLeaf(node)) {
      return [
        [
          {
            width: 1,
            depth: 1,
            path: pathToThis,
            data: node.data,
            isCollapsed: false,
            isPlaceholder: false,
          },
        ],
      ];
    }

    const isCollapsed = deepIncludes(collapsedPaths, pathToThis);
    if (isCollapsed) {
      // debugger;
      const depthUnderThis = depth - pathToThis.length;
      const placeholderRows = _.range(depthUnderThis).reverse().map((thisDepth) => (
        [
          {
            width: 1,
            depth: thisDepth + 1,
            isPlaceholder: true,
            isCollapsed: false,
            path: pathToThis,
            data: node.data,
          },
        ]
      ));
      return [
        [
          {
            width: 1,
            depth: depthUnderThis + 1,
            path: pathToThis,
            data: node.data,
            isCollapsed: true,
            isPlaceholder: false,
          },
        ],
        ...placeholderRows,
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
          isCollapsed,
          isPlaceholder: false,
        },
      ],
      ...transposedChildLayouts,
    ];
  }

  return removePlaceholdersFromEnd(recur(root, []));
}

function removePlaceholdersFromEnd<T>(arr: LayoutNode<T>[][]): LayoutNode<T>[][] {
  const output: LayoutNode<T>[][] = [];
  for (let i = 0; i < arr.length; i++) {
    const row = arr[i];
    if (row.every((cell) => cell.isPlaceholder)) {
      return output;
    }
    output.push(row);
  }
  return output;
}

export interface FlattenedNode<T> {
  depth: number;
  isLeaf: boolean;
  data: T;
  path: TreePath;
}

export function flatten<T>(
  tree: TreeNode<T>,
  collapsedPaths: TreePath[],
  includeNodes: boolean,
): FlattenedNode<T>[] {
  const output: FlattenedNode<T>[] = [];
  function recur(node: TreeNode<T>, depth: number, pathSoFar: TreePath) {
    if (isLeaf(node)) {
      output.push({
        depth,
        isLeaf: true,
        data: node.data,
        path: pathSoFar,
      });
    } else {
      const isExpanded = _.filter(collapsedPaths, (p) => _.isEqual(p, pathSoFar)).length === 0;
      if (includeNodes || !isExpanded) {
        output.push({
          depth,
          isLeaf: false,
          data: node.data,
          path: pathSoFar,
        });
      }
      if (isExpanded) {
        node.children.forEach((child) => {
          recur(child, depth + 1, [...pathSoFar, child.name]);
        });
      }
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

// throws an error if not found
function nodeAtPath<T>(node: TreeNode<T>, path: TreePath): TreeNode<T> {
  if (path.length === 0) {
    return node;
  }
  const pathSegment = path[0];
  const child = node.children.find((c) => (c.name === pathSegment));
  if (child === undefined) {
    throw new Error(`not found: ${path}`);
  }
  return nodeAtPath(child, path.slice(1));
}

function visitNodes<T>(root: TreeNode<T>, f: (node: TreeNode<T>, path: TreePath) => void) {
  function recur(node: TreeNode<T>, path: TreePath) {
    f(node, path);
    if (node.children) {
      node.children.forEach((child) => {
        recur(child, [...path, child.name]);
      });
    }
  }
  recur(root, []);
}

function getDepth<T>(root: TreeNode<T>): number {
  return _.max(getLeafPaths(root).map((p) => p.length));
}

function getLeafPathsUnderPath<T>(root: TreeNode<T>, path: TreePath): TreePath[] {
  const atPath = nodeAtPath(root, path);
  const output: TreePath[] = [];
  visitNodes(atPath, (node, subPath) => {
    if (isLeaf(node)) {
      output.push([...path, ...subPath]);
    }
  });
  return output;
}

export function getLeafPaths<T>(root: TreeNode<T>): TreePath[] {
  return getLeafPathsUnderPath(root, []);
}

function cartProd<A, B>(as: A[], bs: B[]): {a: A, b: B}[] {
  const output: {a: A, b: B}[] = [];
  as.forEach((a) => {
    bs.forEach((b) => {
      output.push({ a, b });
    });
  });
  return output;
}

export function sumValuesUnderPaths<R, C>(
  rowTree: TreeNode<R>,
  colTree: TreeNode<C>,
  rowPath: TreePath,
  colPath: TreePath,
  getValue: (row: TreePath, col: TreePath) => number,
): number {
  const rowPaths = getLeafPathsUnderPath(rowTree, rowPath);
  const colPaths = getLeafPathsUnderPath(colTree, colPath);
  const prod = cartProd(rowPaths, colPaths);
  let sum = 0;
  prod.forEach((coords) => {
    sum += getValue(coords.a, coords.b);
  });
  return sum;
}

// utilities that should be in lodash...

export function deepIncludes<T>(array: T[], val: T): boolean {
  return _.some(array, (v) => _.isEqual(val, v));
}

export function hasPrefix<T>(array: T[], prefix: T[]): boolean {
  if (prefix.length > array.length) {
    return false;
  }
  for (let i = 0; i < prefix.length; i++) {
    if (prefix[i] !== array[i]) {
      return false;
    }
  }
  return true;
}

function repeat<T>(v: T, n: number): T[] {
  const output = [];
  for (let i = 0; i < n; i++) {
    output.push(v);
  }
  return output;
}
