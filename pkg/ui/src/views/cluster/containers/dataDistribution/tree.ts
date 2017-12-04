import _ from "lodash";

export interface TreeNode<T> {
  name: string;
  children?: TreeNode<T>[];
  data?: T;
}

export type TreePath = string[];

export function isLeaf<T>(t: TreeNode<T>): boolean {
  return !_.has(t, "children");
}

export interface LayoutNode<T> {
  width: number;
  depth: number; // Depth of this subtree. Leaves have a depth of 1.
  path: TreePath;
  isCollapsed: boolean;
  isPlaceholder: boolean;
  data: T;
}

/**
 * layoutTree turns a tree into a tabular, horizontal layout.
 * For instance, the tree
 *
 *   a/
 *     b
 *     c
 *
 * becomes:
 *
 *   |   a   |
 *   | b | c |
 *
 * The layout is returned as a 2d array of LayoutNodes:
 *
 *   [ [             <LayoutNode for a>         ],
 *     [ <LayoutNode for b>, <LayoutNode for c> ] ]
 *
 * If the tree is of uneven depth, leaf nodes are pushed to the bottom and placeholder elements
 * are returned to maintain the rectangularity of the table.
 *
 * For instance, the tree
 *
 *   a/
 *     b/
 *       c
 *       d
 *     e
 *
 * becomes:
 *
 *   |      a      |
 *   |   b   | <P> |
 *   | c | d |  e  |
 *
 * Where <P> is a LayoutNode with `isPlaceholder: true`.
 *
 * Further, if part of the tree is collapsed (specified by the `collapsedPaths` argument), its
 * LayoutNodes are returned with `isCollapsed: true`, and placeholders are returned to maintain
 * rectangularity.
 *
 * The tree
 *
 *   a/
 *     b/
 *       c
 *       d
 *     e/
 *       f
 *       g
 *
 * without anything collapsed becomes:
 *
 *   |       a       |
 *   |   b   |   e   |
 *   | c | d | f | g |
 *
 * Collapsing `e` yields:
 *
 *   |      a      |
 *   |   b   |  e  |
 *   | c | d | <P> |
 *
 * Where <P> is a LayoutNode with `isPlaceholder: true` and e is a LayoutNode with
 * `isCollapsed: true`.
 *
 */
export function layoutTree<T>(root: TreeNode<T>, collapsedPaths: TreePath[]): LayoutNode<T>[][] {
  const depth = getDepth(root);
  function recur(node: TreeNode<T>, pathToThis: TreePath): LayoutNode<T>[][] {
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

    if (isLeaf(node)) {
      return [
        ...placeholderRows,
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
    const transposedChildLayouts = _.range(depth).map(() => ([]));

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
          data: node.data,
          path: pathToThis,
          isCollapsed,
          isPlaceholder: false,
        },
      ],
      ...transposedChildLayouts,
    ];
  }

  const recurRes = recur(root, []);
  return removePlaceholdersFromEnd(recurRes);
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
  isCollapsed: boolean;
  data: T;
  path: TreePath;
}

/**
 * flatten takes a tree and returns it as an array with depth information.
 *
 * E.g. the tree
 *
 *   a/
 *     b
 *     c
 *
 * Becomes (with includeNodes = true):
 *
 *   [
 *     a (depth: 0),
 *     b (depth: 1),
 *     c (depth: 1),
 *   ]
 *
 * Or (with includeNodes = false):
 *
 *   [
 *     b (depth: 1),
 *     c (depth: 1),
 *   ]
 *
 * Collapsed nodes (specified with the `collapsedPaths` argument)
 * are returned with `isCollapsed: true`; their children are not
 * returned.
 *
 * E.g. the tree
 *
 *   a/
 *     b/
 *       c
 *       d
 *     e/
 *       f
 *       g
 *
 * with b collapsed becomes:
 *
 *   [
 *     a (depth: 0),
 *     b (depth: 1, isCollapsed: true),
 *     e (depth: 1),
 *     f (depth: 2),
 *     g (depth: 2),
 *   ]
 *
 */
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
        isCollapsed: false,
        data: node.data,
        path: pathSoFar,
      });
    } else {
      const isExpanded = _.filter(collapsedPaths, (p) => _.isEqual(p, pathSoFar)).length === 0;
      if (includeNodes || !isExpanded) {
        output.push({
          depth,
          isLeaf: false,
          isCollapsed: !isExpanded,
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

/**
 * nodeAtPath returns the node found under `root` at `path`, throwing
 * an error if nothing is found.
 */
function nodeAtPath<T>(root: TreeNode<T>, path: TreePath): TreeNode<T> {
  if (path.length === 0) {
    return root;
  }
  const pathSegment = path[0];
  const child = root.children.find((c) => (c.name === pathSegment));
  if (child === undefined) {
    throw new Error(`not found: ${path}`);
  }
  return nodeAtPath(child, path.slice(1));
}

/**
 * visitNodes invokes `f` on each node in the tree in pre-order
 * (`f` is invoked on a node before being invoked on its children).
 */
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

/**
 * getLeafPathsUnderPath returns paths to all leaf nodes under the given
 * `path` in `root`.
 *
 * E.g. for the tree T =
 *
 *   a/
 *     b/
 *       c
 *       d
 *     e/
 *       f
 *       g
 *
 * getLeafPaths(T, ['a', 'b']) yields:
 *
 *   [ ['a', 'b', 'c'],
 *     ['a', 'b', 'd'] ]
 *
 */
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

/**
 * getLeafPaths returns paths to all leaves under `root`.
 */
function getLeafPaths<T>(root: TreeNode<T>): TreePath[] {
  return getLeafPathsUnderPath(root, []);
}

/**
 * cartProd returns all combinations of elements in `as` and `bs`.
 *
 * e.g. cartProd([1, 2], ['a', 'b'])
 * yields:
 * [
 *   {a: 1, b: 'a'},
 *   {a: 1, b: 'b'},
 *   {a: 2, b: 'a'},
 *   {a: 2, b: 'b'},
 * ]
 */
function cartProd<A, B>(as: A[], bs: B[]): {a: A, b: B}[] {
  const output: {a: A, b: B}[] = [];
  as.forEach((a) => {
    bs.forEach((b) => {
      output.push({ a, b });
    });
  });
  return output;
}

/**
 * sumValuesUnderPaths returns the sum of `getValue(R, C)`
 * for all leaf paths R under `rowPath` in `rowTree`,
 * and all leaf paths C under `colPath` in `rowTree`.
 *
 * E.g. in the matrix
 *
 *  |       |    C_1    |
 *  |       | C_2 | C_3 |
 *  |-------|-----|-----|
 *  | R_a   |     |     |
 *  |   R_b |  1  |  2  |
 *  |   R_c |  3  |  4  |
 *
 * represented by
 *
 *   rowTree = (R_a [R_b R_c])
 *   colTree = (C_1 [C_2 C_3])
 *
 * calling sumValuesUnderPath(rowTree, colTree, ['R_a'], ['C_b'], getValue)
 * sums up all the cells in the matrix, yielding
 * yielding 1 + 2 + 3 + 4 = 10.
 *
 * Calling sumValuesUnderPath(rowTree, colTree, ['R_a', 'R_b'], ['C_b'], getValue)
 * sums up only the cells under R_b,
 * yielding 1 + 2 = 3.
 *
 */
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

/**
 * deepIncludes returns true if `array` contains `val`, doing
 * a deep equality comparison.
 */
export function deepIncludes<T>(array: T[], val: T): boolean {
  return _.some(array, (v) => _.isEqual(val, v));
}
