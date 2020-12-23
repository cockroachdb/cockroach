// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

/**
 * A Layout is a 2d (row, column) array of LayoutCells, for rendering
 * a tree to the screen horizontally.
 *
 * E.g. the layout intended to be rendered as
 *
 *   |   a   |
 *   | b | c |
 *
 * Is represented as:
 *
 *    [ [             <LayoutCell for a>         ],
 *     [ <LayoutCell for b>, <LayoutCell for c> ] ]
 *
 */
export type Layout<T> = LayoutCell<T>[][];

export interface LayoutCell<T> {
  width: number;
  path: TreePath;
  isCollapsed: boolean;
  isPlaceholder: boolean;
  isLeaf: boolean;
  data: T;
}

/**
 * layoutTreeHorizontal turns a tree into a tabular, horizontal layout.
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
 * Where <P> is a LayoutCell with `isPlaceholder: true`.
 *
 * Further, if part of the tree is collapsed (specified by the `collapsedPaths` argument), its
 * LayoutCells are returned with `isCollapsed: true`, and placeholders are returned to maintain
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
 * Where <P> is a LayoutCell with `isPlaceholder: true` and e is a LayoutCell with
 * `isCollapsed: true`.
 *
 */
export function layoutTreeHorizontal<T>(
  root: TreeNode<T>,
  collapsedPaths: TreePath[],
): Layout<T> {
  const height = expandedHeight(root, collapsedPaths);
  return recur(root, []);

  function recur(node: TreeNode<T>, pathToThis: TreePath): Layout<T> {
    const heightUnderThis = height - pathToThis.length;

    const placeholdersLayout: Layout<T> = repeat(heightUnderThis, [
      {
        width: 1,
        path: pathToThis,
        data: node.data,
        isPlaceholder: true,
        isCollapsed: false,
        isLeaf: false,
      },
    ]);

    // Put placeholders above this cell if it's a leaf.
    if (isLeaf(node)) {
      return verticalConcatLayouts([
        placeholdersLayout,
        layoutFromCell({
          width: 1,
          path: pathToThis,
          data: node.data,
          isPlaceholder: false,
          isCollapsed: false,
          isLeaf: true,
        }),
      ]);
    }

    // Put placeholders below this if it's a collapsed internal node.
    const isCollapsed = deepIncludes(collapsedPaths, pathToThis);
    if (isCollapsed) {
      return verticalConcatLayouts([
        layoutFromCell({
          width: 1,
          path: pathToThis,
          data: node.data,
          isPlaceholder: false,
          isCollapsed: true,
          isLeaf: false,
        }),
        placeholdersLayout,
      ]);
    }

    const childLayouts = node.children.map((childNode) =>
      recur(childNode, [...pathToThis, childNode.name]),
    );

    const childrenLayout = horizontalConcatLayouts(childLayouts);

    const currentCell = {
      width: _.sumBy(childLayouts, (cl) => cl[0][0].width),
      data: node.data,
      path: pathToThis,
      isCollapsed,
      isPlaceholder: false,
      isLeaf: false,
    };

    return verticalConcatLayouts([layoutFromCell(currentCell), childrenLayout]);
  }
}

/**
 * horizontalConcatLayouts takes an array of layouts and returns
 * a new layout composed of its inputs laid out side by side.
 *
 * E.g.
 *
 *   horizontalConcatLayouts([ |   a   |  |   d   |
 *                             | b | c |, | e | f | ])
 *
 * yields
 *
 *   |   a   |   d   |
 *   | b | c | e | f |
 */
function horizontalConcatLayouts<T>(layouts: Layout<T>[]): Layout<T> {
  if (layouts.length === 0) {
    return [];
  }
  const output = _.range(layouts[0].length).map(() => []);

  _.forEach(layouts, (childLayout) => {
    _.forEach(childLayout, (row, rowIdx) => {
      _.forEach(row, (col) => {
        output[rowIdx].push(col);
      });
    });
  });

  return output;
}

/**
 * verticalConcatLayouts takes an array of layouts and returns
 * a new layout composed of its inputs laid out vertically.
 *
 * E.g.
 *
 *   verticalConcatLayouts([ |   a   |  |   d   |
 *                           | b | c |, | e | f | ])
 *
 * yields
 *
 *   |   a   |
 *   | b | c |
 *   |   d   |
 *   | e | f |
 */
function verticalConcatLayouts<T>(layouts: Layout<T>[]): Layout<T> {
  const output: Layout<T> = [];
  return _.concat(output, ...layouts);
}

function layoutFromCell<T>(cell: LayoutCell<T>): Layout<T> {
  return [[cell]];
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
  includeInternalNodes: boolean,
): FlattenedNode<T>[] {
  const output: FlattenedNode<T>[] = [];

  visitNodes(tree, (node: TreeNode<T>, pathSoFar: TreePath): boolean => {
    const depth = pathSoFar.length;

    if (isLeaf(node)) {
      output.push({
        depth,
        isLeaf: true,
        isCollapsed: false,
        data: node.data,
        path: pathSoFar,
      });
      return true;
    }

    const isExpanded = !deepIncludes(collapsedPaths, pathSoFar);
    const nodeBecomesLeaf = !includeInternalNodes && !isExpanded;
    if (includeInternalNodes || nodeBecomesLeaf) {
      output.push({
        depth,
        isLeaf: false,
        isCollapsed: !isExpanded,
        data: node.data,
        path: pathSoFar,
      });
    }

    // Continue the traversal if this node is expanded.
    return isExpanded;
  });

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
  const child = root.children.find((c) => c.name === pathSegment);
  if (child === undefined) {
    throw new Error(`not found: ${path}`);
  }
  return nodeAtPath(child, path.slice(1));
}

/**
 * visitNodes invokes `f` on each node in the tree in pre-order
 * (`f` is invoked on a node before being invoked on its children).
 *
 * If `f` returns false, the traversal stops. Otherwise, the traversal
 * continues.
 */
function visitNodes<T>(
  root: TreeNode<T>,
  f: (node: TreeNode<T>, path: TreePath) => boolean,
) {
  function recur(node: TreeNode<T>, path: TreePath) {
    const continueTraversal = f(node, path);
    if (!continueTraversal) {
      return;
    }
    if (node.children) {
      node.children.forEach((child) => {
        recur(child, [...path, child.name]);
      });
    }
  }
  recur(root, []);
}

/**
 * expandedHeight returns the height of the "uncollapsed" part of the tree,
 * i.e. the height of the tree where collapsed internal nodes count as leaf nodes.
 */
function expandedHeight<T>(
  root: TreeNode<T>,
  collapsedPaths: TreePath[],
): number {
  let maxHeight = 0;
  visitNodes(root, (_node, path) => {
    const depth = path.length;
    if (depth > maxHeight) {
      maxHeight = depth;
    }
    const nodeCollapsed = deepIncludes(collapsedPaths, path);
    return !nodeCollapsed; // Only continue the traversal if the node is expanded.
  });
  return maxHeight;
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
function getLeafPathsUnderPath<T>(
  root: TreeNode<T>,
  path: TreePath,
): TreePath[] {
  const atPath = nodeAtPath(root, path);
  const output: TreePath[] = [];
  visitNodes(atPath, (node, subPath) => {
    if (isLeaf(node)) {
      output.push([...path, ...subPath]);
    }
    return true;
  });
  return output;
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
function cartProd<A, B>(as: A[], bs: B[]): { a: A; b: B }[] {
  const output: { a: A; b: B }[] = [];
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
 * calling sumValuesUnderPath(rowTree, colTree, ['R_a'], ['C_1'], getValue)
 * sums up all the cells in the matrix, yielding 1 + 2 + 3 + 4 = 10.
 *
 * Calling sumValuesUnderPath(rowTree, colTree, ['R_a', 'R_b'], ['C_1'], getValue)
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

/**
 * repeat returns an array with the given element repeated `times`
 * times. Sadly, `_.repeat` only works for strings.
 */
function repeat<T>(times: number, item: T): T[] {
  const output: T[] = [];
  for (let i = 0; i < times; i++) {
    output.push(item);
  }
  return output;
}
