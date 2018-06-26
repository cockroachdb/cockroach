import _ from "lodash";
import {AssocList, getAssocList} from "oss/src/views/cluster/containers/dataDistribution/assocList";

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
 *      [ <LayoutCell for b>, <LayoutCell for c> ] ]
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
export function layoutTreeHorizontal<T>(root: TreeNode<T>, collapsedPaths: TreePath[]): Layout<T> {
  const height = expandedHeight(root, collapsedPaths);
  return recur(root, []);

  function recur(node: TreeNode<T>, pathToThis: TreePath): Layout<T> {
    const heightUnderThis = height - pathToThis.length;

    const placeholdersLayout: Layout<T> = repeat(heightUnderThis, [{
      width: 1,
      path: pathToThis,
      data: node.data,
      isPlaceholder: true,
      isCollapsed: false,
      isLeaf: false,
    }]);

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

    const childLayouts = node.children.map((childNode) => (
      recur(childNode, [...pathToThis, childNode.name])
    ));

    const childrenLayout = horizontalConcatLayouts(childLayouts);

    const currentCell = {
      width: _.sumBy(childLayouts, (cl) => cl[0][0].width),
      data: node.data,
      path: pathToThis,
      isCollapsed,
      isPlaceholder: false,
      isLeaf: false,
    };

    return verticalConcatLayouts([
      layoutFromCell(currentCell),
      childrenLayout,
    ]);
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
  const output = _.range(layouts[0].length).map(() => ([]));

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
  return [
    [cell],
  ];
}

export interface FlattenedNode<T> {
  depth: number;
  isLeaf: boolean;
  isCollapsed: boolean;
  node: TreeNode<T>;
  path: TreePath;
  isPaginated: boolean;
  masterIdx: number;
}

// TODO(vilterp): this is defined somewhere else... Sortable table?
export enum SortState {
  ASC = "ASC",
  DESC = "DESC",
  NONE = "NONE",
}

export interface PaginationState {
  path: TreePath;
  page: number;
  sortState: SortState;
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
  tree: TreeWithSize<T>,
  collapsedPaths: TreePath[],
  includeInternalNodes: boolean,
  paginationStates: AssocList<TreePath, PaginationState> = [],
  pageSize: number = Number.MAX_VALUE,
  sortBy?: (masterIndex: number) => number,
): FlattenedNode<T>[] {
  const output: FlattenedNode<T>[] = [];
  recur(tree, [], 0);

  function recur(node: TreeWithSize<T>, pathSoFar: TreePath, masterIdx: number): number {
    const depth = pathSoFar.length;

    if (isLeaf(node.node)) {
      output.push({
        depth,
        isLeaf: true,
        isCollapsed: false,
        node: node.node,
        path: pathSoFar,
        isPaginated: false,
        masterIdx,
      });
      return 1;
    }

    let increase = 0;

    const isExpanded = !deepIncludes(collapsedPaths, pathSoFar);
    const nodeBecomesLeaf = !includeInternalNodes && !isExpanded;
    if (includeInternalNodes || nodeBecomesLeaf) {
      output.push({
        depth,
        isLeaf: false,
        isCollapsed: !isExpanded,
        node: node.node,
        path: pathSoFar,
        isPaginated: (node.children || []).length > pageSize,
        masterIdx,
      });
    }
    increase++;

    // TODO: we can't be traversing the entire tree (including collapsed subtrees) to get indices here
    // need to cache the size of each subtree or something
    if (node.children) {
      if (isExpanded) {
        const paginationState = getAssocList(paginationStates, pathSoFar);
        const page = paginationState
          ? paginationState.page
          : 0;
        const offset = page * pageSize;

        const sortState = paginationState ? paginationState.sortState : SortState.NONE;
        const sortedChildren = sortChildren(node.children, sortState, sortBy);

        for (let i = 0; i < offset; i++) {
          const child = sortedChildren[i];
          increase += child.size;
        }

        for (let i = offset; i < Math.min(sortedChildren.length, offset + pageSize); i++) {
          const child = sortedChildren[i];
          increase += recur(child, [...pathSoFar, child.node.name], masterIdx + increase);
        }
      } else {
        increase += node.size - 1; // -1 since we already added the node itself
      }
    }

    return increase;
  }

  return output;
}

function sortChildren<T>(
  children: TreeWithSize<T>[],
  sortState: SortState,
  sortBy?: (masterIndex: number) => number,
): TreeWithSize<T>[] {
  if (sortState === SortState.NONE) {
    return children;
  }
  if (!sortBy) {
    throw Error(`sortState ${sortState} but no sortBy provided`);
  }
  const sortedChildren = _.sortBy(children, (child) => {
    return sortBy(child.masterIdx);
  });
  if (sortState === SortState.DESC) {
    sortedChildren.reverse();
  }
  return sortedChildren;
}

/**
 * visitNodes invokes `f` on each node in the tree in pre-order
 * (`f` is invoked on a node before being invoked on its children).
 *
 * If `f` returns false, the traversal stops. Otherwise, the traversal
 * continues.
 */
export function visitNodes<T>(
  root: TreeNode<T>,
  f: (node: TreeNode<T>, path: TreePath, childIdx: number) => boolean,
) {
  function recur(node: TreeNode<T>, path: TreePath, childIdx: number) {
    const continueTraversal = f(node, path, childIdx);
    if (!continueTraversal) {
      return;
    }
    if (node.children) {
      for (let i = 0; i < node.children.length; i++) {
        const child = node.children[i];
        recur(child, [...path, child.name], i);
      }
    }
  }
  recur(root, [], 0);
}

/**
 * expandedHeight returns the height of the "uncollapsed" part of the tree,
 * i.e. the height of the tree where collapsed internal nodes count as leaf nodes.
 */
function expandedHeight<T>(root: TreeNode<T>, collapsedPaths: TreePath[]): number {
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
export function repeat<T>(times: number, item: T): T[] {
  const output: T[] = [];
  for (let i = 0; i < times; i++) {
    output.push(item);
  }
  return output;
}

export interface TreeWithSize<T> {
  size: number;
  masterIdx: number;
  node: TreeNode<T>;
  children?: TreeWithSize<T>[];
}

// TODO(vilterp): not store the child arrays twice...
// maybe actually just add teh size to teh same struct
export function augmentWithSize<T>(root: TreeNode<T>): TreeWithSize<T> {
  function recur(node: TreeNode<T>, masterIdx: number): TreeWithSize<T> {
    if (isLeaf(node)) {
      return {
        size: 1,
        masterIdx,
        node,
      };
    }

    let size = 0;
    const children: TreeWithSize<T>[] = [];
    node.children.forEach((child) => {
      const augmentedChild = recur(child, masterIdx + size);
      size += augmentedChild.size;
      children.push(augmentedChild);
    });
    return {
      size: size + 1, // add 1 for the node itself
      children,
      node,
      masterIdx,
    };
  }

  return recur(root, 0);
}
