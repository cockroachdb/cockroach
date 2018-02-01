// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import _ from "lodash";
import * as protos from "src/js/protos";
import * as vector from "src/util/vector";

import { SimulatedNodeStatus } from "./nodeSimulator";

type Tier = protos.cockroach.roachpb.Tier$Properties;

function localityKey(tiers: Tier[]) {
  return _.chain(tiers)
    .map(t => t.key + "=" + t.value)
    .join(",")
    .value();
}

class Locality {
  key: string;
  constructor(public tiers: Tier[]) {
    this.key = localityKey(tiers);
  }
}

export class LocalityTreeNode {
  children: LocalityTreeNode[];
  x?: number;
  y?: number;
  dx?: number;
  dy?: number;

  constructor(public data: SimulatedNodeStatus | Locality | "root") {
    this.data = data;
    this.children = [];
  }

  isNode() {
    return this.data instanceof SimulatedNodeStatus;
  }

  isLocality() {
    return this.data instanceof Locality;
  }

  longLat(): [number, number] {
    if (this.isNode()) {
      return (this.data as SimulatedNodeStatus).longLat();
    }

    let centroid: [number, number] = [0, 0];
    this.children.forEach(c => centroid = vector.add(centroid, c.longLat()));
    return vector.mult(centroid, 1 / this.children.length);
  }

  descendants(): LocalityTreeNode[] {
    let descendants = this.children || [];
    if (this.children) {
      this.children.forEach(c => descendants = descendants.concat(c.descendants()));
    }
    return descendants;
  }
}

// LocalityCollection is used to generate the tree of localities described
// by the nodes added to the collections. Localties are not explicitly defined
// in any place apart from nodes; their existence is rather implied from the
// locality tiers present on each node.  Adding a node to this collection will
// add the localities described by that node if they were not already created
// by a previously added node.
export class LocalityCollection {
  tree = new LocalityTreeNode("root");
  byKey: {[key: string]: LocalityTreeNode} = {"root": this.tree};

  // Adds a new node to the collection. This will also add any localties
  // described by the node that are not already present in the collection.
  addNode(node: SimulatedNodeStatus) {
    const locality = this.getOrCreateTreeNode(node.tiers());
    locality.children.push(new LocalityTreeNode(node));
  }

  private getOrCreateTreeNode(tiers: Tier[]): LocalityTreeNode {
    const key = tiers.length > 0 ? localityKey(tiers) : "root";
    let treeNode = this.byKey[key];
    if (treeNode) {
      return treeNode;
    }

    // Create node, add to byKey and to parent.
    treeNode = new LocalityTreeNode(new Locality(tiers));
    this.byKey[key] = treeNode;
    const parent = this.getOrCreateTreeNode(tiers.slice(0, -1));
    parent.children.push(treeNode);
    return treeNode;
  }
}
