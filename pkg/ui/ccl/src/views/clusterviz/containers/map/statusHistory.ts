// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

import * as protos from "src/js/protos";
import { NanoToMilli } from "src/util/convert";

type NodeStatus = protos.cockroach.server.status.NodeStatus$Properties;

export default class NodeStatusHistory {
    // "Client Activity" is a generic measurement present on each locality in
    // the current prototype design.
    // Currently, it is the to number SQL operations executed per second,
    // computed from the previous two node statuses.
    clientActivityRate: number;
    private statusHistory: NodeStatus[];
    private maxHistory = 2;

    constructor(initialStatus: NodeStatus) {
        this.statusHistory = [initialStatus];
        this.computeClientActivityRate();
    }

    update(nextStatus: NodeStatus) {
        if (this.statusHistory[0].updated_at.lessThan(nextStatus.updated_at)) {
            this.statusHistory.unshift(nextStatus);
            if (this.statusHistory.length > this.maxHistory) {
                this.statusHistory.pop();
            }

            this.computeClientActivityRate();
        }
    }

    id() {
        return this.statusHistory[0].desc.node_id;
    }

    latest() {
        return this.statusHistory[0];
    }

    private computeClientActivityRate() {
        this.clientActivityRate = 0;
        if (this.statusHistory.length > 1) {
            const [latest, prev] = this.statusHistory;
            const seconds = NanoToMilli(latest.updated_at.subtract(prev.updated_at).toNumber()) / 1000;
            const totalOps = (latest.metrics["sql.select.count"] - prev.metrics["sql.select.count"]) +
                (latest.metrics["sql.update.count"] - prev.metrics["sql.update.count"]) +
                (latest.metrics["sql.insert.count"] - prev.metrics["sql.insert.count"]) +
                (latest.metrics["sql.delete.count"] - prev.metrics["sql.delete.count"]);
            this.clientActivityRate = totalOps / seconds;
        }
    }
}
