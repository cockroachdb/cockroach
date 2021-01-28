// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as protos from "src/js/protos";
import { FixLong } from "src/util/fixLong";

import _ from "lodash";

type TableDetailsResponse = protos.cockroach.server.serverpb.TableDetailsResponse;
type TableStatsResponse = protos.cockroach.server.serverpb.TableStatsResponse;

// TableInfo is a supporting data structure which combines data about a single
// table that was obtained from multiple backend sources.
export class TableInfo {
  public name: string;
  public id: number;
  public numColumns: number;
  public numIndices: number;
  public physicalSize: number;
  public mvccSize: protos.cockroach.storage.enginepb.IMVCCStats;
  public rangeCount: number;
  public createStatement: string;
  public configureZoneStatement: string;
  public grants: protos.cockroach.server.serverpb.TableDetailsResponse.IGrant[];
  public numReplicas: number;
  constructor(
    name: string,
    details: TableDetailsResponse,
    stats: TableStatsResponse,
  ) {
    this.name = name;
    this.id = details && details.descriptor_id.toNumber();
    this.numColumns = details && details.columns.length;
    this.numIndices =
      details && _.uniqBy(details.indexes, (idx) => idx.name).length;
    this.rangeCount =
      stats && stats.range_count && stats.range_count.toNumber();
    this.createStatement = details && details.create_table_statement;
    this.configureZoneStatement = details && details.configure_zone_statement;
    this.grants = details && details.grants;
    this.numReplicas =
      details && details.zone_config && details.zone_config.num_replicas;
    if (stats) {
      this.mvccSize = stats.stats;
      this.physicalSize = FixLong(stats.approximate_disk_bytes).toNumber();
    }
  }

  public detailsAndStatsLoaded(): boolean {
    return this.id !== undefined && this.physicalSize !== undefined;
  }
}
