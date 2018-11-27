// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
  public mvccSize: protos.cockroach.storage.engine.enginepb.IMVCCStats;
  public rangeCount: number;
  public createStatement: string;
  public grants: protos.cockroach.server.serverpb.TableDetailsResponse.IGrant[];
  constructor(name: string, details: TableDetailsResponse, stats: TableStatsResponse) {
      this.name = name;
      this.id = details && details.descriptor_id.toNumber();
      this.numColumns = details && details.columns.length;
      this.numIndices = details && _.uniqBy(details.indexes, idx => idx.name).length;
      this.rangeCount = stats && stats.range_count && stats.range_count.toNumber();
      this.createStatement = details && details.create_table_statement;
      this.grants = details && details.grants;
      if (stats) {
          this.mvccSize = stats.stats;
          this.physicalSize = FixLong(stats.approximate_disk_bytes).toNumber();
      }
  }
}
