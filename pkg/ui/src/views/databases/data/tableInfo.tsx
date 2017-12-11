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
  public physical_size: number;
  public mvcc_stats: protos.cockroach.storage.engine.enginepb.MVCCStats$Properties;
  public rangeCount: number;
  public createStatement: string;
  public grants: protos.cockroach.server.serverpb.TableDetailsResponse.Grant$Properties[];
  constructor(name: string, details: TableDetailsResponse, stats: TableStatsResponse) {
      this.name = name;
      this.id = details && details.descriptor_id.toNumber();
      this.numColumns = details && details.columns.length;
      this.numIndices = details && _.uniqBy(details.indexes, idx => idx.name).length;
      this.rangeCount = stats && stats.range_count && stats.range_count.toNumber();
      this.createStatement = details && details.create_table_statement;
      this.grants = details && details.grants;
      if (stats) {
          this.mvcc_stats = stats.stats;
          this.physical_size = FixLong(stats.approximate_disk_bytes).toNumber();
          if (!this.physical_size) {
            this.physical_size = Number(1024); // 1kb
          }
      }
  }
}
