import * as protos from "../../js/protos";

type TableDetailsResponse = protos.cockroach.server.serverpb.TableDetailsResponse;
type TableStatsResponse = protos.cockroach.server.serverpb.TableStatsResponse;

// TableInfo is a supporting data structure which combines data about a single
// table that was obtained from multiple backend sources.
export class TableInfo {
  public name: string;
  public id: number;
  public numColumns: number;
  public numIndices: number;
  public size: number;
  public rangeCount: number;
  public createStatement: string;
  public grants: protos.cockroach.server.serverpb.TableDetailsResponse.Grant$Properties[];
  constructor(name: string, details: TableDetailsResponse, stats: TableStatsResponse) {
      this.name = name;
      this.id = details && details.descriptor_id.toNumber();
      this.numColumns = details && details.columns.length;
      this.numIndices = details && details.indexes.length;
      this.rangeCount = stats && stats.range_count && stats.range_count.toNumber();
      this.createStatement = details && details.create_table_statement;
      this.grants = details && details.grants;
      if (stats && stats.stats) {
          this.size = stats.stats.val_bytes.add(stats.stats.sys_bytes).toNumber();
      }
  }
}
