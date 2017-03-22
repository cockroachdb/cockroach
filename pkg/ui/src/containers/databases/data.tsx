type TableDetailsResponseMessage = Proto2TypeScript.cockroach.server.serverpb.TableDetailsResponseMessage;
type TableStatsResponseMessage = Proto2TypeScript.cockroach.server.serverpb.TableStatsResponseMessage;
type Grant = Proto2TypeScript.cockroach.server.serverpb.TableDetailsResponse.Grant;

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
  public grants: Grant[];
  constructor(name: string, details: TableDetailsResponseMessage, stats: TableStatsResponseMessage) {
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
