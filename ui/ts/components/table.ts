// source: components/table.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
// Author: Matt Tracy (matt@cockroachlabs.com)

module Components {
  "use strict";

  /**
   * Table component is used to display a set of comparable objects in a table
   * with each row representing the data of a single object from the set.
   */
  export module Table {
    import MithrilElement = _mithril.MithrilVirtualElement;

    /**
     * TableColumn describes a single column of a data table.
     */
    export interface TableColumn<T> {
      /**
       * title is the string that will display in the header row for this
       * column.
       */
      title: string;
      /**
       * data is a function which accepts the data for the row of the table, and
       * returns either a string or a mithril element to display in the table in
       * this column for the given row.
       */
      data: (row: T) => string|MithrilElement;
    }

    /**
     * TableData is the argument for creating a table component.
     */
    export interface TableData<T> {
      /**
       * columns is a function which returns a array of TableColumn objects
       * describing the columns of the table. Each TableColumn must accept the
       * same row data type as the TableData itself. 
       *
       * The columns in the table will be displayed in the same order as the
       * returned array.
       */
      columns: () => TableColumn<T>[];
      /* rows is a function that returns an array of row data for display in the
       * table. 
       *
       * Rows will be displayed in the same order as the returned array.
       */
      rows: () => T[];
    }

    class Controller<T> {
      data: TableData<T>;

      constructor(data: TableData<T>) {
        this.data = data;
      }

      /**
       * renderHeaders returns a mithril element which contains the header row
       * for the table.
       */
      renderHeaders(): MithrilElement {
        let cols = this.data.columns();
        let renderedCols = cols.map((col: TableColumn<T>) => m("th", col.title));
        return m("tr", renderedCols);
      }

      /**
       * renderRows returns a mithril element which contains the various column
       * rows for the table.
       */
      renderRows(): MithrilElement {
        let cols = this.data.columns();
        let rows = this.data.rows();
        let renderedRows = rows.map((row: T) => {
          let renderedCols = cols.map((col: TableColumn<T>) => m("td", col.data(row)));
          return m("tr", renderedCols);
        });
        return renderedRows;
      }
    }

    export function controller<T>(data: TableData<T>): Controller<T> {
      return new Controller(data);
    }

    export function view<T>(ctrl: Controller<T>): MithrilElement {
      return m("table", [
        ctrl.renderHeaders(),
        ctrl.renderRows(),
      ]);
    }

    export function create<T>(data: TableData<T>): _mithril.MithrilComponent<Controller<T>> {
      return m.component(Table, data);
    }
  }
}
