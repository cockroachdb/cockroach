// source: components/table.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../typings/lodash/lodash.d.ts" />
/// <reference path="../util/property.ts" />
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
       * view is a function which accepts a row of data for the table, and
       * returns either a string or a MithrilVirtualElement that should be
       * displayed inside of the column for this row.
       */
      view: (row: T) => string|MithrilElement;
      /**
       * Sortable determines if this column is sortable. Default is false.
       */
      sortable?: boolean;
      /**
       * sortValue is a function which accepts a row of data from the table, and
       * returns a value for this column of that row. This value should be
       * appropriate for sorting.
       *
       * This function should be specified when the view for a column is not
       * appropriate for sorting (for example, a complex mithril element). If
       * sortValue is not specified, but the column is sortable, then the value
       * of view() will be used to sort the column.
       */
      sortValue?: (r: T) => any;
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
      columns: Utils.ReadOnlyProperty<TableColumn<T>[]>;
      /* rows is a function that returns an array of row data for display in the
       * table. 
       */
      rows: Utils.ReadOnlyProperty<T[]>;
    }

    class Controller<T> {
      data: TableData<T>;
      sortedRows: Utils.ReadOnlyProperty<T[]>;
      private _sortColumn: Utils.Property<TableColumn<T>> = Utils.Prop(null);
      private _sortAscend: Utils.Property<boolean> = Utils.Prop(false);

      constructor(data: TableData<T>) {
        this.data = data;

        this.sortedRows = Utils.Computed(data.rows, this._sortColumn, this._sortAscend, (rows: T[], sortCol: TableColumn<T>, asc: boolean): T[] => {
          let result = _(rows);
          if (sortCol && sortCol.sortable) {
            // Sort the rows using the currently selected column, if the column
            // is sortable. Sort by the output of the column's sortValue()
            // method if specified, using the output of view() otherwise.
            if (sortCol.sortValue) {
              result = result.sortBy(sortCol.sortValue);
            } else {
              result = result.sortBy(sortCol.view);
            }

            if (asc) {
              result = result.reverse();
            }
          }
          return result.value();
        });
      };

      /**
       * SetSortColumn sets the column which is currently used for sorting
       * purposes. 
       *
       * When setting a new sort column, the sort direction is always ascending.
       * If the same column is set again, the sort direction is reversed to
       * ascending.
       */
      SetSortColumn(col: TableColumn<T>): void {
        if (!col.sortable) {
          return;
        }
        if (this._sortColumn() !== col) {
          this._sortColumn(col);
          this._sortAscend(false);
        } else {
          this._sortAscend(!this._sortAscend());
        }
      }

      /**
       * IsSortColumn returns true if the provided column is the column
       * currently used for sorting.
       */
      IsSortColumn(col: TableColumn<T>): boolean {
        return this._sortColumn() === col;
      }

      /**
       * RenderHeaders returns a mithril element which contains the header row
       * for the table.
       */
      RenderHeaders(): MithrilElement {
        let cols = this.data.columns();
        let sortClass = "sorted" + (this._sortAscend() ? " ascending" : "");
        let renderedCols = cols.map((col: TableColumn<T>) =>
          m("th",
            {
              onclick: (e: any): void => this.SetSortColumn(col),
              className: this.IsSortColumn(col) ? sortClass : ""
            },
            col.title));
        return m("tr", renderedCols);
      }

      /**
       * RenderRows returns a mithril element which contains the various column
       * rows for the table.
       */
      RenderRows(): MithrilElement {
        let cols = this.data.columns();
        let rows = this.sortedRows();
        let renderedRows = _.map(rows, (row: T) => {
          let renderedCols = cols.map((col: TableColumn<T>) =>
            m("td",
              {
                className: this.IsSortColumn(col) ? "sorted" : ""
              },
              col.view(row)));
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
        ctrl.RenderHeaders(),
        ctrl.RenderRows(),
      ]);
    }

    export function create<T>(data: TableData<T>): _mithril.MithrilComponent<Controller<T>> {
      return m.component(Table, data);
    }
  }
}
