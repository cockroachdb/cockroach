/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../models/proto.ts" />

module Models {
  "use strict";
  export module SQLQuery {
    import Request = Models.Proto.Request;
    import Response = Models.Proto.Response;
    import Column = Models.Proto.Column;
    import Rows = Models.Proto.Rows;
    import Row = Models.Proto.Row;

    interface ColumnType {
      name: string;
      type: string;
    }

    interface RowValue {
      key: string;
      value: string;
      type: string;
    }

    function parseResults(response: Response): Object[] {
      let rowResults: Rows = response.results[0].Union.Rows;

      let cols: ColumnType[] = _.transform(
        rowResults.columns,
        (result: ColumnType[], column: Column): void => {
          result.push({
            name: column.name,
            type: _.keys(column.typ.Payload)[0],
          });
        },
        []
      );

      return _.map(rowResults.rows, (row: Row): Object => {
        let rowVals: RowValue[] = _.map(cols, (col: ColumnType, idx: number): RowValue => {
          return {
            key: col.name,
            value: row.values[idx].Payload ? row.values[idx].Payload[col.type] : null,
            type: col.type,
          };
        });
        return _.transform(
          rowVals,
          (result: Object, rowValue: RowValue): void => {
            if (rowValue.value === null) {
              result[rowValue.key] = rowValue.value;
            } else if (rowValue.type === "BoolVal") {
              result[rowValue.key] = rowValue.value.toString().toLowerCase() === "true";
            } else if (rowValue.type === "TimeVal") {
              result[rowValue.key] = Utils.Convert.TimestampToMoment(rowValue.value).toString();
            } else if (rowValue.type === "DateVal") {
              result[rowValue.key] = moment.utc(0).add(rowValue.value, "days").format("YYYY-MM-DD");
            } else {
              result[rowValue.key] = rowValue.value;
            }
          },
          {});
      });
    }

    export function runQuery(q: string, parse?: boolean): _mithril.MithrilPromise<any> {

      let data: Request = {user: "root", sql: q};

      let xhrConfig = function(xhr: XMLHttpRequest): XMLHttpRequest {
        xhr.setRequestHeader("Content-Type", "application/x-protobuf");
        xhr.setRequestHeader("Accept", "application/json");

        xhr.timeout = 2000;
        /**
         * TODO: receive data as protobuf instead of JSON
         *
         * Mithril doesn't support sending/receiving binary data, but xhr
         * does. We just need the following lines to send/receive binary
         * data at the xhr level, but mithril would need to be modified to
         * support it, or we'd need a different ajax library.
         *
         *   xhr.setRequestHeader("Accept", "application/x-protobuf");
         *   xhr.responseType = "arraybuffer";
         *
         * Once this was enabled, we could use proto2typescript to generate
         * typescript definitions of protobufs.
         *
         */
        return xhr;
      };

      let request: _mithril.MithrilPromise<any> = m.request({
        url: "/sql/Execute",
        config: xhrConfig,
        method: "POST",
        data: data,
      });

      if (parse) {
        request = request.then(parseResults);
      }
      return request;
    }

  }
}
