/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/tsd.d.ts" />
/// <reference path="../generated/protos/wireproto.ts" />
///<reference path="../../typings/protobufjs/protobufjs.d.ts"/>

module Models {
  "use strict";
  export module SQLQuery {

    // TODO: eliminate 'any's
    export function runQuery(q: string): _mithril.MithrilPromise<any> {
      let root: any = Protos._root;

      let SQL: any = root.cockroach.sql.driver;

      let req: any = new SQL.Request({user: "root", sql: q});
      let buf: any = req.encode().toBuffer();

      let xhrConfig: any = function(xhr: any): any {
        xhr.setRequestHeader("Content-Type", "application/x-protobuf");
        xhr.setRequestHeader("Accept", "application/json");

        // TODO: receive data as protobuf instead of JSON
        // xhr.setRequestHeader("Accept", "application/x-protobuf");
        // xhr.responseType = "arraybuffer";
        return xhr;
      };

      return m.request({
        url: "/sql/Execute",
        config: xhrConfig,
        method: "POST",
        data: buf,
        serialize: function(data: any): any {
          // TODO: mithril PR to accept ArrayBuffer/Blob data
          data.constructor = FormData; // HACK: circumvent mithril's request datatype sanity check
          return data;
        },
        deserialize: function(data: any): any {
          // TODO: receive data as protobuf instead of JSON
          // let resp = new SQL.Response.decode(data, "binary");
          // return resp;
          return JSON.parse(data);
        },
      });
    }

  }
}
