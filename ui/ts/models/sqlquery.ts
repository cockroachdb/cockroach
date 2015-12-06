/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/tsd.d.ts" />
/// <reference path="../generated/protos/wireproto.d.ts" />
/// <reference path="../../typings/protobufjs/protobufjs.d.ts"/>

module Models {
  "use strict";
  export module SQLQuery {
    import RequestJSON = Proto2TypeScript.cockroach.sql.driver.RequestJSON;

    export function runQuery(q: string): _mithril.MithrilPromise<any> {

      let data: RequestJSON = {user: "root", sql: q};

      let xhrConfig = function(xhr: XMLHttpRequest): XMLHttpRequest {
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
        data: data,
      });
    }

  }
}
