// jsClientDemo.js is meant to be used for debugging SQL requests and responses

var ProtoBuf = require("protobufjs");
var ByteBuffer = require("ByteBuffer");
var path = require("path");
var u = require("util");

console.log("DIR", __dirname)

var root = ProtoBuf.loadJsonFile(path.join(__dirname, "../generated/wireproto.json")).build(),
    SQL = root.cockroach.sql.driver;

var request = require("request");

process.stdout.write('> ');
process.stdin.resume();
process.stdin.setEncoding("utf8");

process.stdin.on("data", function (text) {
  var req = new SQL.Request({user:"root", sql: text});

  console.log();
  console.log("JSON Request");
  console.log();
  console.log(u.inspect(req, {depth: null}));
  console.log();
  console.log("---");

  var buf = req.encode().toBuffer();

  process.stdin.pause();

  request({
    uri: "http://localhost:26257/sql/Execute",
    headers: {
      "Content-Type": "application/x-protobuf",
      //"Accept": "application/json"
      "Accept": "application/x-protobuf"
    },
    method: "POST",
    body: buf,
    encoding: null // required to prevent request from turning the binary protobuf into a string
  }, function (error, response, body) {
    var resp = new SQL.Response.decode(body);

    console.log("JSON Response:");
    console.log();
    console.log(u.inspect(resp, {depth: null}));
    console.log();

    process.stdout.write('> ');
    process.stdin.resume();
  });
});
