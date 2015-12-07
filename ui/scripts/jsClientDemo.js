#!/usr/bin/env node

/** jsClientDemo.js is meant to be used for debugging SQL requests and responses.
 * You can run it from the command line or using node.
 *
 *   $ node jsClientDemo.js
 *   $ ./jsClientDemo.js
 *
 * Once running, you can type in SQL commands and the client will execute them against the default local cockroach
 * instance.
 *
 *   > BEGIN TRANSACTION;
 *   ...
 *   > CREATE DATABASE JS;
 *   ...
 *   > SHOW DATABASES;
 *   ...
 *   > ROLLBACK;
 *   ...
 *   > SHOW DATABASES;
 *   ...
 *
 *   To exit, press ctrl-C
 */

var ProtoBuf = require("protobufjs");
var ByteBuffer = ProtoBuf.ByteBuffer;
var path = require("path");
var u = require("util");

var root = ProtoBuf.loadJsonFile(path.join(__dirname, "../generated/wireproto.json")).build(),
    SQL = root.cockroach.sql.driver;

var request = require("request");

var os = require("os");
var hostname = os.hostname();
var addr = "http://" + hostname + ":26257"; // current cockroach default

console.log("Using Cockroach node address " + addr);

process.stdout.write("> ");
// resume stdin to allow user input
process.stdin.resume();
process.stdin.setEncoding("utf8");

var session = null;

// This function is run every time the user passes a newline. It's the core of the client REPL.
process.stdin.on("data", function (text) {
  // pause stdin while we handle the input
  process.stdin.pause();

  var requestJSON = {user:"root", sql: text};

  if (session) {
    requestJSON.session = session;
  }

  console.log("---");
  console.log("JSON Request");
  console.log();
  console.log(u.inspect(requestJSON, {depth: null}));
  console.log();
  console.log("---");

  // This callback is passed to the json/protobuf requesters and handles the JSON response.
  function requestCallback(err, responseJSON) {
    if (err) {
      console.error("Error making request", err);
    } else if (!responseJSON) {
      console.error("No response JSON");
    } else {

      // Preserve the current session. Needed for transactions.
      if (responseJSON.session) {
        session = responseJSON.session;
      }

      console.log("JSON Response:");
      console.log();
      console.log(u.inspect(responseJSON, {depth: null}));
      console.log("---");
    }

    process.stdout.write('> ');
    // resume stdin to allow more user input
    process.stdin.resume();
  }

  //NOTE: switch the commented line to use binary/json respectively
  protobufRequester(requestJSON, requestCallback);
  //jsonRequester(requestJSON, requestCallback);

});

/**
 * protobufRequester transforms the JSON representation of the protobuf to binary, sends it to Cockroach,
 * and receives binary data which it transforms back into JSON. This is then passed to the callback.
 *
 * @param requestJSON {JSON} JSON representation of the protobuf, to be sent to the server
 * @param cb {function(err, response)} handles the response
 */
function protobufRequester(requestJSON, cb) {
  var requestProto = new SQL.Request(requestJSON);
  var requestBuffer = requestProto.encode().toBuffer();

  request({
    uri: addr + "/sql/Execute",
    headers: {
      "Content-Type": "application/x-protobuf",
      "Accept": "application/x-protobuf"
    },
    method: "POST",
    body: requestBuffer,
    encoding: null // required to prevent request from turning the binary protobuf into a string
  }, function (error, response, body) {
    var responseJSON = new SQL.Response.decode(body);
    cb(error, responseJSON);
  });
}

/**
 * jsonRequester stringifies the JSON representation of the protobuf, sends it to Cockroach,
 * and receives JSON data (as a string) which it parses and then passes to the callback.
 *
 * @param requestJSON {JSON} JSON representation of the protobuf, to be sent to the server
 * @param cb {function(err, response)} handles the response
 */
function jsonRequester(requestJSON, cb) {
  request({
    uri: addr + "/sql/Execute",
    headers: {
      "Content-Type": "application/json",
      "Accept": "application/json"
    },
    method: "POST",
    body: JSON.stringify(requestJSON)
  }, function (error, response, body) {
    cb(error, JSON.parse(body));
  });
}
