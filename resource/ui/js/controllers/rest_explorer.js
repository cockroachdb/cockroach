// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Andrew Bonventre (andybons@gmail.com)
/// <reference path="../typings/angularjs/angular.d.ts" />
var crApp = angular.module('cockroach');
crApp.controller('RestExplorerCtrl', ['$scope', '$http', function (scope, http) {
        scope.kvCounterVal = 0;
        scope.responseLog = [];
        scope.clearResponseLog = function (e) {
            scope.responseLog = [];
        };
        scope.requestPending = false;
        scope.handleClick = function (e) {
            e.preventDefault();
            var method = e.target.getAttribute('data-method');
            var endpoint = e.target.getAttribute('data-endpoint');
            if (endpoint == '/kv/rest/range') {
                endpoint += '?start=' + encodeURIComponent(scope.kvRangeStart);
                if (!!scope.kvRangeEnd) {
                    endpoint += '&end=' + encodeURIComponent(scope.kvRangeEnd);
                }
            }
            else if (!!scope.kvKey) {
                endpoint += scope.kvKey;
            }
            var data = '';
            if (!!scope.kvValue) {
                data = scope.kvValue;
            }
            var req = {
                method: method,
                url: endpoint,
                headers: {
                    'Content-Type': 'text/plain; charset=UTF-8'
                },
                data: data
            };
            if (endpoint.indexOf('/kv/rest/counter/') != -1) {
                req.headers = {
                    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
                };
                req.data = scope.kvCounterVal;
            }
            var responseFn = function (data, status, headers, config) {
                if (typeof data == 'object') {
                    data = JSON.stringify(data);
                }
                var response = data.length > 0 ? data : '(no response body)';
                var msg = ['[', method, '] ', status, ' ', endpoint, ': ', response].join('');
                scope.responseLog.push(msg);
            };
            http(req).success(responseFn).error(responseFn);
        };
    }]);
