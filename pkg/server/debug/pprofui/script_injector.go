// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License.

package pprofui

import (
	"bytes"
	"io"
	"net/http"
	"strings"
)

// scriptInjectingWriter is a http.ResponseWriter that buffers up
// everything written to it, and injects a script when the buffer is
// emptied via WriteReplaced. It's used to massage the links in the
// pprof web ui which unfortunately assume that the website is served
// up at "/".
//
// TODO(tschottdorf): fix this upstream by allowing a `-http` parameter
// of the form `-http :0/path/to/root`.
type scriptInjectingWriter struct {
	http.ResponseWriter
	buf bytes.Buffer
}

func (w *scriptInjectingWriter) Write(b []byte) (int, error) {
	return w.buf.Write(b)
}

const script = `
<script>
var idToPath = {
  "graphbtn": "",
  "topbtn": "top",
  "flamegraph": "flamegraph",
  "peek": "peek",
  "list": "source",
  "disasm": "disasm",
};

var base = window.location.pathname;
if (!base.endsWith("/")) {
  base += "/"
}

var ids = Object.keys(idToPath)
var toplevel = true;
ids.forEach(function(id) {
  if (idToPath[id] != "" && base.endsWith(idToPath[id] + "/")) {
    toplevel = false;
  }
})

if (!toplevel) {
  base += "../" // pretend that toplevel = true
}

var as = document.getElementsByTagName("a");
for (var i=0; i < as.length; i++) {
  var item = as[i];
  if (false && item.id == "") {
    continue;
  }
  var orig = item.href;
  var index = item.href.indexOf("?");
  var query = ""
  if (index >= 0) {
    query = item.href.substring(index, item.href.length);
  }
  path = idToPath[item.id];
  if (path) {
    item.href = base + path + query;
  } else {
    item.href = window.location.pathname + query;
  }
  console.log("rewrote" , item.id, "from", orig, "to", item.href);
};
</script>
`

func (w *scriptInjectingWriter) WriteReplaced(wr io.Writer) error {
	replaced := strings.Replace(w.buf.String(), "</body>", script+"\n</body>", -1)
	_, err := wr.Write([]byte(replaced))
	return err
}
