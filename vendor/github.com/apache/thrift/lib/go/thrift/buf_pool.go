/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package thrift

import (
	"bytes"
	"sync"
)

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// getBufFromPool gets a buffer out of the pool and guarantees that it's reset
// before return.
func getBufFromPool() *bytes.Buffer {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

// returnBufToPool returns a buffer to the pool, and sets it to nil to avoid
// accidental usage after it's returned.
//
// You usually want to use it this way:
//
//     buf := getBufFromPool()
//     defer returnBufToPool(&buf)
//     // use buf
func returnBufToPool(buf **bytes.Buffer) {
	bufPool.Put(*buf)
	*buf = nil
}
