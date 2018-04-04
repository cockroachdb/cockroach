### ANYELEMENT Functions

<table>
<thead><tr><th>Function &rarr; Returns</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>to_json(val: anyelement) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Returns the value as JSON or JSONB.</p>
</span></td></tr>
<tr><td><code>to_jsonb(val: anyelement) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Returns the value as JSON or JSONB.</p>
</span></td></tr></tbody>
</table>

### Array Functions

<table>
<thead><tr><th>Function &rarr; Returns</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>array_append(array: <a href="bool.html">bool</a>[], elem: <a href="bool.html">bool</a>) &rarr; <a href="bool.html">bool</a>[]</code></td><td><span class="funcdesc"><p>Appends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_append(array: <a href="bytes.html">bytes</a>[], elem: <a href="bytes.html">bytes</a>) &rarr; <a href="bytes.html">bytes</a>[]</code></td><td><span class="funcdesc"><p>Appends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_append(array: <a href="date.html">date</a>[], elem: <a href="date.html">date</a>) &rarr; <a href="date.html">date</a>[]</code></td><td><span class="funcdesc"><p>Appends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_append(array: <a href="decimal.html">decimal</a>[], elem: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a>[]</code></td><td><span class="funcdesc"><p>Appends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_append(array: <a href="float.html">float</a>[], elem: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a>[]</code></td><td><span class="funcdesc"><p>Appends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_append(array: <a href="inet.html">inet</a>[], elem: <a href="inet.html">inet</a>) &rarr; <a href="inet.html">inet</a>[]</code></td><td><span class="funcdesc"><p>Appends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_append(array: <a href="int.html">int</a>[], elem: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a>[]</code></td><td><span class="funcdesc"><p>Appends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_append(array: <a href="interval.html">interval</a>[], elem: <a href="interval.html">interval</a>) &rarr; <a href="interval.html">interval</a>[]</code></td><td><span class="funcdesc"><p>Appends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_append(array: <a href="string.html">string</a>[], elem: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a>[]</code></td><td><span class="funcdesc"><p>Appends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_append(array: <a href="time.html">time</a>[], elem: <a href="time.html">time</a>) &rarr; <a href="time.html">time</a>[]</code></td><td><span class="funcdesc"><p>Appends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_append(array: <a href="timestamp.html">timestamp</a>[], elem: <a href="timestamp.html">timestamp</a>) &rarr; <a href="timestamp.html">timestamp</a>[]</code></td><td><span class="funcdesc"><p>Appends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_append(array: <a href="timestamp.html">timestamptz</a>[], elem: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="timestamp.html">timestamptz</a>[]</code></td><td><span class="funcdesc"><p>Appends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_append(array: <a href="uuid.html">uuid</a>[], elem: <a href="uuid.html">uuid</a>) &rarr; <a href="uuid.html">uuid</a>[]</code></td><td><span class="funcdesc"><p>Appends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_append(array: oid[], elem: oid) &rarr; oid[]</code></td><td><span class="funcdesc"><p>Appends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_cat(left: <a href="bool.html">bool</a>[], right: <a href="bool.html">bool</a>[]) &rarr; <a href="bool.html">bool</a>[]</code></td><td><span class="funcdesc"><p>Appends two arrays.</p>
</span></td></tr>
<tr><td><code>array_cat(left: <a href="bytes.html">bytes</a>[], right: <a href="bytes.html">bytes</a>[]) &rarr; <a href="bytes.html">bytes</a>[]</code></td><td><span class="funcdesc"><p>Appends two arrays.</p>
</span></td></tr>
<tr><td><code>array_cat(left: <a href="date.html">date</a>[], right: <a href="date.html">date</a>[]) &rarr; <a href="date.html">date</a>[]</code></td><td><span class="funcdesc"><p>Appends two arrays.</p>
</span></td></tr>
<tr><td><code>array_cat(left: <a href="decimal.html">decimal</a>[], right: <a href="decimal.html">decimal</a>[]) &rarr; <a href="decimal.html">decimal</a>[]</code></td><td><span class="funcdesc"><p>Appends two arrays.</p>
</span></td></tr>
<tr><td><code>array_cat(left: <a href="float.html">float</a>[], right: <a href="float.html">float</a>[]) &rarr; <a href="float.html">float</a>[]</code></td><td><span class="funcdesc"><p>Appends two arrays.</p>
</span></td></tr>
<tr><td><code>array_cat(left: <a href="inet.html">inet</a>[], right: <a href="inet.html">inet</a>[]) &rarr; <a href="inet.html">inet</a>[]</code></td><td><span class="funcdesc"><p>Appends two arrays.</p>
</span></td></tr>
<tr><td><code>array_cat(left: <a href="int.html">int</a>[], right: <a href="int.html">int</a>[]) &rarr; <a href="int.html">int</a>[]</code></td><td><span class="funcdesc"><p>Appends two arrays.</p>
</span></td></tr>
<tr><td><code>array_cat(left: <a href="interval.html">interval</a>[], right: <a href="interval.html">interval</a>[]) &rarr; <a href="interval.html">interval</a>[]</code></td><td><span class="funcdesc"><p>Appends two arrays.</p>
</span></td></tr>
<tr><td><code>array_cat(left: <a href="string.html">string</a>[], right: <a href="string.html">string</a>[]) &rarr; <a href="string.html">string</a>[]</code></td><td><span class="funcdesc"><p>Appends two arrays.</p>
</span></td></tr>
<tr><td><code>array_cat(left: <a href="time.html">time</a>[], right: <a href="time.html">time</a>[]) &rarr; <a href="time.html">time</a>[]</code></td><td><span class="funcdesc"><p>Appends two arrays.</p>
</span></td></tr>
<tr><td><code>array_cat(left: <a href="timestamp.html">timestamp</a>[], right: <a href="timestamp.html">timestamp</a>[]) &rarr; <a href="timestamp.html">timestamp</a>[]</code></td><td><span class="funcdesc"><p>Appends two arrays.</p>
</span></td></tr>
<tr><td><code>array_cat(left: <a href="timestamp.html">timestamptz</a>[], right: <a href="timestamp.html">timestamptz</a>[]) &rarr; <a href="timestamp.html">timestamptz</a>[]</code></td><td><span class="funcdesc"><p>Appends two arrays.</p>
</span></td></tr>
<tr><td><code>array_cat(left: <a href="uuid.html">uuid</a>[], right: <a href="uuid.html">uuid</a>[]) &rarr; <a href="uuid.html">uuid</a>[]</code></td><td><span class="funcdesc"><p>Appends two arrays.</p>
</span></td></tr>
<tr><td><code>array_cat(left: oid[], right: oid[]) &rarr; oid[]</code></td><td><span class="funcdesc"><p>Appends two arrays.</p>
</span></td></tr>
<tr><td><code>array_length(input: anyelement[], array_dimension: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the length of <code>input</code> on the provided <code>array_dimension</code>. However, because CockroachDB doesn’t yet support multi-dimensional arrays, the only supported <code>array_dimension</code> is <strong>1</strong>.</p>
</span></td></tr>
<tr><td><code>array_lower(input: anyelement[], array_dimension: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the minimum value of <code>input</code> on the provided <code>array_dimension</code>. However, because CockroachDB doesn’t yet support multi-dimensional arrays, the only supported <code>array_dimension</code> is <strong>1</strong>.</p>
</span></td></tr>
<tr><td><code>array_position(array: <a href="bool.html">bool</a>[], elem: <a href="bool.html">bool</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Return the index of the first occurrence of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_position(array: <a href="bytes.html">bytes</a>[], elem: <a href="bytes.html">bytes</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Return the index of the first occurrence of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_position(array: <a href="date.html">date</a>[], elem: <a href="date.html">date</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Return the index of the first occurrence of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_position(array: <a href="decimal.html">decimal</a>[], elem: <a href="decimal.html">decimal</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Return the index of the first occurrence of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_position(array: <a href="float.html">float</a>[], elem: <a href="float.html">float</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Return the index of the first occurrence of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_position(array: <a href="inet.html">inet</a>[], elem: <a href="inet.html">inet</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Return the index of the first occurrence of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_position(array: <a href="int.html">int</a>[], elem: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Return the index of the first occurrence of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_position(array: <a href="interval.html">interval</a>[], elem: <a href="interval.html">interval</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Return the index of the first occurrence of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_position(array: <a href="string.html">string</a>[], elem: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Return the index of the first occurrence of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_position(array: <a href="time.html">time</a>[], elem: <a href="time.html">time</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Return the index of the first occurrence of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_position(array: <a href="timestamp.html">timestamp</a>[], elem: <a href="timestamp.html">timestamp</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Return the index of the first occurrence of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_position(array: <a href="timestamp.html">timestamptz</a>[], elem: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Return the index of the first occurrence of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_position(array: <a href="uuid.html">uuid</a>[], elem: <a href="uuid.html">uuid</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Return the index of the first occurrence of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_position(array: oid[], elem: oid) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Return the index of the first occurrence of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_positions(array: <a href="bool.html">bool</a>[], elem: <a href="bool.html">bool</a>) &rarr; <a href="int.html">int</a>[]</code></td><td><span class="funcdesc"><p>Returns and array of indexes of all occurrences of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_positions(array: <a href="bytes.html">bytes</a>[], elem: <a href="bytes.html">bytes</a>) &rarr; <a href="int.html">int</a>[]</code></td><td><span class="funcdesc"><p>Returns and array of indexes of all occurrences of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_positions(array: <a href="date.html">date</a>[], elem: <a href="date.html">date</a>) &rarr; <a href="int.html">int</a>[]</code></td><td><span class="funcdesc"><p>Returns and array of indexes of all occurrences of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_positions(array: <a href="decimal.html">decimal</a>[], elem: <a href="decimal.html">decimal</a>) &rarr; <a href="int.html">int</a>[]</code></td><td><span class="funcdesc"><p>Returns and array of indexes of all occurrences of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_positions(array: <a href="float.html">float</a>[], elem: <a href="float.html">float</a>) &rarr; <a href="int.html">int</a>[]</code></td><td><span class="funcdesc"><p>Returns and array of indexes of all occurrences of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_positions(array: <a href="inet.html">inet</a>[], elem: <a href="inet.html">inet</a>) &rarr; <a href="int.html">int</a>[]</code></td><td><span class="funcdesc"><p>Returns and array of indexes of all occurrences of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_positions(array: <a href="int.html">int</a>[], elem: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a>[]</code></td><td><span class="funcdesc"><p>Returns and array of indexes of all occurrences of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_positions(array: <a href="interval.html">interval</a>[], elem: <a href="interval.html">interval</a>) &rarr; <a href="int.html">int</a>[]</code></td><td><span class="funcdesc"><p>Returns and array of indexes of all occurrences of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_positions(array: <a href="string.html">string</a>[], elem: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a>[]</code></td><td><span class="funcdesc"><p>Returns and array of indexes of all occurrences of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_positions(array: <a href="time.html">time</a>[], elem: <a href="time.html">time</a>) &rarr; <a href="int.html">int</a>[]</code></td><td><span class="funcdesc"><p>Returns and array of indexes of all occurrences of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_positions(array: <a href="timestamp.html">timestamp</a>[], elem: <a href="timestamp.html">timestamp</a>) &rarr; <a href="int.html">int</a>[]</code></td><td><span class="funcdesc"><p>Returns and array of indexes of all occurrences of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_positions(array: <a href="timestamp.html">timestamptz</a>[], elem: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="int.html">int</a>[]</code></td><td><span class="funcdesc"><p>Returns and array of indexes of all occurrences of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_positions(array: <a href="uuid.html">uuid</a>[], elem: <a href="uuid.html">uuid</a>) &rarr; <a href="int.html">int</a>[]</code></td><td><span class="funcdesc"><p>Returns and array of indexes of all occurrences of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_positions(array: oid[], elem: oid) &rarr; <a href="int.html">int</a>[]</code></td><td><span class="funcdesc"><p>Returns and array of indexes of all occurrences of <code>elem</code> in <code>array</code>.</p>
</span></td></tr>
<tr><td><code>array_prepend(elem: <a href="bool.html">bool</a>, array: <a href="bool.html">bool</a>[]) &rarr; <a href="bool.html">bool</a>[]</code></td><td><span class="funcdesc"><p>Prepends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_prepend(elem: <a href="bytes.html">bytes</a>, array: <a href="bytes.html">bytes</a>[]) &rarr; <a href="bytes.html">bytes</a>[]</code></td><td><span class="funcdesc"><p>Prepends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_prepend(elem: <a href="date.html">date</a>, array: <a href="date.html">date</a>[]) &rarr; <a href="date.html">date</a>[]</code></td><td><span class="funcdesc"><p>Prepends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_prepend(elem: <a href="decimal.html">decimal</a>, array: <a href="decimal.html">decimal</a>[]) &rarr; <a href="decimal.html">decimal</a>[]</code></td><td><span class="funcdesc"><p>Prepends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_prepend(elem: <a href="float.html">float</a>, array: <a href="float.html">float</a>[]) &rarr; <a href="float.html">float</a>[]</code></td><td><span class="funcdesc"><p>Prepends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_prepend(elem: <a href="inet.html">inet</a>, array: <a href="inet.html">inet</a>[]) &rarr; <a href="inet.html">inet</a>[]</code></td><td><span class="funcdesc"><p>Prepends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_prepend(elem: <a href="int.html">int</a>, array: <a href="int.html">int</a>[]) &rarr; <a href="int.html">int</a>[]</code></td><td><span class="funcdesc"><p>Prepends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_prepend(elem: <a href="interval.html">interval</a>, array: <a href="interval.html">interval</a>[]) &rarr; <a href="interval.html">interval</a>[]</code></td><td><span class="funcdesc"><p>Prepends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_prepend(elem: <a href="string.html">string</a>, array: <a href="string.html">string</a>[]) &rarr; <a href="string.html">string</a>[]</code></td><td><span class="funcdesc"><p>Prepends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_prepend(elem: <a href="time.html">time</a>, array: <a href="time.html">time</a>[]) &rarr; <a href="time.html">time</a>[]</code></td><td><span class="funcdesc"><p>Prepends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_prepend(elem: <a href="timestamp.html">timestamp</a>, array: <a href="timestamp.html">timestamp</a>[]) &rarr; <a href="timestamp.html">timestamp</a>[]</code></td><td><span class="funcdesc"><p>Prepends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_prepend(elem: <a href="timestamp.html">timestamptz</a>, array: <a href="timestamp.html">timestamptz</a>[]) &rarr; <a href="timestamp.html">timestamptz</a>[]</code></td><td><span class="funcdesc"><p>Prepends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_prepend(elem: <a href="uuid.html">uuid</a>, array: <a href="uuid.html">uuid</a>[]) &rarr; <a href="uuid.html">uuid</a>[]</code></td><td><span class="funcdesc"><p>Prepends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_prepend(elem: oid, array: oid[]) &rarr; oid[]</code></td><td><span class="funcdesc"><p>Prepends <code>elem</code> to <code>array</code>, returning the result.</p>
</span></td></tr>
<tr><td><code>array_remove(array: <a href="bool.html">bool</a>[], elem: <a href="bool.html">bool</a>) &rarr; <a href="bool.html">bool</a>[]</code></td><td><span class="funcdesc"><p>Remove from <code>array</code> all elements equal to <code>elem</code>.</p>
</span></td></tr>
<tr><td><code>array_remove(array: <a href="bytes.html">bytes</a>[], elem: <a href="bytes.html">bytes</a>) &rarr; <a href="bytes.html">bytes</a>[]</code></td><td><span class="funcdesc"><p>Remove from <code>array</code> all elements equal to <code>elem</code>.</p>
</span></td></tr>
<tr><td><code>array_remove(array: <a href="date.html">date</a>[], elem: <a href="date.html">date</a>) &rarr; <a href="date.html">date</a>[]</code></td><td><span class="funcdesc"><p>Remove from <code>array</code> all elements equal to <code>elem</code>.</p>
</span></td></tr>
<tr><td><code>array_remove(array: <a href="decimal.html">decimal</a>[], elem: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a>[]</code></td><td><span class="funcdesc"><p>Remove from <code>array</code> all elements equal to <code>elem</code>.</p>
</span></td></tr>
<tr><td><code>array_remove(array: <a href="float.html">float</a>[], elem: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a>[]</code></td><td><span class="funcdesc"><p>Remove from <code>array</code> all elements equal to <code>elem</code>.</p>
</span></td></tr>
<tr><td><code>array_remove(array: <a href="inet.html">inet</a>[], elem: <a href="inet.html">inet</a>) &rarr; <a href="inet.html">inet</a>[]</code></td><td><span class="funcdesc"><p>Remove from <code>array</code> all elements equal to <code>elem</code>.</p>
</span></td></tr>
<tr><td><code>array_remove(array: <a href="int.html">int</a>[], elem: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a>[]</code></td><td><span class="funcdesc"><p>Remove from <code>array</code> all elements equal to <code>elem</code>.</p>
</span></td></tr>
<tr><td><code>array_remove(array: <a href="interval.html">interval</a>[], elem: <a href="interval.html">interval</a>) &rarr; <a href="interval.html">interval</a>[]</code></td><td><span class="funcdesc"><p>Remove from <code>array</code> all elements equal to <code>elem</code>.</p>
</span></td></tr>
<tr><td><code>array_remove(array: <a href="string.html">string</a>[], elem: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a>[]</code></td><td><span class="funcdesc"><p>Remove from <code>array</code> all elements equal to <code>elem</code>.</p>
</span></td></tr>
<tr><td><code>array_remove(array: <a href="time.html">time</a>[], elem: <a href="time.html">time</a>) &rarr; <a href="time.html">time</a>[]</code></td><td><span class="funcdesc"><p>Remove from <code>array</code> all elements equal to <code>elem</code>.</p>
</span></td></tr>
<tr><td><code>array_remove(array: <a href="timestamp.html">timestamp</a>[], elem: <a href="timestamp.html">timestamp</a>) &rarr; <a href="timestamp.html">timestamp</a>[]</code></td><td><span class="funcdesc"><p>Remove from <code>array</code> all elements equal to <code>elem</code>.</p>
</span></td></tr>
<tr><td><code>array_remove(array: <a href="timestamp.html">timestamptz</a>[], elem: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="timestamp.html">timestamptz</a>[]</code></td><td><span class="funcdesc"><p>Remove from <code>array</code> all elements equal to <code>elem</code>.</p>
</span></td></tr>
<tr><td><code>array_remove(array: <a href="uuid.html">uuid</a>[], elem: <a href="uuid.html">uuid</a>) &rarr; <a href="uuid.html">uuid</a>[]</code></td><td><span class="funcdesc"><p>Remove from <code>array</code> all elements equal to <code>elem</code>.</p>
</span></td></tr>
<tr><td><code>array_remove(array: oid[], elem: oid) &rarr; oid[]</code></td><td><span class="funcdesc"><p>Remove from <code>array</code> all elements equal to <code>elem</code>.</p>
</span></td></tr>
<tr><td><code>array_replace(array: <a href="bool.html">bool</a>[], toreplace: <a href="bool.html">bool</a>, replacewith: <a href="bool.html">bool</a>) &rarr; <a href="bool.html">bool</a>[]</code></td><td><span class="funcdesc"><p>Replace all occurrences of <code>toreplace</code> in <code>array</code> with <code>replacewith</code>.</p>
</span></td></tr>
<tr><td><code>array_replace(array: <a href="bytes.html">bytes</a>[], toreplace: <a href="bytes.html">bytes</a>, replacewith: <a href="bytes.html">bytes</a>) &rarr; <a href="bytes.html">bytes</a>[]</code></td><td><span class="funcdesc"><p>Replace all occurrences of <code>toreplace</code> in <code>array</code> with <code>replacewith</code>.</p>
</span></td></tr>
<tr><td><code>array_replace(array: <a href="date.html">date</a>[], toreplace: <a href="date.html">date</a>, replacewith: <a href="date.html">date</a>) &rarr; <a href="date.html">date</a>[]</code></td><td><span class="funcdesc"><p>Replace all occurrences of <code>toreplace</code> in <code>array</code> with <code>replacewith</code>.</p>
</span></td></tr>
<tr><td><code>array_replace(array: <a href="decimal.html">decimal</a>[], toreplace: <a href="decimal.html">decimal</a>, replacewith: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a>[]</code></td><td><span class="funcdesc"><p>Replace all occurrences of <code>toreplace</code> in <code>array</code> with <code>replacewith</code>.</p>
</span></td></tr>
<tr><td><code>array_replace(array: <a href="float.html">float</a>[], toreplace: <a href="float.html">float</a>, replacewith: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a>[]</code></td><td><span class="funcdesc"><p>Replace all occurrences of <code>toreplace</code> in <code>array</code> with <code>replacewith</code>.</p>
</span></td></tr>
<tr><td><code>array_replace(array: <a href="inet.html">inet</a>[], toreplace: <a href="inet.html">inet</a>, replacewith: <a href="inet.html">inet</a>) &rarr; <a href="inet.html">inet</a>[]</code></td><td><span class="funcdesc"><p>Replace all occurrences of <code>toreplace</code> in <code>array</code> with <code>replacewith</code>.</p>
</span></td></tr>
<tr><td><code>array_replace(array: <a href="int.html">int</a>[], toreplace: <a href="int.html">int</a>, replacewith: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a>[]</code></td><td><span class="funcdesc"><p>Replace all occurrences of <code>toreplace</code> in <code>array</code> with <code>replacewith</code>.</p>
</span></td></tr>
<tr><td><code>array_replace(array: <a href="interval.html">interval</a>[], toreplace: <a href="interval.html">interval</a>, replacewith: <a href="interval.html">interval</a>) &rarr; <a href="interval.html">interval</a>[]</code></td><td><span class="funcdesc"><p>Replace all occurrences of <code>toreplace</code> in <code>array</code> with <code>replacewith</code>.</p>
</span></td></tr>
<tr><td><code>array_replace(array: <a href="string.html">string</a>[], toreplace: <a href="string.html">string</a>, replacewith: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a>[]</code></td><td><span class="funcdesc"><p>Replace all occurrences of <code>toreplace</code> in <code>array</code> with <code>replacewith</code>.</p>
</span></td></tr>
<tr><td><code>array_replace(array: <a href="time.html">time</a>[], toreplace: <a href="time.html">time</a>, replacewith: <a href="time.html">time</a>) &rarr; <a href="time.html">time</a>[]</code></td><td><span class="funcdesc"><p>Replace all occurrences of <code>toreplace</code> in <code>array</code> with <code>replacewith</code>.</p>
</span></td></tr>
<tr><td><code>array_replace(array: <a href="timestamp.html">timestamp</a>[], toreplace: <a href="timestamp.html">timestamp</a>, replacewith: <a href="timestamp.html">timestamp</a>) &rarr; <a href="timestamp.html">timestamp</a>[]</code></td><td><span class="funcdesc"><p>Replace all occurrences of <code>toreplace</code> in <code>array</code> with <code>replacewith</code>.</p>
</span></td></tr>
<tr><td><code>array_replace(array: <a href="timestamp.html">timestamptz</a>[], toreplace: <a href="timestamp.html">timestamptz</a>, replacewith: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="timestamp.html">timestamptz</a>[]</code></td><td><span class="funcdesc"><p>Replace all occurrences of <code>toreplace</code> in <code>array</code> with <code>replacewith</code>.</p>
</span></td></tr>
<tr><td><code>array_replace(array: <a href="uuid.html">uuid</a>[], toreplace: <a href="uuid.html">uuid</a>, replacewith: <a href="uuid.html">uuid</a>) &rarr; <a href="uuid.html">uuid</a>[]</code></td><td><span class="funcdesc"><p>Replace all occurrences of <code>toreplace</code> in <code>array</code> with <code>replacewith</code>.</p>
</span></td></tr>
<tr><td><code>array_replace(array: oid[], toreplace: oid, replacewith: oid) &rarr; oid[]</code></td><td><span class="funcdesc"><p>Replace all occurrences of <code>toreplace</code> in <code>array</code> with <code>replacewith</code>.</p>
</span></td></tr>
<tr><td><code>array_upper(input: anyelement[], array_dimension: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the maximum value of <code>input</code> on the provided <code>array_dimension</code>. However, because CockroachDB doesn’t yet support multi-dimensional arrays, the only supported <code>array_dimension</code> is <strong>1</strong>.</p>
</span></td></tr>
<tr><td><code>string_to_array(str: <a href="string.html">string</a>, delimiter: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a>[]</code></td><td><span class="funcdesc"><p>Split a string into components on a delimiter.</p>
</span></td></tr>
<tr><td><code>string_to_array(str: <a href="string.html">string</a>, delimiter: <a href="string.html">string</a>, null: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a>[]</code></td><td><span class="funcdesc"><p>Split a string into components on a delimiter with a specified string to consider NULL.</p>
</span></td></tr></tbody>
</table>

### BOOL Functions

<table>
<thead><tr><th>Function &rarr; Returns</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>inet_contained_by_or_equals(val: <a href="inet.html">inet</a>, container: <a href="inet.html">inet</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Test for subnet inclusion or equality, using only the network parts of the addresses. The host part of the addresses is ignored.</p>
</span></td></tr>
<tr><td><code>inet_contains_or_contained_by(val: <a href="inet.html">inet</a>, val: <a href="inet.html">inet</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Test for subnet inclusion, using only the network parts of the addresses. The host part of the addresses is ignored.</p>
</span></td></tr>
<tr><td><code>inet_contains_or_equals(container: <a href="inet.html">inet</a>, val: <a href="inet.html">inet</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Test for subnet inclusion or equality, using only the network parts of the addresses. The host part of the addresses is ignored.</p>
</span></td></tr>
<tr><td><code>inet_same_family(val: <a href="inet.html">inet</a>, val: <a href="inet.html">inet</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Checks if two IP addresses are of the same IP family.</p>
</span></td></tr></tbody>
</table>

### Comparison Functions

<table>
<thead><tr><th>Function &rarr; Returns</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>greatest(anyelement...) &rarr; anyelement</code></td><td><span class="funcdesc"><p>Returns the element with the greatest value.</p>
</span></td></tr>
<tr><td><code>least(anyelement...) &rarr; anyelement</code></td><td><span class="funcdesc"><p>Returns the element with the lowest value.</p>
</span></td></tr></tbody>
</table>

### Date and Time Functions

<table>
<thead><tr><th>Function &rarr; Returns</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>age(begin: <a href="timestamp.html">timestamptz</a>, end: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="interval.html">interval</a></code></td><td><span class="funcdesc"><p>Calculates the interval between <code>begin</code> and <code>end</code>.</p>
</span></td></tr>
<tr><td><code>age(val: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="interval.html">interval</a></code></td><td><span class="funcdesc"><p>Calculates the interval between <code>val</code> and the current time.</p>
</span></td></tr>
<tr><td><code>clock_timestamp() &rarr; <a href="timestamp.html">timestamp</a></code></td><td><span class="funcdesc"><p>Returns the current wallclock time.</p>
</span></td></tr>
<tr><td><code>clock_timestamp() &rarr; <a href="timestamp.html">timestamptz</a></code></td><td><span class="funcdesc"><p>Returns the current wallclock time.</p>
</span></td></tr>
<tr><td><code>current_date() &rarr; <a href="date.html">date</a></code></td><td><span class="funcdesc"><p>Returns the current date.</p>
</span></td></tr>
<tr><td><code>current_timestamp() &rarr; <a href="timestamp.html">timestamp</a></code></td><td><span class="funcdesc"><p>Returns the current transaction’s timestamp.</p>
</span></td></tr>
<tr><td><code>current_timestamp() &rarr; <a href="timestamp.html">timestamptz</a></code></td><td><span class="funcdesc"><p>Returns the current transaction’s timestamp.</p>
</span></td></tr>
<tr><td><code>date_trunc(element: <a href="string.html">string</a>, input: <a href="date.html">date</a>) &rarr; <a href="timestamp.html">timestamptz</a></code></td><td><span class="funcdesc"><p>Truncates <code>input</code> to precision <code>element</code>.  Sets all fields that are less
significant than <code>element</code> to zero (or one, for day and month)</p>
<p>Compatible elements: year, quarter, month, week, hour, minute, second,
millisecond, microsecond.</p>
</span></td></tr>
<tr><td><code>date_trunc(element: <a href="string.html">string</a>, input: <a href="time.html">time</a>) &rarr; <a href="interval.html">interval</a></code></td><td><span class="funcdesc"><p>Truncates <code>input</code> to precision <code>element</code>.  Sets all fields that are less
significant than <code>element</code> to zero.</p>
<p>Compatible elements: hour, minute, second, millisecond, microsecond.</p>
</span></td></tr>
<tr><td><code>date_trunc(element: <a href="string.html">string</a>, input: <a href="timestamp.html">timestamp</a>) &rarr; <a href="timestamp.html">timestamp</a></code></td><td><span class="funcdesc"><p>Truncates <code>input</code> to precision <code>element</code>.  Sets all fields that are less
significant than <code>element</code> to zero (or one, for day and month)</p>
<p>Compatible elements: year, quarter, month, week, hour, minute, second,
millisecond, microsecond.</p>
</span></td></tr>
<tr><td><code>date_trunc(element: <a href="string.html">string</a>, input: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="timestamp.html">timestamptz</a></code></td><td><span class="funcdesc"><p>Truncates <code>input</code> to precision <code>element</code>.  Sets all fields that are less
significant than <code>element</code> to zero (or one, for day and month)</p>
<p>Compatible elements: year, quarter, month, week, hour, minute, second,
millisecond, microsecond.</p>
</span></td></tr>
<tr><td><code>experimental_strftime(input: <a href="date.html">date</a>, extract_format: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>From <code>input</code>, extracts and formats the time as identified in <code>extract_format</code> using standard <code>strftime</code> notation (though not all formatting is supported).</p>
</span></td></tr>
<tr><td><code>experimental_strftime(input: <a href="timestamp.html">timestamp</a>, extract_format: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>From <code>input</code>, extracts and formats the time as identified in <code>extract_format</code> using standard <code>strftime</code> notation (though not all formatting is supported).</p>
</span></td></tr>
<tr><td><code>experimental_strftime(input: <a href="timestamp.html">timestamptz</a>, extract_format: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>From <code>input</code>, extracts and formats the time as identified in <code>extract_format</code> using standard <code>strftime</code> notation (though not all formatting is supported).</p>
</span></td></tr>
<tr><td><code>experimental_strptime(input: <a href="string.html">string</a>, format: <a href="string.html">string</a>) &rarr; <a href="timestamp.html">timestamptz</a></code></td><td><span class="funcdesc"><p>Returns <code>input</code> as a timestamptz using <code>format</code> (which uses standard <code>strptime</code> formatting).</p>
</span></td></tr>
<tr><td><code>extract(element: <a href="string.html">string</a>, input: <a href="date.html">date</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Extracts <code>element</code> from <code>input</code>.</p>
<p>Compatible elements: year, quarter, month, week, dayofweek, dayofyear,
hour, minute, second, millisecond, microsecond, epoch</p>
</span></td></tr>
<tr><td><code>extract(element: <a href="string.html">string</a>, input: <a href="time.html">time</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Extracts <code>element</code> from <code>input</code>.</p>
<p>Compatible elements: hour, minute, second, millisecond, microsecond, epoch</p>
</span></td></tr>
<tr><td><code>extract(element: <a href="string.html">string</a>, input: <a href="timestamp.html">timestamp</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Extracts <code>element</code> from <code>input</code>.</p>
<p>Compatible elements: year, quarter, month, week, dayofweek, dayofyear,
hour, minute, second, millisecond, microsecond, epoch</p>
</span></td></tr>
<tr><td><code>extract(element: <a href="string.html">string</a>, input: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Extracts <code>element</code> from <code>input</code>.</p>
<p>Compatible elements: year, quarter, month, week, dayofweek, dayofyear,
hour, minute, second, millisecond, microsecond, epoch</p>
</span></td></tr>
<tr><td><code>extract_duration(element: <a href="string.html">string</a>, input: <a href="interval.html">interval</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Extracts <code>element</code> from <code>input</code>.
Compatible elements: hour, minute, second, millisecond, microsecond.</p>
</span></td></tr>
<tr><td><code>now() &rarr; <a href="timestamp.html">timestamp</a></code></td><td><span class="funcdesc"><p>Returns the current transaction’s timestamp.</p>
</span></td></tr>
<tr><td><code>now() &rarr; <a href="timestamp.html">timestamptz</a></code></td><td><span class="funcdesc"><p>Returns the current transaction’s timestamp.</p>
</span></td></tr>
<tr><td><code>statement_timestamp() &rarr; <a href="timestamp.html">timestamp</a></code></td><td><span class="funcdesc"><p>Returns the current statement’s timestamp.</p>
</span></td></tr>
<tr><td><code>statement_timestamp() &rarr; <a href="timestamp.html">timestamptz</a></code></td><td><span class="funcdesc"><p>Returns the current statement’s timestamp.</p>
</span></td></tr>
<tr><td><code>transaction_timestamp() &rarr; <a href="timestamp.html">timestamp</a></code></td><td><span class="funcdesc"><p>Returns the current transaction’s timestamp.</p>
</span></td></tr>
<tr><td><code>transaction_timestamp() &rarr; <a href="timestamp.html">timestamptz</a></code></td><td><span class="funcdesc"><p>Returns the current transaction’s timestamp.</p>
</span></td></tr></tbody>
</table>

### ID Generation Functions

<table>
<thead><tr><th>Function &rarr; Returns</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>experimental_uuid_v4() &rarr; <a href="bytes.html">bytes</a></code></td><td><span class="funcdesc"><p>Returns a UUID.</p>
</span></td></tr>
<tr><td><code>gen_random_uuid() &rarr; <a href="uuid.html">uuid</a></code></td><td><span class="funcdesc"><p>Generates a random UUID and returns it as a value of UUID type.</p>
</span></td></tr>
<tr><td><code>unique_rowid() &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Returns a unique ID used by CockroachDB to generate unique row IDs if a Primary Key isn’t defined for the table. The value is a combination of the  insert timestamp and the ID of the node executing the statement, which  guarantees this combination is globally unique.</p>
</span></td></tr>
<tr><td><code>uuid_v4() &rarr; <a href="bytes.html">bytes</a></code></td><td><span class="funcdesc"><p>Returns a UUID.</p>
</span></td></tr></tbody>
</table>

### INET Functions

<table>
<thead><tr><th>Function &rarr; Returns</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>abbrev(val: <a href="inet.html">inet</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Converts the combined IP address and prefix length to an abbreviated display format as text.For INET types, this will omit the prefix length if it’s not the default (32 or IPv4, 128 for IPv6)</p>
<p>For example, <code>abbrev('192.168.1.2/24')</code> returns <code>'192.168.1.2/24'</code></p>
</span></td></tr>
<tr><td><code>broadcast(val: <a href="inet.html">inet</a>) &rarr; <a href="inet.html">inet</a></code></td><td><span class="funcdesc"><p>Gets the broadcast address for the network address represented by the value.</p>
<p>For example, <code>broadcast('192.168.1.2/24')</code> returns <code>'192.168.1.255/24'</code></p>
</span></td></tr>
<tr><td><code>family(val: <a href="inet.html">inet</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Extracts the IP family of the value; 4 for IPv4, 6 for IPv6.</p>
<p>For example, <code>family('::1')</code> returns <code>6</code></p>
</span></td></tr>
<tr><td><code>host(val: <a href="inet.html">inet</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Extracts the address part of the combined address/prefixlen value as text.</p>
<p>For example, <code>host('192.168.1.2/16')</code> returns <code>'192.168.1.2'</code></p>
</span></td></tr>
<tr><td><code>hostmask(val: <a href="inet.html">inet</a>) &rarr; <a href="inet.html">inet</a></code></td><td><span class="funcdesc"><p>Creates an IP host mask corresponding to the prefix length in the value.</p>
<p>For example, <code>hostmask('192.168.1.2/16')</code> returns <code>'0.0.255.255'</code></p>
</span></td></tr>
<tr><td><code>masklen(val: <a href="inet.html">inet</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Retrieves the prefix length stored in the value.</p>
<p>For example, <code>masklen('192.168.1.2/16')</code> returns <code>16</code></p>
</span></td></tr>
<tr><td><code>netmask(val: <a href="inet.html">inet</a>) &rarr; <a href="inet.html">inet</a></code></td><td><span class="funcdesc"><p>Creates an IP network mask corresponding to the prefix length in the value.</p>
<p>For example, <code>netmask('192.168.1.2/16')</code> returns <code>'255.255.0.0'</code></p>
</span></td></tr>
<tr><td><code>set_masklen(val: <a href="inet.html">inet</a>, prefixlen: <a href="int.html">int</a>) &rarr; <a href="inet.html">inet</a></code></td><td><span class="funcdesc"><p>Sets the prefix length of <code>val</code> to <code>prefixlen</code>.</p>
<p>For example, <code>set_masklen('192.168.1.2', 16)</code> returns <code>'192.168.1.2/16'</code>.</p>
</span></td></tr>
<tr><td><code>text(val: <a href="inet.html">inet</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Converts the IP address and prefix length to text.</p>
</span></td></tr></tbody>
</table>

### JSONB Functions

<table>
<thead><tr><th>Function &rarr; Returns</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>json_array_length(json: jsonb) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Returns the number of elements in the outermost JSON or JSONB array.</p>
</span></td></tr>
<tr><td><code>json_build_array(anyelement...) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Builds a possibly-heterogeneously-typed JSON or JSONB array out of a variadic argument list.</p>
</span></td></tr>
<tr><td><code>json_build_object(anyelement...) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Builds a JSON object out of a variadic argument list.</p>
</span></td></tr>
<tr><td><code>json_extract_path(jsonb, <a href="string.html">string</a>...) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Returns the JSON value pointed to by the variadic arguments.</p>
</span></td></tr>
<tr><td><code>json_object(keys: <a href="string.html">string</a>[], values: <a href="string.html">string</a>[]) &rarr; jsonb</code></td><td><span class="funcdesc"><p>This form of json_object takes keys and values pairwise from two separate arrays. In all other respects it is identical to the one-argument form.</p>
</span></td></tr>
<tr><td><code>json_remove_path(val: jsonb, path: <a href="string.html">string</a>[]) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Remove the specified path from the JSON object.</p>
</span></td></tr>
<tr><td><code>json_set(val: jsonb, path: <a href="string.html">string</a>[], to: jsonb) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Returns the JSON value pointed to by the variadic arguments.</p>
</span></td></tr>
<tr><td><code>json_set(val: jsonb, path: <a href="string.html">string</a>[], to: jsonb, create_missing: <a href="bool.html">bool</a>) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Returns the JSON value pointed to by the variadic arguments. If <code>create_missing</code> is false, new keys will not be inserted to objects and values will not be prepended or appended to arrays.</p>
</span></td></tr>
<tr><td><code>json_strip_nulls(from_json: jsonb) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Returns from_json with all object fields that have null values omitted. Other null values are untouched.</p>
</span></td></tr>
<tr><td><code>json_typeof(val: jsonb) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns the type of the outermost JSON value as a text string.</p>
</span></td></tr>
<tr><td><code>jsonb_array_length(json: jsonb) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Returns the number of elements in the outermost JSON or JSONB array.</p>
</span></td></tr>
<tr><td><code>jsonb_build_array(anyelement...) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Builds a possibly-heterogeneously-typed JSON or JSONB array out of a variadic argument list.</p>
</span></td></tr>
<tr><td><code>jsonb_build_object(anyelement...) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Builds a JSON object out of a variadic argument list.</p>
</span></td></tr>
<tr><td><code>jsonb_extract_path(jsonb, <a href="string.html">string</a>...) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Returns the JSON value pointed to by the variadic arguments.</p>
</span></td></tr>
<tr><td><code>jsonb_object(keys: <a href="string.html">string</a>[], values: <a href="string.html">string</a>[]) &rarr; jsonb</code></td><td><span class="funcdesc"><p>This form of json_object takes keys and values pairwise from two separate arrays. In all other respects it is identical to the one-argument form.</p>
</span></td></tr>
<tr><td><code>jsonb_pretty(val: jsonb) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns the given JSON value as a STRING indented and with newlines.</p>
</span></td></tr>
<tr><td><code>jsonb_set(val: jsonb, path: <a href="string.html">string</a>[], to: jsonb) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Returns the JSON value pointed to by the variadic arguments.</p>
</span></td></tr>
<tr><td><code>jsonb_set(val: jsonb, path: <a href="string.html">string</a>[], to: jsonb, create_missing: <a href="bool.html">bool</a>) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Returns the JSON value pointed to by the variadic arguments. If <code>create_missing</code> is false, new keys will not be inserted to objects and values will not be prepended or appended to arrays.</p>
</span></td></tr>
<tr><td><code>jsonb_strip_nulls(from_json: jsonb) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Returns from_json with all object fields that have null values omitted. Other null values are untouched.</p>
</span></td></tr>
<tr><td><code>jsonb_typeof(val: jsonb) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns the type of the outermost JSON value as a text string.</p>
</span></td></tr></tbody>
</table>

### Math and Numeric Functions

<table>
<thead><tr><th>Function &rarr; Returns</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>abs(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the absolute value of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>abs(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the absolute value of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>abs(val: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the absolute value of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>acos(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the inverse cosine of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>asin(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the inverse sine of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>atan(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the inverse tangent of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>atan2(x: <a href="float.html">float</a>, y: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the inverse tangent of <code>x</code>/<code>y</code>.</p>
</span></td></tr>
<tr><td><code>cbrt(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the cube root (∛) of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>cbrt(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the cube root (∛) of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>ceil(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the smallest integer greater than <code>val</code>.</p>
</span></td></tr>
<tr><td><code>ceil(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the smallest integer greater than <code>val</code>.</p>
</span></td></tr>
<tr><td><code>ceiling(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the smallest integer greater than <code>val</code>.</p>
</span></td></tr>
<tr><td><code>ceiling(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the smallest integer greater than <code>val</code>.</p>
</span></td></tr>
<tr><td><code>cos(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the cosine of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>cot(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the cotangent of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>crc32c(<a href="bytes.html">bytes</a>...) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the CRC-32 hash using the Castagnoli polynomial.</p>
</span></td></tr>
<tr><td><code>crc32c(<a href="string.html">string</a>...) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the CRC-32 hash using the Castagnoli polynomial.</p>
</span></td></tr>
<tr><td><code>crc32ieee(<a href="bytes.html">bytes</a>...) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the CRC-32 hash using the IEEE polynomial.</p>
</span></td></tr>
<tr><td><code>crc32ieee(<a href="string.html">string</a>...) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the CRC-32 hash using the IEEE polynomial.</p>
</span></td></tr>
<tr><td><code>degrees(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Converts <code>val</code> as a radian value to a degree value.</p>
</span></td></tr>
<tr><td><code>div(x: <a href="decimal.html">decimal</a>, y: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the integer quotient of <code>x</code>/<code>y</code>.</p>
</span></td></tr>
<tr><td><code>div(x: <a href="float.html">float</a>, y: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the integer quotient of <code>x</code>/<code>y</code>.</p>
</span></td></tr>
<tr><td><code>div(x: <a href="int.html">int</a>, y: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the integer quotient of <code>x</code>/<code>y</code>.</p>
</span></td></tr>
<tr><td><code>exp(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates <em>e</em> ^ <code>val</code>.</p>
</span></td></tr>
<tr><td><code>exp(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates <em>e</em> ^ <code>val</code>.</p>
</span></td></tr>
<tr><td><code>floor(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the largest integer not greater than <code>val</code>.</p>
</span></td></tr>
<tr><td><code>floor(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the largest integer not greater than <code>val</code>.</p>
</span></td></tr>
<tr><td><code>fnv32(<a href="bytes.html">bytes</a>...) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the 32-bit FNV-1 hash value of a set of values.</p>
</span></td></tr>
<tr><td><code>fnv32(<a href="string.html">string</a>...) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the 32-bit FNV-1 hash value of a set of values.</p>
</span></td></tr>
<tr><td><code>fnv32a(<a href="bytes.html">bytes</a>...) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the 32-bit FNV-1a hash value of a set of values.</p>
</span></td></tr>
<tr><td><code>fnv32a(<a href="string.html">string</a>...) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the 32-bit FNV-1a hash value of a set of values.</p>
</span></td></tr>
<tr><td><code>fnv64(<a href="bytes.html">bytes</a>...) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the 64-bit FNV-1 hash value of a set of values.</p>
</span></td></tr>
<tr><td><code>fnv64(<a href="string.html">string</a>...) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the 64-bit FNV-1 hash value of a set of values.</p>
</span></td></tr>
<tr><td><code>fnv64a(<a href="bytes.html">bytes</a>...) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the 64-bit FNV-1a hash value of a set of values.</p>
</span></td></tr>
<tr><td><code>fnv64a(<a href="string.html">string</a>...) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the 64-bit FNV-1a hash value of a set of values.</p>
</span></td></tr>
<tr><td><code>isnan(val: <a href="decimal.html">decimal</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns true if <code>val</code> is NaN, false otherwise.</p>
</span></td></tr>
<tr><td><code>isnan(val: <a href="float.html">float</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns true if <code>val</code> is NaN, false otherwise.</p>
</span></td></tr>
<tr><td><code>ln(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the natural log of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>ln(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the natural log of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>log(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the base 10 log of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>log(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the base 10 log of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>mod(x: <a href="decimal.html">decimal</a>, y: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates <code>x</code>%<code>y</code>.</p>
</span></td></tr>
<tr><td><code>mod(x: <a href="float.html">float</a>, y: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates <code>x</code>%<code>y</code>.</p>
</span></td></tr>
<tr><td><code>mod(x: <a href="int.html">int</a>, y: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates <code>x</code>%<code>y</code>.</p>
</span></td></tr>
<tr><td><code>pi() &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Returns the value for pi (3.141592653589793).</p>
</span></td></tr>
<tr><td><code>pow(x: <a href="decimal.html">decimal</a>, y: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates <code>x</code>^<code>y</code>.</p>
</span></td></tr>
<tr><td><code>pow(x: <a href="float.html">float</a>, y: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates <code>x</code>^<code>y</code>.</p>
</span></td></tr>
<tr><td><code>pow(x: <a href="int.html">int</a>, y: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates <code>x</code>^<code>y</code>.</p>
</span></td></tr>
<tr><td><code>power(x: <a href="decimal.html">decimal</a>, y: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates <code>x</code>^<code>y</code>.</p>
</span></td></tr>
<tr><td><code>power(x: <a href="float.html">float</a>, y: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates <code>x</code>^<code>y</code>.</p>
</span></td></tr>
<tr><td><code>power(x: <a href="int.html">int</a>, y: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates <code>x</code>^<code>y</code>.</p>
</span></td></tr>
<tr><td><code>radians(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Converts <code>val</code> as a degree value to a radians value.</p>
</span></td></tr>
<tr><td><code>random() &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Returns a random float between 0 and 1.</p>
</span></td></tr>
<tr><td><code>round(input: <a href="decimal.html">decimal</a>, decimal_accuracy: <a href="int.html">int</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Keeps <code>decimal_accuracy</code> number of figures to the right of the zero position  in <code>input using half away from zero rounding. If</code>decimal_accuracy` is not in the range -2^31…(2^31-1), the results are undefined.</p>
</span></td></tr>
<tr><td><code>round(input: <a href="float.html">float</a>, decimal_accuracy: <a href="int.html">int</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Keeps <code>decimal_accuracy</code> number of figures to the right of the zero position  in <code>input</code> using half to even (banker’s) rounding.</p>
</span></td></tr>
<tr><td><code>round(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Rounds <code>val</code> to the nearest integer, half away from zero: ROUND(+/-2.4) = +/-2, ROUND(+/-2.5) = +/-3.</p>
</span></td></tr>
<tr><td><code>round(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Rounds <code>val</code> to the nearest integer using half to even (banker’s) rounding.</p>
</span></td></tr>
<tr><td><code>sign(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Determines the sign of <code>val</code>: <strong>1</strong> for positive; <strong>0</strong> for 0 values; <strong>-1</strong> for negative.</p>
</span></td></tr>
<tr><td><code>sign(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Determines the sign of <code>val</code>: <strong>1</strong> for positive; <strong>0</strong> for 0 values; <strong>-1</strong> for negative.</p>
</span></td></tr>
<tr><td><code>sign(val: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Determines the sign of <code>val</code>: <strong>1</strong> for positive; <strong>0</strong> for 0 values; <strong>-1</strong> for negative.</p>
</span></td></tr>
<tr><td><code>sin(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the sine of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>sqrt(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the square root of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>sqrt(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the square root of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>tan(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the tangent of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>to_hex(val: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Converts <code>val</code> to its hexadecimal representation.</p>
</span></td></tr>
<tr><td><code>trunc(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Truncates the decimal values of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>trunc(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Truncates the decimal values of <code>val</code>.</p>
</span></td></tr></tbody>
</table>

### STRING[] Functions

<table>
<thead><tr><th>Function &rarr; Returns</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>json_object(texts: <a href="string.html">string</a>[]) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Builds a JSON or JSONB object out of a text array. The array must have exactly one dimension with an even number of members, in which case they are taken as alternating key/value pairs.</p>
</span></td></tr>
<tr><td><code>jsonb_object(texts: <a href="string.html">string</a>[]) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Builds a JSON or JSONB object out of a text array. The array must have exactly one dimension with an even number of members, in which case they are taken as alternating key/value pairs.</p>
</span></td></tr></tbody>
</table>

### Sequence Functions

<table>
<thead><tr><th>Function &rarr; Returns</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>currval(sequence_name: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Returns the latest value obtained with nextval for this sequence in this session.</p>
</span></td></tr>
<tr><td><code>lastval() &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Return value most recently obtained with nextval in this session.</p>
</span></td></tr>
<tr><td><code>nextval(sequence_name: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Advances the given sequence and returns its new value.</p>
</span></td></tr>
<tr><td><code>setval(sequence_name: <a href="string.html">string</a>, value: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Set the given sequence’s current value. The next call to nextval will return <code>value + Increment</code></p>
</span></td></tr>
<tr><td><code>setval(sequence_name: <a href="string.html">string</a>, value: <a href="int.html">int</a>, is_called: <a href="bool.html">bool</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Set the given sequence’s current value. If is_called is false, the next call to nextval will return <code>value</code>; otherwise <code>value + Increment</code>.</p>
</span></td></tr></tbody>
</table>

### String and Byte Functions

<table>
<thead><tr><th>Function &rarr; Returns</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>ascii(val: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the ASCII value for the first character in <code>val</code>.</p>
</span></td></tr>
<tr><td><code>btrim(input: <a href="string.html">string</a>, trim_chars: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Removes any characters included in <code>trim_chars</code> from the beginning or end of <code>input</code> (applies recursively).</p>
<p>For example, <code>btrim('doggie', 'eod')</code> returns <code>ggi</code>.</p>
</span></td></tr>
<tr><td><code>btrim(val: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Removes all spaces from the beginning and end of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>concat(<a href="string.html">string</a>...) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Concatenates a comma-separated list of strings.</p>
</span></td></tr>
<tr><td><code>concat_ws(<a href="string.html">string</a>...) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Uses the first argument as a separator between the concatenation of the subsequent arguments.</p>
<p>For example <code>concat_ws('!','wow','great')</code> returns <code>wow!great</code>.</p>
</span></td></tr>
<tr><td><code>decode(text: <a href="string.html">string</a>, format: <a href="string.html">string</a>) &rarr; <a href="bytes.html">bytes</a></code></td><td><span class="funcdesc"><p>Decodes <code>data</code> as the format specified by <code>format</code> (only “hex” and “escape” are supported).</p>
</span></td></tr>
<tr><td><code>encode(data: <a href="bytes.html">bytes</a>, format: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Encodes <code>data</code> in the text format specified by <code>format</code> (only “hex” and “escape” are supported).</p>
</span></td></tr>
<tr><td><code>from_ip(val: <a href="bytes.html">bytes</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Converts the byte string representation of an IP to its character string representation.</p>
</span></td></tr>
<tr><td><code>from_uuid(val: <a href="bytes.html">bytes</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Converts the byte string representation of a UUID to its character string representation.</p>
</span></td></tr>
<tr><td><code>initcap(val: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Capitalizes the first letter of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>left(input: <a href="bytes.html">bytes</a>, return_set: <a href="int.html">int</a>) &rarr; <a href="bytes.html">bytes</a></code></td><td><span class="funcdesc"><p>Returns the first <code>return_set</code> bytes from <code>input</code>.</p>
</span></td></tr>
<tr><td><code>left(input: <a href="string.html">string</a>, return_set: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns the first <code>return_set</code> characters from <code>input</code>.</p>
</span></td></tr>
<tr><td><code>length(val: <a href="bytes.html">bytes</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the number of bytes in <code>val</code>.</p>
</span></td></tr>
<tr><td><code>length(val: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the number of characters in <code>val</code>.</p>
</span></td></tr>
<tr><td><code>lower(val: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Converts all characters in <code>val</code> to their lower-case equivalents.</p>
</span></td></tr>
<tr><td><code>ltrim(input: <a href="string.html">string</a>, trim_chars: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Removes any characters included in <code>trim_chars</code> from the beginning (left-hand side) of <code>input</code> (applies recursively).</p>
<p>For example, <code>ltrim('doggie', 'od')</code> returns <code>ggie</code>.</p>
</span></td></tr>
<tr><td><code>ltrim(val: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Removes all spaces from the beginning (left-hand side) of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>md5(<a href="bytes.html">bytes</a>...) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Calculates the MD5 hash value of a set of values.</p>
</span></td></tr>
<tr><td><code>md5(<a href="string.html">string</a>...) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Calculates the MD5 hash value of a set of values.</p>
</span></td></tr>
<tr><td><code>octet_length(val: <a href="bytes.html">bytes</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the number of bytes in <code>val</code>.</p>
</span></td></tr>
<tr><td><code>octet_length(val: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the number of bytes used to represent <code>val</code>.</p>
</span></td></tr>
<tr><td><code>overlay(input: <a href="string.html">string</a>, overlay_val: <a href="string.html">string</a>, start_pos: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Replaces characters in <code>input</code> with <code>overlay_val</code> starting at <code>start_pos</code> (begins at 1).</p>
<p>For example, <code>overlay('doggie', 'CAT', 2)</code> returns <code>dCATie</code>.</p>
</span></td></tr>
<tr><td><code>overlay(input: <a href="string.html">string</a>, overlay_val: <a href="string.html">string</a>, start_pos: <a href="int.html">int</a>, end_pos: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Deletes the characters in <code>input</code> between <code>start_pos</code> and <code>end_pos</code> (count starts at 1), and then insert <code>overlay_val</code> at <code>start_pos</code>.</p>
</span></td></tr>
<tr><td><code>regexp_extract(input: <a href="string.html">string</a>, regex: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns the first match for the Regular Expression <code>regex</code> in <code>input</code>.</p>
</span></td></tr>
<tr><td><code>regexp_replace(input: <a href="string.html">string</a>, regex: <a href="string.html">string</a>, replace: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Replaces matches for the Regular Expression <code>regex</code> in <code>input</code> with the Regular Expression <code>replace</code>.</p>
</span></td></tr>
<tr><td><code>regexp_replace(input: <a href="string.html">string</a>, regex: <a href="string.html">string</a>, replace: <a href="string.html">string</a>, flags: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Replaces matches for the regular expression <code>regex</code> in <code>input</code> with the regular expression <code>replace</code> using <code>flags</code>.</p>
<p>CockroachDB supports the following flags:</p>
<table>
<thead>
<tr>
<th>Flag</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>c</strong></td>
<td>Case-sensitive matching</td>
</tr>
<tr>
<td><strong>i</strong></td>
<td>Global matching (match each substring instead of only the first).</td>
</tr>
<tr>
<td><strong>m</strong> or <strong>n</strong></td>
<td>Newline-sensitive (see below)</td>
</tr>
<tr>
<td><strong>p</strong></td>
<td>Partial newline-sensitive matching (see below)</td>
</tr>
<tr>
<td><strong>s</strong></td>
<td>Newline-insensitive (default)</td>
</tr>
<tr>
<td><strong>w</strong></td>
<td>Inverse partial newline-sensitive matching (see below)</td>
</tr>
</tbody>
</table>
<table>
<thead>
<tr>
<th>Mode</th>
<th><code>.</code> and <code>[^...]</code> match newlines</th>
<th><code>^</code> and <code>$</code> match line boundaries</th>
</tr>
</thead>
<tbody>
<tr>
<td>s</td>
<td>yes</td>
<td>no</td>
</tr>
<tr>
<td>w</td>
<td>yes</td>
<td>yes</td>
</tr>
<tr>
<td>p</td>
<td>no</td>
<td>no</td>
</tr>
<tr>
<td>m/n</td>
<td>no</td>
<td>yes</td>
</tr>
</tbody>
</table>
</span></td></tr>
<tr><td><code>repeat(input: <a href="string.html">string</a>, repeat_counter: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Concatenates <code>input</code> <code>repeat_counter</code> number of times.</p>
<p>For example, <code>repeat('dog', 2)</code> returns <code>dogdog</code>.</p>
</span></td></tr>
<tr><td><code>replace(input: <a href="string.html">string</a>, find: <a href="string.html">string</a>, replace: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Replaces all occurrences of <code>find</code> with <code>replace</code> in <code>input</code></p>
</span></td></tr>
<tr><td><code>reverse(val: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Reverses the order of the string’s characters.</p>
</span></td></tr>
<tr><td><code>right(input: <a href="bytes.html">bytes</a>, return_set: <a href="int.html">int</a>) &rarr; <a href="bytes.html">bytes</a></code></td><td><span class="funcdesc"><p>Returns the last <code>return_set</code> bytes from <code>input</code>.</p>
</span></td></tr>
<tr><td><code>right(input: <a href="string.html">string</a>, return_set: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns the last <code>return_set</code> characters from <code>input</code>.</p>
</span></td></tr>
<tr><td><code>rtrim(input: <a href="string.html">string</a>, trim_chars: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Removes any characters included in <code>trim_chars</code> from the end (right-hand side) of <code>input</code> (applies recursively).</p>
<p>For example, <code>rtrim('doggie', 'ei')</code> returns <code>dogg</code>.</p>
</span></td></tr>
<tr><td><code>rtrim(val: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Removes all spaces from the end (right-hand side) of <code>val</code>.</p>
</span></td></tr>
<tr><td><code>sha1(<a href="bytes.html">bytes</a>...) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Calculates the SHA1 hash value of a set of values.</p>
</span></td></tr>
<tr><td><code>sha1(<a href="string.html">string</a>...) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Calculates the SHA1 hash value of a set of values.</p>
</span></td></tr>
<tr><td><code>sha256(<a href="bytes.html">bytes</a>...) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Calculates the SHA256 hash value of a set of values.</p>
</span></td></tr>
<tr><td><code>sha256(<a href="string.html">string</a>...) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Calculates the SHA256 hash value of a set of values.</p>
</span></td></tr>
<tr><td><code>sha512(<a href="bytes.html">bytes</a>...) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Calculates the SHA512 hash value of a set of values.</p>
</span></td></tr>
<tr><td><code>sha512(<a href="string.html">string</a>...) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Calculates the SHA512 hash value of a set of values.</p>
</span></td></tr>
<tr><td><code>split_part(input: <a href="string.html">string</a>, delimiter: <a href="string.html">string</a>, return_index_pos: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Splits <code>input</code> on <code>delimiter</code> and return the value in the <code>return_index_pos</code>  position (starting at 1).</p>
<p>For example, <code>split_part('123.456.789.0','.',3)</code>returns <code>789</code>.</p>
</span></td></tr>
<tr><td><code>strpos(input: <a href="string.html">string</a>, find: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the position where the string <code>find</code> begins in <code>input</code>.</p>
<p>For example, <code>strpos('doggie', 'gie')</code> returns <code>4</code>.</p>
</span></td></tr>
<tr><td><code>substr(input: <a href="string.html">string</a>, regex: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns a substring of <code>input</code> that matches the regular expression <code>regex</code>.</p>
</span></td></tr>
<tr><td><code>substr(input: <a href="string.html">string</a>, regex: <a href="string.html">string</a>, escape_char: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns a substring of <code>input</code> that matches the regular expression <code>regex</code> using <code>escape_char</code> as your escape character instead of <code>\</code>.</p>
</span></td></tr>
<tr><td><code>substr(input: <a href="string.html">string</a>, start_pos: <a href="int.html">int</a>, end_pos: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns a substring of <code>input</code> between <code>start_pos</code> and <code>end_pos</code> (count starts at 1).</p>
</span></td></tr>
<tr><td><code>substr(input: <a href="string.html">string</a>, substr_pos: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns a substring of <code>input</code> starting at <code>substr_pos</code> (count starts at 1).</p>
</span></td></tr>
<tr><td><code>substring(input: <a href="string.html">string</a>, regex: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns a substring of <code>input</code> that matches the regular expression <code>regex</code>.</p>
</span></td></tr>
<tr><td><code>substring(input: <a href="string.html">string</a>, regex: <a href="string.html">string</a>, escape_char: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns a substring of <code>input</code> that matches the regular expression <code>regex</code> using <code>escape_char</code> as your escape character instead of <code>\</code>.</p>
</span></td></tr>
<tr><td><code>substring(input: <a href="string.html">string</a>, start_pos: <a href="int.html">int</a>, end_pos: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns a substring of <code>input</code> between <code>start_pos</code> and <code>end_pos</code> (count starts at 1).</p>
</span></td></tr>
<tr><td><code>substring(input: <a href="string.html">string</a>, substr_pos: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns a substring of <code>input</code> starting at <code>substr_pos</code> (count starts at 1).</p>
</span></td></tr>
<tr><td><code>to_english(val: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>This function enunciates the value of its argument using English cardinals.</p>
</span></td></tr>
<tr><td><code>to_hex(val: <a href="bytes.html">bytes</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Converts <code>val</code> to its hexadecimal representation.</p>
</span></td></tr>
<tr><td><code>to_ip(val: <a href="string.html">string</a>) &rarr; <a href="bytes.html">bytes</a></code></td><td><span class="funcdesc"><p>Converts the character string representation of an IP to its byte string representation.</p>
</span></td></tr>
<tr><td><code>to_uuid(val: <a href="string.html">string</a>) &rarr; <a href="bytes.html">bytes</a></code></td><td><span class="funcdesc"><p>Converts the character string representation of a UUID to its byte string representation.</p>
</span></td></tr>
<tr><td><code>translate(input: <a href="string.html">string</a>, find: <a href="string.html">string</a>, replace: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>In <code>input</code>, replaces the first character from <code>find</code> with the first character in <code>replace</code>; repeat for each character in <code>find</code>.</p>
<p>For example, <code>translate('doggie', 'dog', '123');</code> returns <code>1233ie</code>.</p>
</span></td></tr>
<tr><td><code>upper(val: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Converts all characters in <code>val</code> to their to their upper-case equivalents.</p>
</span></td></tr></tbody>
</table>

### System Info Functions

<table>
<thead><tr><th>Function &rarr; Returns</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>cluster_logical_timestamp() &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>This function is used only by CockroachDB’s developers for testing purposes.</p>
</span></td></tr>
<tr><td><code>crdb_internal.cluster_id() &rarr; <a href="uuid.html">uuid</a></code></td><td><span class="funcdesc"><p>Returns the cluster ID.</p>
</span></td></tr>
<tr><td><code>crdb_internal.force_error(errorCode: <a href="string.html">string</a>, msg: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>This function is used only by CockroachDB’s developers for testing purposes.</p>
</span></td></tr>
<tr><td><code>crdb_internal.force_log_fatal(msg: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>This function is used only by CockroachDB’s developers for testing purposes.</p>
</span></td></tr>
<tr><td><code>crdb_internal.force_panic(msg: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>This function is used only by CockroachDB’s developers for testing purposes.</p>
</span></td></tr>
<tr><td><code>crdb_internal.force_retry(val: <a href="interval.html">interval</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>This function is used only by CockroachDB’s developers for testing purposes.</p>
</span></td></tr>
<tr><td><code>crdb_internal.no_constant_folding(input: anyelement) &rarr; anyelement</code></td><td><span class="funcdesc"><p>This function is used only by CockroachDB’s developers for testing purposes.</p>
</span></td></tr>
<tr><td><code>crdb_internal.node_executable_version() &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns the version of CockroachDB this node is running.</p>
</span></td></tr>
<tr><td><code>crdb_internal.set_vmodule(vmodule_string: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>This function is used for internal debugging purposes. Incorrect use can severely impact performance.</p>
</span></td></tr>
<tr><td><code>current_database() &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns the current database.</p>
</span></td></tr>
<tr><td><code>current_schema() &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns the current schema. This function is provided for compatibility with PostgreSQL. For a new CockroachDB application, consider using current_database() instead.</p>
</span></td></tr>
<tr><td><code>current_schemas(include_pg_catalog: <a href="bool.html">bool</a>) &rarr; <a href="string.html">string</a>[]</code></td><td><span class="funcdesc"><p>Returns the current search path for unqualified names.</p>
</span></td></tr>
<tr><td><code>current_user() &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns the current user. This function is provided for compatibility with PostgreSQL.</p>
</span></td></tr>
<tr><td><code>version() &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns the node’s version of CockroachDB.</p>
</span></td></tr></tbody>
</table>

### Compatibility Functions

<table>
<thead><tr><th>Function &rarr; Returns</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>crdb_internal.unary_table() &rarr; setof tuple{}</code></td><td><span class="funcdesc"><p>Produces a virtual table containing a single row with no values.</p>
<p>This function is used only by CockroachDB’s developers for testing purposes.</p>
</span></td></tr>
<tr><td><code>format_type(type_oid: oid, typemod: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns the SQL name of a data type that is identified by its type OID and possibly a type modifier. Currently, the type modifier is ignored.</p>
</span></td></tr>
<tr><td><code>generate_series(start: <a href="int.html">int</a>, end: <a href="int.html">int</a>) &rarr; setof tuple{int}</code></td><td><span class="funcdesc"><p>Produces a virtual table containing the integer values from <code>start</code> to <code>end</code>, inclusive.</p>
</span></td></tr>
<tr><td><code>generate_series(start: <a href="int.html">int</a>, end: <a href="int.html">int</a>, step: <a href="int.html">int</a>) &rarr; setof tuple{int}</code></td><td><span class="funcdesc"><p>Produces a virtual table containing the integer values from <code>start</code> to <code>end</code>, inclusive, by increment of <code>step</code>.</p>
</span></td></tr>
<tr><td><code>generate_series(start: <a href="timestamp.html">timestamp</a>, end: <a href="timestamp.html">timestamp</a>, step: <a href="interval.html">interval</a>) &rarr; setof tuple{timestamp}</code></td><td><span class="funcdesc"><p>Produces a virtual table containing the timestamp values from <code>start</code> to <code>end</code>, inclusive, by increment of <code>step</code>.</p>
</span></td></tr>
<tr><td><code>has_any_column_privilege(table: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for any column of table.</p>
</span></td></tr>
<tr><td><code>has_any_column_privilege(table: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for any column of table.</p>
</span></td></tr>
<tr><td><code>has_any_column_privilege(user: <a href="string.html">string</a>, table: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for any column of table.</p>
</span></td></tr>
<tr><td><code>has_any_column_privilege(user: <a href="string.html">string</a>, table: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for any column of table.</p>
</span></td></tr>
<tr><td><code>has_any_column_privilege(user: oid, table: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for any column of table.</p>
</span></td></tr>
<tr><td><code>has_any_column_privilege(user: oid, table: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for any column of table.</p>
</span></td></tr>
<tr><td><code>has_column_privilege(table: <a href="string.html">string</a>, column: <a href="int.html">int</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for column.</p>
</span></td></tr>
<tr><td><code>has_column_privilege(table: <a href="string.html">string</a>, column: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for column.</p>
</span></td></tr>
<tr><td><code>has_column_privilege(table: oid, column: <a href="int.html">int</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for column.</p>
</span></td></tr>
<tr><td><code>has_column_privilege(table: oid, column: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for column.</p>
</span></td></tr>
<tr><td><code>has_column_privilege(user: <a href="string.html">string</a>, table: <a href="string.html">string</a>, column: <a href="int.html">int</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for column.</p>
</span></td></tr>
<tr><td><code>has_column_privilege(user: <a href="string.html">string</a>, table: <a href="string.html">string</a>, column: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for column.</p>
</span></td></tr>
<tr><td><code>has_column_privilege(user: <a href="string.html">string</a>, table: oid, column: <a href="int.html">int</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for column.</p>
</span></td></tr>
<tr><td><code>has_column_privilege(user: <a href="string.html">string</a>, table: oid, column: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for column.</p>
</span></td></tr>
<tr><td><code>has_column_privilege(user: oid, table: <a href="string.html">string</a>, column: <a href="int.html">int</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for column.</p>
</span></td></tr>
<tr><td><code>has_column_privilege(user: oid, table: <a href="string.html">string</a>, column: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for column.</p>
</span></td></tr>
<tr><td><code>has_column_privilege(user: oid, table: oid, column: <a href="int.html">int</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for column.</p>
</span></td></tr>
<tr><td><code>has_column_privilege(user: oid, table: oid, column: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for column.</p>
</span></td></tr>
<tr><td><code>has_database_privilege(database: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for database.</p>
</span></td></tr>
<tr><td><code>has_database_privilege(database: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for database.</p>
</span></td></tr>
<tr><td><code>has_database_privilege(user: <a href="string.html">string</a>, database: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for database.</p>
</span></td></tr>
<tr><td><code>has_database_privilege(user: <a href="string.html">string</a>, database: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for database.</p>
</span></td></tr>
<tr><td><code>has_database_privilege(user: oid, database: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for database.</p>
</span></td></tr>
<tr><td><code>has_database_privilege(user: oid, database: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for database.</p>
</span></td></tr>
<tr><td><code>has_foreign_data_wrapper_privilege(fdw: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for foreign-data wrapper.</p>
</span></td></tr>
<tr><td><code>has_foreign_data_wrapper_privilege(fdw: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for foreign-data wrapper.</p>
</span></td></tr>
<tr><td><code>has_foreign_data_wrapper_privilege(user: <a href="string.html">string</a>, fdw: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for foreign-data wrapper.</p>
</span></td></tr>
<tr><td><code>has_foreign_data_wrapper_privilege(user: <a href="string.html">string</a>, fdw: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for foreign-data wrapper.</p>
</span></td></tr>
<tr><td><code>has_foreign_data_wrapper_privilege(user: oid, fdw: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for foreign-data wrapper.</p>
</span></td></tr>
<tr><td><code>has_foreign_data_wrapper_privilege(user: oid, fdw: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for foreign-data wrapper.</p>
</span></td></tr>
<tr><td><code>has_function_privilege(function: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for function.</p>
</span></td></tr>
<tr><td><code>has_function_privilege(function: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for function.</p>
</span></td></tr>
<tr><td><code>has_function_privilege(user: <a href="string.html">string</a>, function: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for function.</p>
</span></td></tr>
<tr><td><code>has_function_privilege(user: <a href="string.html">string</a>, function: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for function.</p>
</span></td></tr>
<tr><td><code>has_function_privilege(user: oid, function: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for function.</p>
</span></td></tr>
<tr><td><code>has_function_privilege(user: oid, function: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for function.</p>
</span></td></tr>
<tr><td><code>has_language_privilege(language: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for language.</p>
</span></td></tr>
<tr><td><code>has_language_privilege(language: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for language.</p>
</span></td></tr>
<tr><td><code>has_language_privilege(user: <a href="string.html">string</a>, language: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for language.</p>
</span></td></tr>
<tr><td><code>has_language_privilege(user: <a href="string.html">string</a>, language: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for language.</p>
</span></td></tr>
<tr><td><code>has_language_privilege(user: oid, language: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for language.</p>
</span></td></tr>
<tr><td><code>has_language_privilege(user: oid, language: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for language.</p>
</span></td></tr>
<tr><td><code>has_schema_privilege(schema: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for schema.</p>
</span></td></tr>
<tr><td><code>has_schema_privilege(schema: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for schema.</p>
</span></td></tr>
<tr><td><code>has_schema_privilege(user: <a href="string.html">string</a>, schema: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for schema.</p>
</span></td></tr>
<tr><td><code>has_schema_privilege(user: <a href="string.html">string</a>, schema: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for schema.</p>
</span></td></tr>
<tr><td><code>has_schema_privilege(user: oid, schema: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for schema.</p>
</span></td></tr>
<tr><td><code>has_schema_privilege(user: oid, schema: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for schema.</p>
</span></td></tr>
<tr><td><code>has_sequence_privilege(sequence: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for sequence.</p>
</span></td></tr>
<tr><td><code>has_sequence_privilege(sequence: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for sequence.</p>
</span></td></tr>
<tr><td><code>has_sequence_privilege(user: <a href="string.html">string</a>, sequence: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for sequence.</p>
</span></td></tr>
<tr><td><code>has_sequence_privilege(user: <a href="string.html">string</a>, sequence: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for sequence.</p>
</span></td></tr>
<tr><td><code>has_sequence_privilege(user: oid, sequence: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for sequence.</p>
</span></td></tr>
<tr><td><code>has_sequence_privilege(user: oid, sequence: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for sequence.</p>
</span></td></tr>
<tr><td><code>has_server_privilege(server: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for foreign server.</p>
</span></td></tr>
<tr><td><code>has_server_privilege(server: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for foreign server.</p>
</span></td></tr>
<tr><td><code>has_server_privilege(user: <a href="string.html">string</a>, server: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for foreign server.</p>
</span></td></tr>
<tr><td><code>has_server_privilege(user: <a href="string.html">string</a>, server: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for foreign server.</p>
</span></td></tr>
<tr><td><code>has_server_privilege(user: oid, server: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for foreign server.</p>
</span></td></tr>
<tr><td><code>has_server_privilege(user: oid, server: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for foreign server.</p>
</span></td></tr>
<tr><td><code>has_table_privilege(table: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for table.</p>
</span></td></tr>
<tr><td><code>has_table_privilege(table: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for table.</p>
</span></td></tr>
<tr><td><code>has_table_privilege(user: <a href="string.html">string</a>, table: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for table.</p>
</span></td></tr>
<tr><td><code>has_table_privilege(user: <a href="string.html">string</a>, table: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for table.</p>
</span></td></tr>
<tr><td><code>has_table_privilege(user: oid, table: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for table.</p>
</span></td></tr>
<tr><td><code>has_table_privilege(user: oid, table: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for table.</p>
</span></td></tr>
<tr><td><code>has_tablespace_privilege(tablespace: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for tablespace.</p>
</span></td></tr>
<tr><td><code>has_tablespace_privilege(tablespace: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for tablespace.</p>
</span></td></tr>
<tr><td><code>has_tablespace_privilege(user: <a href="string.html">string</a>, tablespace: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for tablespace.</p>
</span></td></tr>
<tr><td><code>has_tablespace_privilege(user: <a href="string.html">string</a>, tablespace: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for tablespace.</p>
</span></td></tr>
<tr><td><code>has_tablespace_privilege(user: oid, tablespace: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for tablespace.</p>
</span></td></tr>
<tr><td><code>has_tablespace_privilege(user: oid, tablespace: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for tablespace.</p>
</span></td></tr>
<tr><td><code>has_type_privilege(type: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for type.</p>
</span></td></tr>
<tr><td><code>has_type_privilege(type: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the current user has privileges for type.</p>
</span></td></tr>
<tr><td><code>has_type_privilege(user: <a href="string.html">string</a>, type: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for type.</p>
</span></td></tr>
<tr><td><code>has_type_privilege(user: <a href="string.html">string</a>, type: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for type.</p>
</span></td></tr>
<tr><td><code>has_type_privilege(user: oid, type: <a href="string.html">string</a>, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for type.</p>
</span></td></tr>
<tr><td><code>has_type_privilege(user: oid, type: oid, privilege: <a href="string.html">string</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns whether or not the user has privileges for type.</p>
</span></td></tr>
<tr><td><code>information_schema._pg_expandarray(input: anyelement[]) &rarr; anyelement</code></td><td><span class="funcdesc"><p>Returns the input array as a set of rows with an index</p>
</span></td></tr>
<tr><td><code>json_array_elements(input: jsonb) &rarr; setof tuple{jsonb}</code></td><td><span class="funcdesc"><p>Expands a JSON array to a set of JSON values.</p>
</span></td></tr>
<tr><td><code>json_array_elements_text(input: jsonb) &rarr; setof tuple{string}</code></td><td><span class="funcdesc"><p>Expands a JSON array to a set of text values.</p>
</span></td></tr>
<tr><td><code>json_each(input: jsonb) &rarr; setof tuple{<a href="string.html">string</a>, jsonb}</code></td><td><span class="funcdesc"><p>Expands the outermost JSON or JSONB object into a set of key/value pairs.</p>
</span></td></tr>
<tr><td><code>json_each_text(input: jsonb) &rarr; setof tuple{<a href="string.html">string</a>, string}</code></td><td><span class="funcdesc"><p>Expands the outermost JSON or JSONB object into a set of key/value pairs. The returned values will be of type text.</p>
</span></td></tr>
<tr><td><code>json_object_keys(input: jsonb) &rarr; setof tuple{string}</code></td><td><span class="funcdesc"><p>Returns sorted set of keys in the outermost JSON object.</p>
</span></td></tr>
<tr><td><code>jsonb_array_elements(input: jsonb) &rarr; setof tuple{jsonb}</code></td><td><span class="funcdesc"><p>Expands a JSON array to a set of JSON values.</p>
</span></td></tr>
<tr><td><code>jsonb_array_elements_text(input: jsonb) &rarr; setof tuple{string}</code></td><td><span class="funcdesc"><p>Expands a JSON array to a set of text values.</p>
</span></td></tr>
<tr><td><code>jsonb_each(input: jsonb) &rarr; setof tuple{<a href="string.html">string</a>, jsonb}</code></td><td><span class="funcdesc"><p>Expands the outermost JSON or JSONB object into a set of key/value pairs.</p>
</span></td></tr>
<tr><td><code>jsonb_each_text(input: jsonb) &rarr; setof tuple{<a href="string.html">string</a>, string}</code></td><td><span class="funcdesc"><p>Expands the outermost JSON or JSONB object into a set of key/value pairs. The returned values will be of type text.</p>
</span></td></tr>
<tr><td><code>jsonb_object_keys(input: jsonb) &rarr; setof tuple{string}</code></td><td><span class="funcdesc"><p>Returns sorted set of keys in the outermost JSON object.</p>
</span></td></tr>
<tr><td><code>oid(int: <a href="int.html">int</a>) &rarr; oid</code></td><td><span class="funcdesc"><p>Converts an integer to an OID.</p>
</span></td></tr>
<tr><td><code>pg_get_keywords() &rarr; setof tuple{<a href="string.html">string</a>, <a href="string.html">string</a>, string}</code></td><td><span class="funcdesc"><p>Produces a virtual table containing the keywords known to the SQL parser.</p>
</span></td></tr>
<tr><td><code>pg_sleep(seconds: <a href="float.html">float</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>pg_sleep makes the current session’s process sleep until seconds seconds have elapsed. seconds is a value of type double precision, so fractional-second delays can be specified.</p>
</span></td></tr>
<tr><td><code>unnest(input: anyelement[]) &rarr; anyelement</code></td><td><span class="funcdesc"><p>Returns the input array as a set of rows</p>
</span></td></tr></tbody>
</table>

