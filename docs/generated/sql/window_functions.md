<table>
<thead><tr><th>Function &rarr; Returns</th><th>Description</th></tr></thead>
<tbody>
<tr><td><a name="cume_dist"></a><code>cume_dist() &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the relative rank of the current row: (number of rows preceding or peer with current row) / (total rows).</p>
</span></td></tr>
<tr><td><a name="dense_rank"></a><code>dense_rank() &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the rank of the current row without gaps; this function counts peer groups.</p>
</span></td></tr>
<tr><td><a name="first_value"></a><code>first_value(val: <a href="bool.html">bool</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the first row of the window frame.</p>
</span></td></tr>
<tr><td><a name="first_value"></a><code>first_value(val: <a href="bytes.html">bytes</a>) &rarr; <a href="bytes.html">bytes</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the first row of the window frame.</p>
</span></td></tr>
<tr><td><a name="first_value"></a><code>first_value(val: <a href="date.html">date</a>) &rarr; <a href="date.html">date</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the first row of the window frame.</p>
</span></td></tr>
<tr><td><a name="first_value"></a><code>first_value(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the first row of the window frame.</p>
</span></td></tr>
<tr><td><a name="first_value"></a><code>first_value(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the first row of the window frame.</p>
</span></td></tr>
<tr><td><a name="first_value"></a><code>first_value(val: <a href="inet.html">inet</a>) &rarr; <a href="inet.html">inet</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the first row of the window frame.</p>
</span></td></tr>
<tr><td><a name="first_value"></a><code>first_value(val: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the first row of the window frame.</p>
</span></td></tr>
<tr><td><a name="first_value"></a><code>first_value(val: <a href="interval.html">interval</a>) &rarr; <a href="interval.html">interval</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the first row of the window frame.</p>
</span></td></tr>
<tr><td><a name="first_value"></a><code>first_value(val: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the first row of the window frame.</p>
</span></td></tr>
<tr><td><a name="first_value"></a><code>first_value(val: <a href="time.html">time</a>) &rarr; <a href="time.html">time</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the first row of the window frame.</p>
</span></td></tr>
<tr><td><a name="first_value"></a><code>first_value(val: <a href="timestamp.html">timestamp</a>) &rarr; <a href="timestamp.html">timestamp</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the first row of the window frame.</p>
</span></td></tr>
<tr><td><a name="first_value"></a><code>first_value(val: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="timestamp.html">timestamptz</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the first row of the window frame.</p>
</span></td></tr>
<tr><td><a name="first_value"></a><code>first_value(val: <a href="uuid.html">uuid</a>) &rarr; <a href="uuid.html">uuid</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the first row of the window frame.</p>
</span></td></tr>
<tr><td><a name="first_value"></a><code>first_value(val: box2d) &rarr; box2d</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the first row of the window frame.</p>
</span></td></tr>
<tr><td><a name="first_value"></a><code>first_value(val: geography) &rarr; geography</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the first row of the window frame.</p>
</span></td></tr>
<tr><td><a name="first_value"></a><code>first_value(val: geometry) &rarr; geometry</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the first row of the window frame.</p>
</span></td></tr>
<tr><td><a name="first_value"></a><code>first_value(val: jsonb) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the first row of the window frame.</p>
</span></td></tr>
<tr><td><a name="first_value"></a><code>first_value(val: oid) &rarr; oid</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the first row of the window frame.</p>
</span></td></tr>
<tr><td><a name="first_value"></a><code>first_value(val: timetz) &rarr; timetz</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the first row of the window frame.</p>
</span></td></tr>
<tr><td><a name="first_value"></a><code>first_value(val: varbit) &rarr; varbit</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the first row of the window frame.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="bool.html">bool</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the previous row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="bool.html">bool</a>, n: <a href="int.html">int</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="bool.html">bool</a>, n: <a href="int.html">int</a>, default: <a href="bool.html">bool</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="bytes.html">bytes</a>) &rarr; <a href="bytes.html">bytes</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the previous row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="bytes.html">bytes</a>, n: <a href="int.html">int</a>) &rarr; <a href="bytes.html">bytes</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="bytes.html">bytes</a>, n: <a href="int.html">int</a>, default: <a href="bytes.html">bytes</a>) &rarr; <a href="bytes.html">bytes</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="date.html">date</a>) &rarr; <a href="date.html">date</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the previous row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="date.html">date</a>, n: <a href="int.html">int</a>) &rarr; <a href="date.html">date</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="date.html">date</a>, n: <a href="int.html">int</a>, default: <a href="date.html">date</a>) &rarr; <a href="date.html">date</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the previous row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="decimal.html">decimal</a>, n: <a href="int.html">int</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="decimal.html">decimal</a>, n: <a href="int.html">int</a>, default: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the previous row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="float.html">float</a>, n: <a href="int.html">int</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="float.html">float</a>, n: <a href="int.html">int</a>, default: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="inet.html">inet</a>) &rarr; <a href="inet.html">inet</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the previous row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="inet.html">inet</a>, n: <a href="int.html">int</a>) &rarr; <a href="inet.html">inet</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="inet.html">inet</a>, n: <a href="int.html">int</a>, default: <a href="inet.html">inet</a>) &rarr; <a href="inet.html">inet</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the previous row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="int.html">int</a>, n: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="int.html">int</a>, n: <a href="int.html">int</a>, default: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="interval.html">interval</a>) &rarr; <a href="interval.html">interval</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the previous row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="interval.html">interval</a>, n: <a href="int.html">int</a>) &rarr; <a href="interval.html">interval</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="interval.html">interval</a>, n: <a href="int.html">int</a>, default: <a href="interval.html">interval</a>) &rarr; <a href="interval.html">interval</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the previous row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="string.html">string</a>, n: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="string.html">string</a>, n: <a href="int.html">int</a>, default: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="time.html">time</a>) &rarr; <a href="time.html">time</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the previous row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="time.html">time</a>, n: <a href="int.html">int</a>) &rarr; <a href="time.html">time</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="time.html">time</a>, n: <a href="int.html">int</a>, default: <a href="time.html">time</a>) &rarr; <a href="time.html">time</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="timestamp.html">timestamp</a>) &rarr; <a href="timestamp.html">timestamp</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the previous row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="timestamp.html">timestamp</a>, n: <a href="int.html">int</a>) &rarr; <a href="timestamp.html">timestamp</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="timestamp.html">timestamp</a>, n: <a href="int.html">int</a>, default: <a href="timestamp.html">timestamp</a>) &rarr; <a href="timestamp.html">timestamp</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="timestamp.html">timestamptz</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the previous row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="timestamp.html">timestamptz</a>, n: <a href="int.html">int</a>) &rarr; <a href="timestamp.html">timestamptz</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="timestamp.html">timestamptz</a>, n: <a href="int.html">int</a>, default: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="timestamp.html">timestamptz</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="uuid.html">uuid</a>) &rarr; <a href="uuid.html">uuid</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the previous row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="uuid.html">uuid</a>, n: <a href="int.html">int</a>) &rarr; <a href="uuid.html">uuid</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: <a href="uuid.html">uuid</a>, n: <a href="int.html">int</a>, default: <a href="uuid.html">uuid</a>) &rarr; <a href="uuid.html">uuid</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: box2d) &rarr; box2d</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the previous row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: box2d, n: <a href="int.html">int</a>) &rarr; box2d</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: box2d, n: <a href="int.html">int</a>, default: box2d) &rarr; box2d</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: geography) &rarr; geography</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the previous row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: geography, n: <a href="int.html">int</a>) &rarr; geography</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: geography, n: <a href="int.html">int</a>, default: geography) &rarr; geography</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: geometry) &rarr; geometry</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the previous row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: geometry, n: <a href="int.html">int</a>) &rarr; geometry</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: geometry, n: <a href="int.html">int</a>, default: geometry) &rarr; geometry</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: jsonb) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the previous row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: jsonb, n: <a href="int.html">int</a>) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: jsonb, n: <a href="int.html">int</a>, default: jsonb) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: oid) &rarr; oid</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the previous row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: oid, n: <a href="int.html">int</a>) &rarr; oid</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: oid, n: <a href="int.html">int</a>, default: oid) &rarr; oid</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: timetz) &rarr; timetz</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the previous row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: timetz, n: <a href="int.html">int</a>) &rarr; timetz</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: timetz, n: <a href="int.html">int</a>, default: timetz) &rarr; timetz</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: varbit) &rarr; varbit</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the previous row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: varbit, n: <a href="int.html">int</a>) &rarr; varbit</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lag"></a><code>lag(val: varbit, n: <a href="int.html">int</a>, default: varbit) &rarr; varbit</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows before the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="last_value"></a><code>last_value(val: <a href="bool.html">bool</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the last row of the window frame.</p>
</span></td></tr>
<tr><td><a name="last_value"></a><code>last_value(val: <a href="bytes.html">bytes</a>) &rarr; <a href="bytes.html">bytes</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the last row of the window frame.</p>
</span></td></tr>
<tr><td><a name="last_value"></a><code>last_value(val: <a href="date.html">date</a>) &rarr; <a href="date.html">date</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the last row of the window frame.</p>
</span></td></tr>
<tr><td><a name="last_value"></a><code>last_value(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the last row of the window frame.</p>
</span></td></tr>
<tr><td><a name="last_value"></a><code>last_value(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the last row of the window frame.</p>
</span></td></tr>
<tr><td><a name="last_value"></a><code>last_value(val: <a href="inet.html">inet</a>) &rarr; <a href="inet.html">inet</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the last row of the window frame.</p>
</span></td></tr>
<tr><td><a name="last_value"></a><code>last_value(val: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the last row of the window frame.</p>
</span></td></tr>
<tr><td><a name="last_value"></a><code>last_value(val: <a href="interval.html">interval</a>) &rarr; <a href="interval.html">interval</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the last row of the window frame.</p>
</span></td></tr>
<tr><td><a name="last_value"></a><code>last_value(val: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the last row of the window frame.</p>
</span></td></tr>
<tr><td><a name="last_value"></a><code>last_value(val: <a href="time.html">time</a>) &rarr; <a href="time.html">time</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the last row of the window frame.</p>
</span></td></tr>
<tr><td><a name="last_value"></a><code>last_value(val: <a href="timestamp.html">timestamp</a>) &rarr; <a href="timestamp.html">timestamp</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the last row of the window frame.</p>
</span></td></tr>
<tr><td><a name="last_value"></a><code>last_value(val: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="timestamp.html">timestamptz</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the last row of the window frame.</p>
</span></td></tr>
<tr><td><a name="last_value"></a><code>last_value(val: <a href="uuid.html">uuid</a>) &rarr; <a href="uuid.html">uuid</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the last row of the window frame.</p>
</span></td></tr>
<tr><td><a name="last_value"></a><code>last_value(val: box2d) &rarr; box2d</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the last row of the window frame.</p>
</span></td></tr>
<tr><td><a name="last_value"></a><code>last_value(val: geography) &rarr; geography</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the last row of the window frame.</p>
</span></td></tr>
<tr><td><a name="last_value"></a><code>last_value(val: geometry) &rarr; geometry</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the last row of the window frame.</p>
</span></td></tr>
<tr><td><a name="last_value"></a><code>last_value(val: jsonb) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the last row of the window frame.</p>
</span></td></tr>
<tr><td><a name="last_value"></a><code>last_value(val: oid) &rarr; oid</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the last row of the window frame.</p>
</span></td></tr>
<tr><td><a name="last_value"></a><code>last_value(val: timetz) &rarr; timetz</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the last row of the window frame.</p>
</span></td></tr>
<tr><td><a name="last_value"></a><code>last_value(val: varbit) &rarr; varbit</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the last row of the window frame.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="bool.html">bool</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the following row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="bool.html">bool</a>, n: <a href="int.html">int</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="bool.html">bool</a>, n: <a href="int.html">int</a>, default: <a href="bool.html">bool</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="bytes.html">bytes</a>) &rarr; <a href="bytes.html">bytes</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the following row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="bytes.html">bytes</a>, n: <a href="int.html">int</a>) &rarr; <a href="bytes.html">bytes</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="bytes.html">bytes</a>, n: <a href="int.html">int</a>, default: <a href="bytes.html">bytes</a>) &rarr; <a href="bytes.html">bytes</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="date.html">date</a>) &rarr; <a href="date.html">date</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the following row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="date.html">date</a>, n: <a href="int.html">int</a>) &rarr; <a href="date.html">date</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="date.html">date</a>, n: <a href="int.html">int</a>, default: <a href="date.html">date</a>) &rarr; <a href="date.html">date</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the following row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="decimal.html">decimal</a>, n: <a href="int.html">int</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="decimal.html">decimal</a>, n: <a href="int.html">int</a>, default: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the following row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="float.html">float</a>, n: <a href="int.html">int</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="float.html">float</a>, n: <a href="int.html">int</a>, default: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="inet.html">inet</a>) &rarr; <a href="inet.html">inet</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the following row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="inet.html">inet</a>, n: <a href="int.html">int</a>) &rarr; <a href="inet.html">inet</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="inet.html">inet</a>, n: <a href="int.html">int</a>, default: <a href="inet.html">inet</a>) &rarr; <a href="inet.html">inet</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the following row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="int.html">int</a>, n: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="int.html">int</a>, n: <a href="int.html">int</a>, default: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="interval.html">interval</a>) &rarr; <a href="interval.html">interval</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the following row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="interval.html">interval</a>, n: <a href="int.html">int</a>) &rarr; <a href="interval.html">interval</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="interval.html">interval</a>, n: <a href="int.html">int</a>, default: <a href="interval.html">interval</a>) &rarr; <a href="interval.html">interval</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the following row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="string.html">string</a>, n: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="string.html">string</a>, n: <a href="int.html">int</a>, default: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="time.html">time</a>) &rarr; <a href="time.html">time</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the following row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="time.html">time</a>, n: <a href="int.html">int</a>) &rarr; <a href="time.html">time</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="time.html">time</a>, n: <a href="int.html">int</a>, default: <a href="time.html">time</a>) &rarr; <a href="time.html">time</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="timestamp.html">timestamp</a>) &rarr; <a href="timestamp.html">timestamp</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the following row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="timestamp.html">timestamp</a>, n: <a href="int.html">int</a>) &rarr; <a href="timestamp.html">timestamp</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="timestamp.html">timestamp</a>, n: <a href="int.html">int</a>, default: <a href="timestamp.html">timestamp</a>) &rarr; <a href="timestamp.html">timestamp</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="timestamp.html">timestamptz</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the following row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="timestamp.html">timestamptz</a>, n: <a href="int.html">int</a>) &rarr; <a href="timestamp.html">timestamptz</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="timestamp.html">timestamptz</a>, n: <a href="int.html">int</a>, default: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="timestamp.html">timestamptz</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="uuid.html">uuid</a>) &rarr; <a href="uuid.html">uuid</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the following row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="uuid.html">uuid</a>, n: <a href="int.html">int</a>) &rarr; <a href="uuid.html">uuid</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: <a href="uuid.html">uuid</a>, n: <a href="int.html">int</a>, default: <a href="uuid.html">uuid</a>) &rarr; <a href="uuid.html">uuid</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: box2d) &rarr; box2d</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the following row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: box2d, n: <a href="int.html">int</a>) &rarr; box2d</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: box2d, n: <a href="int.html">int</a>, default: box2d) &rarr; box2d</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: geography) &rarr; geography</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the following row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: geography, n: <a href="int.html">int</a>) &rarr; geography</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: geography, n: <a href="int.html">int</a>, default: geography) &rarr; geography</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: geometry) &rarr; geometry</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the following row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: geometry, n: <a href="int.html">int</a>) &rarr; geometry</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: geometry, n: <a href="int.html">int</a>, default: geometry) &rarr; geometry</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: jsonb) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the following row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: jsonb, n: <a href="int.html">int</a>) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: jsonb, n: <a href="int.html">int</a>, default: jsonb) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: oid) &rarr; oid</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the following row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: oid, n: <a href="int.html">int</a>) &rarr; oid</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: oid, n: <a href="int.html">int</a>, default: oid) &rarr; oid</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: timetz) &rarr; timetz</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the following row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: timetz, n: <a href="int.html">int</a>) &rarr; timetz</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: timetz, n: <a href="int.html">int</a>, default: timetz) &rarr; timetz</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: varbit) &rarr; varbit</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the following row within current row’s partition; if there is no such row, instead returns null.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: varbit, n: <a href="int.html">int</a>) &rarr; varbit</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such row, instead returns null. <code>n</code> is evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="lead"></a><code>lead(val: varbit, n: <a href="int.html">int</a>, default: varbit) &rarr; varbit</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is <code>n</code> rows after the current row within its partition; if there is no such, row, instead returns <code>default</code> (which must be of the same type as <code>val</code>). Both <code>n</code> and <code>default</code> are evaluated with respect to the current row.</p>
</span></td></tr>
<tr><td><a name="nth_value"></a><code>nth_value(val: <a href="bool.html">bool</a>, n: <a href="int.html">int</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the <code>n</code>th row of the window frame (counting from 1); null if no such row.</p>
</span></td></tr>
<tr><td><a name="nth_value"></a><code>nth_value(val: <a href="bytes.html">bytes</a>, n: <a href="int.html">int</a>) &rarr; <a href="bytes.html">bytes</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the <code>n</code>th row of the window frame (counting from 1); null if no such row.</p>
</span></td></tr>
<tr><td><a name="nth_value"></a><code>nth_value(val: <a href="date.html">date</a>, n: <a href="int.html">int</a>) &rarr; <a href="date.html">date</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the <code>n</code>th row of the window frame (counting from 1); null if no such row.</p>
</span></td></tr>
<tr><td><a name="nth_value"></a><code>nth_value(val: <a href="decimal.html">decimal</a>, n: <a href="int.html">int</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the <code>n</code>th row of the window frame (counting from 1); null if no such row.</p>
</span></td></tr>
<tr><td><a name="nth_value"></a><code>nth_value(val: <a href="float.html">float</a>, n: <a href="int.html">int</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the <code>n</code>th row of the window frame (counting from 1); null if no such row.</p>
</span></td></tr>
<tr><td><a name="nth_value"></a><code>nth_value(val: <a href="inet.html">inet</a>, n: <a href="int.html">int</a>) &rarr; <a href="inet.html">inet</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the <code>n</code>th row of the window frame (counting from 1); null if no such row.</p>
</span></td></tr>
<tr><td><a name="nth_value"></a><code>nth_value(val: <a href="int.html">int</a>, n: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the <code>n</code>th row of the window frame (counting from 1); null if no such row.</p>
</span></td></tr>
<tr><td><a name="nth_value"></a><code>nth_value(val: <a href="interval.html">interval</a>, n: <a href="int.html">int</a>) &rarr; <a href="interval.html">interval</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the <code>n</code>th row of the window frame (counting from 1); null if no such row.</p>
</span></td></tr>
<tr><td><a name="nth_value"></a><code>nth_value(val: <a href="string.html">string</a>, n: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the <code>n</code>th row of the window frame (counting from 1); null if no such row.</p>
</span></td></tr>
<tr><td><a name="nth_value"></a><code>nth_value(val: <a href="time.html">time</a>, n: <a href="int.html">int</a>) &rarr; <a href="time.html">time</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the <code>n</code>th row of the window frame (counting from 1); null if no such row.</p>
</span></td></tr>
<tr><td><a name="nth_value"></a><code>nth_value(val: <a href="timestamp.html">timestamp</a>, n: <a href="int.html">int</a>) &rarr; <a href="timestamp.html">timestamp</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the <code>n</code>th row of the window frame (counting from 1); null if no such row.</p>
</span></td></tr>
<tr><td><a name="nth_value"></a><code>nth_value(val: <a href="timestamp.html">timestamptz</a>, n: <a href="int.html">int</a>) &rarr; <a href="timestamp.html">timestamptz</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the <code>n</code>th row of the window frame (counting from 1); null if no such row.</p>
</span></td></tr>
<tr><td><a name="nth_value"></a><code>nth_value(val: <a href="uuid.html">uuid</a>, n: <a href="int.html">int</a>) &rarr; <a href="uuid.html">uuid</a></code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the <code>n</code>th row of the window frame (counting from 1); null if no such row.</p>
</span></td></tr>
<tr><td><a name="nth_value"></a><code>nth_value(val: box2d, n: <a href="int.html">int</a>) &rarr; box2d</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the <code>n</code>th row of the window frame (counting from 1); null if no such row.</p>
</span></td></tr>
<tr><td><a name="nth_value"></a><code>nth_value(val: geography, n: <a href="int.html">int</a>) &rarr; geography</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the <code>n</code>th row of the window frame (counting from 1); null if no such row.</p>
</span></td></tr>
<tr><td><a name="nth_value"></a><code>nth_value(val: geometry, n: <a href="int.html">int</a>) &rarr; geometry</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the <code>n</code>th row of the window frame (counting from 1); null if no such row.</p>
</span></td></tr>
<tr><td><a name="nth_value"></a><code>nth_value(val: jsonb, n: <a href="int.html">int</a>) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the <code>n</code>th row of the window frame (counting from 1); null if no such row.</p>
</span></td></tr>
<tr><td><a name="nth_value"></a><code>nth_value(val: oid, n: <a href="int.html">int</a>) &rarr; oid</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the <code>n</code>th row of the window frame (counting from 1); null if no such row.</p>
</span></td></tr>
<tr><td><a name="nth_value"></a><code>nth_value(val: timetz, n: <a href="int.html">int</a>) &rarr; timetz</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the <code>n</code>th row of the window frame (counting from 1); null if no such row.</p>
</span></td></tr>
<tr><td><a name="nth_value"></a><code>nth_value(val: varbit, n: <a href="int.html">int</a>) &rarr; varbit</code></td><td><span class="funcdesc"><p>Returns <code>val</code> evaluated at the row that is the <code>n</code>th row of the window frame (counting from 1); null if no such row.</p>
</span></td></tr>
<tr><td><a name="ntile"></a><code>ntile(n: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates an integer ranging from 1 to <code>n</code>, dividing the partition as equally as possible.</p>
</span></td></tr>
<tr><td><a name="percent_rank"></a><code>percent_rank() &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the relative rank of the current row: (rank - 1) / (total rows - 1).</p>
</span></td></tr>
<tr><td><a name="rank"></a><code>rank() &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the rank of the current row with gaps; same as row_number of its first peer.</p>
</span></td></tr>
<tr><td><a name="row_number"></a><code>row_number() &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the number of the current row within its partition, counting from 1.</p>
</span></td></tr></tbody>
</table>

