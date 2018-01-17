<table>
<thead><tr><th>Function &rarr; Returns</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>array_agg(arg1: <a href="bool.html">bool</a>) &rarr; <a href="bool.html">bool</a>[]</code></td><td><span class="funcdesc"><p>Aggregates the selected values into an array.</p>
</span></td></tr>
<tr><td><code>array_agg(arg1: <a href="bytes.html">bytes</a>) &rarr; <a href="bytes.html">bytes</a>[]</code></td><td><span class="funcdesc"><p>Aggregates the selected values into an array.</p>
</span></td></tr>
<tr><td><code>array_agg(arg1: <a href="date.html">date</a>) &rarr; <a href="date.html">date</a>[]</code></td><td><span class="funcdesc"><p>Aggregates the selected values into an array.</p>
</span></td></tr>
<tr><td><code>array_agg(arg1: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a>[]</code></td><td><span class="funcdesc"><p>Aggregates the selected values into an array.</p>
</span></td></tr>
<tr><td><code>array_agg(arg1: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a>[]</code></td><td><span class="funcdesc"><p>Aggregates the selected values into an array.</p>
</span></td></tr>
<tr><td><code>array_agg(arg1: <a href="inet.html">inet</a>) &rarr; <a href="inet.html">inet</a>[]</code></td><td><span class="funcdesc"><p>Aggregates the selected values into an array.</p>
</span></td></tr>
<tr><td><code>array_agg(arg1: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a>[]</code></td><td><span class="funcdesc"><p>Aggregates the selected values into an array.</p>
</span></td></tr>
<tr><td><code>array_agg(arg1: <a href="interval.html">interval</a>) &rarr; <a href="interval.html">interval</a>[]</code></td><td><span class="funcdesc"><p>Aggregates the selected values into an array.</p>
</span></td></tr>
<tr><td><code>array_agg(arg1: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a>[]</code></td><td><span class="funcdesc"><p>Aggregates the selected values into an array.</p>
</span></td></tr>
<tr><td><code>array_agg(arg1: <a href="time.html">time</a>) &rarr; <a href="time.html">time</a>[]</code></td><td><span class="funcdesc"><p>Aggregates the selected values into an array.</p>
</span></td></tr>
<tr><td><code>array_agg(arg1: <a href="timestamp.html">timestamp</a>) &rarr; <a href="timestamp.html">timestamp</a>[]</code></td><td><span class="funcdesc"><p>Aggregates the selected values into an array.</p>
</span></td></tr>
<tr><td><code>array_agg(arg1: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="timestamp.html">timestamptz</a>[]</code></td><td><span class="funcdesc"><p>Aggregates the selected values into an array.</p>
</span></td></tr>
<tr><td><code>array_agg(arg1: <a href="uuid.html">uuid</a>) &rarr; <a href="uuid.html">uuid</a>[]</code></td><td><span class="funcdesc"><p>Aggregates the selected values into an array.</p>
</span></td></tr>
<tr><td><code>array_agg(arg1: oid) &rarr; oid[]</code></td><td><span class="funcdesc"><p>Aggregates the selected values into an array.</p>
</span></td></tr>
<tr><td><code>avg(arg1: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the average of the selected values.</p>
</span></td></tr>
<tr><td><code>avg(arg1: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the average of the selected values.</p>
</span></td></tr>
<tr><td><code>avg(arg1: <a href="int.html">int</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the average of the selected values.</p>
</span></td></tr>
<tr><td><code>bool_and(arg1: <a href="bool.html">bool</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Calculates the boolean value of <code>AND</code>ing all selected values.</p>
</span></td></tr>
<tr><td><code>bool_or(arg1: <a href="bool.html">bool</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Calculates the boolean value of <code>OR</code>ing all selected values.</p>
</span></td></tr>
<tr><td><code>concat_agg(arg1: <a href="bytes.html">bytes</a>) &rarr; <a href="bytes.html">bytes</a></code></td><td><span class="funcdesc"><p>Concatenates all selected values.</p>
</span></td></tr>
<tr><td><code>concat_agg(arg1: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Concatenates all selected values.</p>
</span></td></tr>
<tr><td><code>count(arg1: anyelement) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the number of selected elements.</p>
</span></td></tr>
<tr><td><code>count_rows() &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the number of rows.</p>
</span></td></tr>
<tr><td><code>final_stddev(arg1: <a href="decimal.html">decimal</a>, arg2: <a href="decimal.html">decimal</a>, arg3: <a href="int.html">int</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the standard deviation from the selected locally-computed squared difference values.</p>
</span></td></tr>
<tr><td><code>final_stddev(arg1: <a href="float.html">float</a>, arg2: <a href="float.html">float</a>, arg3: <a href="int.html">int</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the standard deviation from the selected locally-computed squared difference values.</p>
</span></td></tr>
<tr><td><code>final_variance(arg1: <a href="decimal.html">decimal</a>, arg2: <a href="decimal.html">decimal</a>, arg3: <a href="int.html">int</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the variance from the selected locally-computed squared difference values.</p>
</span></td></tr>
<tr><td><code>final_variance(arg1: <a href="float.html">float</a>, arg2: <a href="float.html">float</a>, arg3: <a href="int.html">int</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the variance from the selected locally-computed squared difference values.</p>
</span></td></tr>
<tr><td><code>json_agg(arg1: anyelement) &rarr; jsonb</code></td><td><span class="funcdesc"><p>aggregates values as a JSON or JSONB array</p>
</span></td></tr>
<tr><td><code>jsonb_agg(arg1: anyelement) &rarr; jsonb</code></td><td><span class="funcdesc"><p>aggregates values as a JSON or JSONB array</p>
</span></td></tr>
<tr><td><code>max(arg1: <a href="bool.html">bool</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Identifies the maximum selected value.</p>
</span></td></tr>
<tr><td><code>max(arg1: <a href="bytes.html">bytes</a>) &rarr; <a href="bytes.html">bytes</a></code></td><td><span class="funcdesc"><p>Identifies the maximum selected value.</p>
</span></td></tr>
<tr><td><code>max(arg1: <a href="date.html">date</a>) &rarr; <a href="date.html">date</a></code></td><td><span class="funcdesc"><p>Identifies the maximum selected value.</p>
</span></td></tr>
<tr><td><code>max(arg1: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Identifies the maximum selected value.</p>
</span></td></tr>
<tr><td><code>max(arg1: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Identifies the maximum selected value.</p>
</span></td></tr>
<tr><td><code>max(arg1: <a href="inet.html">inet</a>) &rarr; <a href="inet.html">inet</a></code></td><td><span class="funcdesc"><p>Identifies the maximum selected value.</p>
</span></td></tr>
<tr><td><code>max(arg1: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Identifies the maximum selected value.</p>
</span></td></tr>
<tr><td><code>max(arg1: <a href="interval.html">interval</a>) &rarr; <a href="interval.html">interval</a></code></td><td><span class="funcdesc"><p>Identifies the maximum selected value.</p>
</span></td></tr>
<tr><td><code>max(arg1: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Identifies the maximum selected value.</p>
</span></td></tr>
<tr><td><code>max(arg1: <a href="time.html">time</a>) &rarr; <a href="time.html">time</a></code></td><td><span class="funcdesc"><p>Identifies the maximum selected value.</p>
</span></td></tr>
<tr><td><code>max(arg1: <a href="timestamp.html">timestamp</a>) &rarr; <a href="timestamp.html">timestamp</a></code></td><td><span class="funcdesc"><p>Identifies the maximum selected value.</p>
</span></td></tr>
<tr><td><code>max(arg1: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="timestamp.html">timestamptz</a></code></td><td><span class="funcdesc"><p>Identifies the maximum selected value.</p>
</span></td></tr>
<tr><td><code>max(arg1: <a href="uuid.html">uuid</a>) &rarr; <a href="uuid.html">uuid</a></code></td><td><span class="funcdesc"><p>Identifies the maximum selected value.</p>
</span></td></tr>
<tr><td><code>max(arg1: jsonb) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Identifies the maximum selected value.</p>
</span></td></tr>
<tr><td><code>max(arg1: oid) &rarr; oid</code></td><td><span class="funcdesc"><p>Identifies the maximum selected value.</p>
</span></td></tr>
<tr><td><code>min(arg1: <a href="bool.html">bool</a>) &rarr; <a href="bool.html">bool</a></code></td><td><span class="funcdesc"><p>Identifies the minimum selected value.</p>
</span></td></tr>
<tr><td><code>min(arg1: <a href="bytes.html">bytes</a>) &rarr; <a href="bytes.html">bytes</a></code></td><td><span class="funcdesc"><p>Identifies the minimum selected value.</p>
</span></td></tr>
<tr><td><code>min(arg1: <a href="date.html">date</a>) &rarr; <a href="date.html">date</a></code></td><td><span class="funcdesc"><p>Identifies the minimum selected value.</p>
</span></td></tr>
<tr><td><code>min(arg1: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Identifies the minimum selected value.</p>
</span></td></tr>
<tr><td><code>min(arg1: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Identifies the minimum selected value.</p>
</span></td></tr>
<tr><td><code>min(arg1: <a href="inet.html">inet</a>) &rarr; <a href="inet.html">inet</a></code></td><td><span class="funcdesc"><p>Identifies the minimum selected value.</p>
</span></td></tr>
<tr><td><code>min(arg1: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Identifies the minimum selected value.</p>
</span></td></tr>
<tr><td><code>min(arg1: <a href="interval.html">interval</a>) &rarr; <a href="interval.html">interval</a></code></td><td><span class="funcdesc"><p>Identifies the minimum selected value.</p>
</span></td></tr>
<tr><td><code>min(arg1: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code></td><td><span class="funcdesc"><p>Identifies the minimum selected value.</p>
</span></td></tr>
<tr><td><code>min(arg1: <a href="time.html">time</a>) &rarr; <a href="time.html">time</a></code></td><td><span class="funcdesc"><p>Identifies the minimum selected value.</p>
</span></td></tr>
<tr><td><code>min(arg1: <a href="timestamp.html">timestamp</a>) &rarr; <a href="timestamp.html">timestamp</a></code></td><td><span class="funcdesc"><p>Identifies the minimum selected value.</p>
</span></td></tr>
<tr><td><code>min(arg1: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="timestamp.html">timestamptz</a></code></td><td><span class="funcdesc"><p>Identifies the minimum selected value.</p>
</span></td></tr>
<tr><td><code>min(arg1: <a href="uuid.html">uuid</a>) &rarr; <a href="uuid.html">uuid</a></code></td><td><span class="funcdesc"><p>Identifies the minimum selected value.</p>
</span></td></tr>
<tr><td><code>min(arg1: jsonb) &rarr; jsonb</code></td><td><span class="funcdesc"><p>Identifies the minimum selected value.</p>
</span></td></tr>
<tr><td><code>min(arg1: oid) &rarr; oid</code></td><td><span class="funcdesc"><p>Identifies the minimum selected value.</p>
</span></td></tr>
<tr><td><code>sqrdiff(arg1: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the sum of squared differences from the mean of the selected values.</p>
</span></td></tr>
<tr><td><code>sqrdiff(arg1: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the sum of squared differences from the mean of the selected values.</p>
</span></td></tr>
<tr><td><code>sqrdiff(arg1: <a href="int.html">int</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the sum of squared differences from the mean of the selected values.</p>
</span></td></tr>
<tr><td><code>stddev(arg1: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the standard deviation of the selected values.</p>
</span></td></tr>
<tr><td><code>stddev(arg1: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the standard deviation of the selected values.</p>
</span></td></tr>
<tr><td><code>stddev(arg1: <a href="int.html">int</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the standard deviation of the selected values.</p>
</span></td></tr>
<tr><td><code>sum(arg1: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the sum of the selected values.</p>
</span></td></tr>
<tr><td><code>sum(arg1: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the sum of the selected values.</p>
</span></td></tr>
<tr><td><code>sum(arg1: <a href="int.html">int</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the sum of the selected values.</p>
</span></td></tr>
<tr><td><code>sum(arg1: <a href="interval.html">interval</a>) &rarr; <a href="interval.html">interval</a></code></td><td><span class="funcdesc"><p>Calculates the sum of the selected values.</p>
</span></td></tr>
<tr><td><code>sum_int(arg1: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the sum of the selected values.</p>
</span></td></tr>
<tr><td><code>variance(arg1: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the variance of the selected values.</p>
</span></td></tr>
<tr><td><code>variance(arg1: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code></td><td><span class="funcdesc"><p>Calculates the variance of the selected values.</p>
</span></td></tr>
<tr><td><code>variance(arg1: <a href="int.html">int</a>) &rarr; <a href="decimal.html">decimal</a></code></td><td><span class="funcdesc"><p>Calculates the variance of the selected values.</p>
</span></td></tr>
<tr><td><code>xor_agg(arg1: <a href="bytes.html">bytes</a>) &rarr; <a href="bytes.html">bytes</a></code></td><td><span class="funcdesc"><p>Calculates the bitwise XOR of the selected values.</p>
</span></td></tr>
<tr><td><code>xor_agg(arg1: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code></td><td><span class="funcdesc"><p>Calculates the bitwise XOR of the selected values.</p>
</span></td></tr></tbody>
</table>

