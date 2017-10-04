### Array Functions

Function &rarr; Returns | Description
--- | ---
<code>array_append(array: <a href="bool.html">bool</a>[], elem: <a href="bool.html">bool</a>) &rarr; <a href="bool.html">bool</a>[]</code> | <span class="funcdesc">Appends `elem` to `array`, returning the result.</span>
<code>array_append(array: <a href="bytes.html">bytes</a>[], elem: <a href="bytes.html">bytes</a>) &rarr; <a href="bytes.html">bytes</a>[]</code> | <span class="funcdesc">Appends `elem` to `array`, returning the result.</span>
<code>array_append(array: <a href="date.html">date</a>[], elem: <a href="date.html">date</a>) &rarr; <a href="date.html">date</a>[]</code> | <span class="funcdesc">Appends `elem` to `array`, returning the result.</span>
<code>array_append(array: <a href="decimal.html">decimal</a>[], elem: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a>[]</code> | <span class="funcdesc">Appends `elem` to `array`, returning the result.</span>
<code>array_append(array: <a href="float.html">float</a>[], elem: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a>[]</code> | <span class="funcdesc">Appends `elem` to `array`, returning the result.</span>
<code>array_append(array: <a href="int.html">int</a>[], elem: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a>[]</code> | <span class="funcdesc">Appends `elem` to `array`, returning the result.</span>
<code>array_append(array: <a href="interval.html">interval</a>[], elem: <a href="interval.html">interval</a>) &rarr; <a href="interval.html">interval</a>[]</code> | <span class="funcdesc">Appends `elem` to `array`, returning the result.</span>
<code>array_append(array: <a href="string.html">string</a>[], elem: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a>[]</code> | <span class="funcdesc">Appends `elem` to `array`, returning the result.</span>
<code>array_append(array: <a href="timestamp.html">timestamp</a>[], elem: <a href="timestamp.html">timestamp</a>) &rarr; <a href="timestamp.html">timestamp</a>[]</code> | <span class="funcdesc">Appends `elem` to `array`, returning the result.</span>
<code>array_append(array: <a href="timestamp.html">timestamptz</a>[], elem: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="timestamp.html">timestamptz</a>[]</code> | <span class="funcdesc">Appends `elem` to `array`, returning the result.</span>
<code>array_append(array: inet[], elem: inet) &rarr; inet[]</code> | <span class="funcdesc">Appends `elem` to `array`, returning the result.</span>
<code>array_append(array: oid[], elem: oid) &rarr; oid[]</code> | <span class="funcdesc">Appends `elem` to `array`, returning the result.</span>
<code>array_append(array: uuid[], elem: uuid) &rarr; uuid[]</code> | <span class="funcdesc">Appends `elem` to `array`, returning the result.</span>
<code>array_cat(left: <a href="bool.html">bool</a>[], right: <a href="bool.html">bool</a>[]) &rarr; <a href="bool.html">bool</a>[]</code> | <span class="funcdesc">Appends two arrays.</span>
<code>array_cat(left: <a href="bytes.html">bytes</a>[], right: <a href="bytes.html">bytes</a>[]) &rarr; <a href="bytes.html">bytes</a>[]</code> | <span class="funcdesc">Appends two arrays.</span>
<code>array_cat(left: <a href="date.html">date</a>[], right: <a href="date.html">date</a>[]) &rarr; <a href="date.html">date</a>[]</code> | <span class="funcdesc">Appends two arrays.</span>
<code>array_cat(left: <a href="decimal.html">decimal</a>[], right: <a href="decimal.html">decimal</a>[]) &rarr; <a href="decimal.html">decimal</a>[]</code> | <span class="funcdesc">Appends two arrays.</span>
<code>array_cat(left: <a href="float.html">float</a>[], right: <a href="float.html">float</a>[]) &rarr; <a href="float.html">float</a>[]</code> | <span class="funcdesc">Appends two arrays.</span>
<code>array_cat(left: <a href="int.html">int</a>[], right: <a href="int.html">int</a>[]) &rarr; <a href="int.html">int</a>[]</code> | <span class="funcdesc">Appends two arrays.</span>
<code>array_cat(left: <a href="interval.html">interval</a>[], right: <a href="interval.html">interval</a>[]) &rarr; <a href="interval.html">interval</a>[]</code> | <span class="funcdesc">Appends two arrays.</span>
<code>array_cat(left: <a href="string.html">string</a>[], right: <a href="string.html">string</a>[]) &rarr; <a href="string.html">string</a>[]</code> | <span class="funcdesc">Appends two arrays.</span>
<code>array_cat(left: <a href="timestamp.html">timestamp</a>[], right: <a href="timestamp.html">timestamp</a>[]) &rarr; <a href="timestamp.html">timestamp</a>[]</code> | <span class="funcdesc">Appends two arrays.</span>
<code>array_cat(left: <a href="timestamp.html">timestamptz</a>[], right: <a href="timestamp.html">timestamptz</a>[]) &rarr; <a href="timestamp.html">timestamptz</a>[]</code> | <span class="funcdesc">Appends two arrays.</span>
<code>array_cat(left: inet[], right: inet[]) &rarr; inet[]</code> | <span class="funcdesc">Appends two arrays.</span>
<code>array_cat(left: oid[], right: oid[]) &rarr; oid[]</code> | <span class="funcdesc">Appends two arrays.</span>
<code>array_cat(left: uuid[], right: uuid[]) &rarr; uuid[]</code> | <span class="funcdesc">Appends two arrays.</span>
<code>array_length(input: anyelement[], array_dimension: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the length of `input` on the provided `array_dimension`. However, because CockroachDB doesn't yet support multi-dimensional arrays, the only supported `array_dimension` is **1**.</span>
<code>array_lower(input: anyelement[], array_dimension: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the minimum value of `input` on the provided `array_dimension`. However, because CockroachDB doesn't yet support multi-dimensional arrays, the only supported `array_dimension` is **1**.</span>
<code>array_position(array: <a href="bool.html">bool</a>[], elem: <a href="bool.html">bool</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Return the index of the first occurrence of `elem` in `array`.</span>
<code>array_position(array: <a href="bytes.html">bytes</a>[], elem: <a href="bytes.html">bytes</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Return the index of the first occurrence of `elem` in `array`.</span>
<code>array_position(array: <a href="date.html">date</a>[], elem: <a href="date.html">date</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Return the index of the first occurrence of `elem` in `array`.</span>
<code>array_position(array: <a href="decimal.html">decimal</a>[], elem: <a href="decimal.html">decimal</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Return the index of the first occurrence of `elem` in `array`.</span>
<code>array_position(array: <a href="float.html">float</a>[], elem: <a href="float.html">float</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Return the index of the first occurrence of `elem` in `array`.</span>
<code>array_position(array: <a href="int.html">int</a>[], elem: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Return the index of the first occurrence of `elem` in `array`.</span>
<code>array_position(array: <a href="interval.html">interval</a>[], elem: <a href="interval.html">interval</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Return the index of the first occurrence of `elem` in `array`.</span>
<code>array_position(array: <a href="string.html">string</a>[], elem: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Return the index of the first occurrence of `elem` in `array`.</span>
<code>array_position(array: <a href="timestamp.html">timestamp</a>[], elem: <a href="timestamp.html">timestamp</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Return the index of the first occurrence of `elem` in `array`.</span>
<code>array_position(array: <a href="timestamp.html">timestamptz</a>[], elem: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Return the index of the first occurrence of `elem` in `array`.</span>
<code>array_position(array: inet[], elem: inet) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Return the index of the first occurrence of `elem` in `array`.</span>
<code>array_position(array: oid[], elem: oid) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Return the index of the first occurrence of `elem` in `array`.</span>
<code>array_position(array: uuid[], elem: uuid) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Return the index of the first occurrence of `elem` in `array`.</span>
<code>array_positions(array: <a href="bool.html">bool</a>[], elem: <a href="bool.html">bool</a>) &rarr; <a href="bool.html">bool</a>[]</code> | <span class="funcdesc">Returns and array of indexes of all occurrences of `elem` in `array`.</span>
<code>array_positions(array: <a href="bytes.html">bytes</a>[], elem: <a href="bytes.html">bytes</a>) &rarr; <a href="bytes.html">bytes</a>[]</code> | <span class="funcdesc">Returns and array of indexes of all occurrences of `elem` in `array`.</span>
<code>array_positions(array: <a href="date.html">date</a>[], elem: <a href="date.html">date</a>) &rarr; <a href="date.html">date</a>[]</code> | <span class="funcdesc">Returns and array of indexes of all occurrences of `elem` in `array`.</span>
<code>array_positions(array: <a href="decimal.html">decimal</a>[], elem: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a>[]</code> | <span class="funcdesc">Returns and array of indexes of all occurrences of `elem` in `array`.</span>
<code>array_positions(array: <a href="float.html">float</a>[], elem: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a>[]</code> | <span class="funcdesc">Returns and array of indexes of all occurrences of `elem` in `array`.</span>
<code>array_positions(array: <a href="int.html">int</a>[], elem: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a>[]</code> | <span class="funcdesc">Returns and array of indexes of all occurrences of `elem` in `array`.</span>
<code>array_positions(array: <a href="interval.html">interval</a>[], elem: <a href="interval.html">interval</a>) &rarr; <a href="interval.html">interval</a>[]</code> | <span class="funcdesc">Returns and array of indexes of all occurrences of `elem` in `array`.</span>
<code>array_positions(array: <a href="string.html">string</a>[], elem: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a>[]</code> | <span class="funcdesc">Returns and array of indexes of all occurrences of `elem` in `array`.</span>
<code>array_positions(array: <a href="timestamp.html">timestamp</a>[], elem: <a href="timestamp.html">timestamp</a>) &rarr; <a href="timestamp.html">timestamp</a>[]</code> | <span class="funcdesc">Returns and array of indexes of all occurrences of `elem` in `array`.</span>
<code>array_positions(array: <a href="timestamp.html">timestamptz</a>[], elem: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="timestamp.html">timestamptz</a>[]</code> | <span class="funcdesc">Returns and array of indexes of all occurrences of `elem` in `array`.</span>
<code>array_positions(array: inet[], elem: inet) &rarr; inet[]</code> | <span class="funcdesc">Returns and array of indexes of all occurrences of `elem` in `array`.</span>
<code>array_positions(array: oid[], elem: oid) &rarr; oid[]</code> | <span class="funcdesc">Returns and array of indexes of all occurrences of `elem` in `array`.</span>
<code>array_positions(array: uuid[], elem: uuid) &rarr; uuid[]</code> | <span class="funcdesc">Returns and array of indexes of all occurrences of `elem` in `array`.</span>
<code>array_prepend(elem: <a href="bool.html">bool</a>, array: <a href="bool.html">bool</a>[]) &rarr; <a href="bool.html">bool</a>[]</code> | <span class="funcdesc">Prepends `elem` to `array`, returning the result.</span>
<code>array_prepend(elem: <a href="bytes.html">bytes</a>, array: <a href="bytes.html">bytes</a>[]) &rarr; <a href="bytes.html">bytes</a>[]</code> | <span class="funcdesc">Prepends `elem` to `array`, returning the result.</span>
<code>array_prepend(elem: <a href="date.html">date</a>, array: <a href="date.html">date</a>[]) &rarr; <a href="date.html">date</a>[]</code> | <span class="funcdesc">Prepends `elem` to `array`, returning the result.</span>
<code>array_prepend(elem: <a href="decimal.html">decimal</a>, array: <a href="decimal.html">decimal</a>[]) &rarr; <a href="decimal.html">decimal</a>[]</code> | <span class="funcdesc">Prepends `elem` to `array`, returning the result.</span>
<code>array_prepend(elem: <a href="float.html">float</a>, array: <a href="float.html">float</a>[]) &rarr; <a href="float.html">float</a>[]</code> | <span class="funcdesc">Prepends `elem` to `array`, returning the result.</span>
<code>array_prepend(elem: <a href="int.html">int</a>, array: <a href="int.html">int</a>[]) &rarr; <a href="int.html">int</a>[]</code> | <span class="funcdesc">Prepends `elem` to `array`, returning the result.</span>
<code>array_prepend(elem: <a href="interval.html">interval</a>, array: <a href="interval.html">interval</a>[]) &rarr; <a href="interval.html">interval</a>[]</code> | <span class="funcdesc">Prepends `elem` to `array`, returning the result.</span>
<code>array_prepend(elem: <a href="string.html">string</a>, array: <a href="string.html">string</a>[]) &rarr; <a href="string.html">string</a>[]</code> | <span class="funcdesc">Prepends `elem` to `array`, returning the result.</span>
<code>array_prepend(elem: <a href="timestamp.html">timestamp</a>, array: <a href="timestamp.html">timestamp</a>[]) &rarr; <a href="timestamp.html">timestamp</a>[]</code> | <span class="funcdesc">Prepends `elem` to `array`, returning the result.</span>
<code>array_prepend(elem: <a href="timestamp.html">timestamptz</a>, array: <a href="timestamp.html">timestamptz</a>[]) &rarr; <a href="timestamp.html">timestamptz</a>[]</code> | <span class="funcdesc">Prepends `elem` to `array`, returning the result.</span>
<code>array_prepend(elem: inet, array: inet[]) &rarr; inet[]</code> | <span class="funcdesc">Prepends `elem` to `array`, returning the result.</span>
<code>array_prepend(elem: oid, array: oid[]) &rarr; oid[]</code> | <span class="funcdesc">Prepends `elem` to `array`, returning the result.</span>
<code>array_prepend(elem: uuid, array: uuid[]) &rarr; uuid[]</code> | <span class="funcdesc">Prepends `elem` to `array`, returning the result.</span>
<code>array_remove(array: <a href="bool.html">bool</a>[], elem: <a href="bool.html">bool</a>) &rarr; <a href="bool.html">bool</a>[]</code> | <span class="funcdesc">Remove from `array` all elements equal to `elem`.</span>
<code>array_remove(array: <a href="bytes.html">bytes</a>[], elem: <a href="bytes.html">bytes</a>) &rarr; <a href="bytes.html">bytes</a>[]</code> | <span class="funcdesc">Remove from `array` all elements equal to `elem`.</span>
<code>array_remove(array: <a href="date.html">date</a>[], elem: <a href="date.html">date</a>) &rarr; <a href="date.html">date</a>[]</code> | <span class="funcdesc">Remove from `array` all elements equal to `elem`.</span>
<code>array_remove(array: <a href="decimal.html">decimal</a>[], elem: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a>[]</code> | <span class="funcdesc">Remove from `array` all elements equal to `elem`.</span>
<code>array_remove(array: <a href="float.html">float</a>[], elem: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a>[]</code> | <span class="funcdesc">Remove from `array` all elements equal to `elem`.</span>
<code>array_remove(array: <a href="int.html">int</a>[], elem: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a>[]</code> | <span class="funcdesc">Remove from `array` all elements equal to `elem`.</span>
<code>array_remove(array: <a href="interval.html">interval</a>[], elem: <a href="interval.html">interval</a>) &rarr; <a href="interval.html">interval</a>[]</code> | <span class="funcdesc">Remove from `array` all elements equal to `elem`.</span>
<code>array_remove(array: <a href="string.html">string</a>[], elem: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a>[]</code> | <span class="funcdesc">Remove from `array` all elements equal to `elem`.</span>
<code>array_remove(array: <a href="timestamp.html">timestamp</a>[], elem: <a href="timestamp.html">timestamp</a>) &rarr; <a href="timestamp.html">timestamp</a>[]</code> | <span class="funcdesc">Remove from `array` all elements equal to `elem`.</span>
<code>array_remove(array: <a href="timestamp.html">timestamptz</a>[], elem: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="timestamp.html">timestamptz</a>[]</code> | <span class="funcdesc">Remove from `array` all elements equal to `elem`.</span>
<code>array_remove(array: inet[], elem: inet) &rarr; inet[]</code> | <span class="funcdesc">Remove from `array` all elements equal to `elem`.</span>
<code>array_remove(array: oid[], elem: oid) &rarr; oid[]</code> | <span class="funcdesc">Remove from `array` all elements equal to `elem`.</span>
<code>array_remove(array: uuid[], elem: uuid) &rarr; uuid[]</code> | <span class="funcdesc">Remove from `array` all elements equal to `elem`.</span>
<code>array_replace(array: <a href="bool.html">bool</a>[], toreplace: <a href="bool.html">bool</a>, replacewith: <a href="bool.html">bool</a>) &rarr; <a href="bool.html">bool</a>[]</code> | <span class="funcdesc">Replace all occurrences of `toreplace` in `array` with `replacewith`.</span>
<code>array_replace(array: <a href="bytes.html">bytes</a>[], toreplace: <a href="bytes.html">bytes</a>, replacewith: <a href="bytes.html">bytes</a>) &rarr; <a href="bytes.html">bytes</a>[]</code> | <span class="funcdesc">Replace all occurrences of `toreplace` in `array` with `replacewith`.</span>
<code>array_replace(array: <a href="date.html">date</a>[], toreplace: <a href="date.html">date</a>, replacewith: <a href="date.html">date</a>) &rarr; <a href="date.html">date</a>[]</code> | <span class="funcdesc">Replace all occurrences of `toreplace` in `array` with `replacewith`.</span>
<code>array_replace(array: <a href="decimal.html">decimal</a>[], toreplace: <a href="decimal.html">decimal</a>, replacewith: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a>[]</code> | <span class="funcdesc">Replace all occurrences of `toreplace` in `array` with `replacewith`.</span>
<code>array_replace(array: <a href="float.html">float</a>[], toreplace: <a href="float.html">float</a>, replacewith: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a>[]</code> | <span class="funcdesc">Replace all occurrences of `toreplace` in `array` with `replacewith`.</span>
<code>array_replace(array: <a href="int.html">int</a>[], toreplace: <a href="int.html">int</a>, replacewith: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a>[]</code> | <span class="funcdesc">Replace all occurrences of `toreplace` in `array` with `replacewith`.</span>
<code>array_replace(array: <a href="interval.html">interval</a>[], toreplace: <a href="interval.html">interval</a>, replacewith: <a href="interval.html">interval</a>) &rarr; <a href="interval.html">interval</a>[]</code> | <span class="funcdesc">Replace all occurrences of `toreplace` in `array` with `replacewith`.</span>
<code>array_replace(array: <a href="string.html">string</a>[], toreplace: <a href="string.html">string</a>, replacewith: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a>[]</code> | <span class="funcdesc">Replace all occurrences of `toreplace` in `array` with `replacewith`.</span>
<code>array_replace(array: <a href="timestamp.html">timestamp</a>[], toreplace: <a href="timestamp.html">timestamp</a>, replacewith: <a href="timestamp.html">timestamp</a>) &rarr; <a href="timestamp.html">timestamp</a>[]</code> | <span class="funcdesc">Replace all occurrences of `toreplace` in `array` with `replacewith`.</span>
<code>array_replace(array: <a href="timestamp.html">timestamptz</a>[], toreplace: <a href="timestamp.html">timestamptz</a>, replacewith: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="timestamp.html">timestamptz</a>[]</code> | <span class="funcdesc">Replace all occurrences of `toreplace` in `array` with `replacewith`.</span>
<code>array_replace(array: inet[], toreplace: inet, replacewith: inet) &rarr; inet[]</code> | <span class="funcdesc">Replace all occurrences of `toreplace` in `array` with `replacewith`.</span>
<code>array_replace(array: oid[], toreplace: oid, replacewith: oid) &rarr; oid[]</code> | <span class="funcdesc">Replace all occurrences of `toreplace` in `array` with `replacewith`.</span>
<code>array_replace(array: uuid[], toreplace: uuid, replacewith: uuid) &rarr; uuid[]</code> | <span class="funcdesc">Replace all occurrences of `toreplace` in `array` with `replacewith`.</span>
<code>array_upper(input: anyelement[], array_dimension: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the maximum value of `input` on the provided `array_dimension`. However, because CockroachDB doesn't yet support multi-dimensional arrays, the only supported `array_dimension` is **1**.</span>

### BOOL Functions

Function &rarr; Returns | Description
--- | ---
<code>inet_same_family(val: inet, val: inet) &rarr; <a href="bool.html">bool</a></code> | <span class="funcdesc">Checks if two IP addresses are of the same IP family.</span>

### Comparison Functions

Function &rarr; Returns | Description
--- | ---
<code>greatest(anyelement...) &rarr; anyelement</code> | <span class="funcdesc">Returns the element with the greatest value.</span>
<code>least(anyelement...) &rarr; anyelement</code> | <span class="funcdesc">Returns the element with the lowest value.</span>

### Date and Time Functions

Function &rarr; Returns | Description
--- | ---
<code>age(begin: <a href="timestamp.html">timestamptz</a>, end: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="interval.html">interval</a></code> | <span class="funcdesc">Calculates the interval between `begin` and `end`.</span>
<code>age(val: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="interval.html">interval</a></code> | <span class="funcdesc">Calculates the interval between `val` and the current time.</span>
<code>clock_timestamp() &rarr; <a href="timestamp.html">timestamp</a></code> | <span class="funcdesc">Returns the current wallclock time.</span>
<code>clock_timestamp() &rarr; <a href="timestamp.html">timestamptz</a></code> | <span class="funcdesc">Returns the current wallclock time.</span>
<code>current_date() &rarr; <a href="date.html">date</a></code> | <span class="funcdesc">Returns the current date.</span>
<code>current_timestamp() &rarr; <a href="timestamp.html">timestamp</a></code> | <span class="funcdesc">Returns the current transaction's timestamp.</span>
<code>current_timestamp() &rarr; <a href="timestamp.html">timestamptz</a></code> | <span class="funcdesc">Returns the current transaction's timestamp.</span>
<code>experimental_strftime(input: <a href="date.html">date</a>, extract_format: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">From `input`, extracts and formats the time as identified in `extract_format` using standard `strftime` notation (though not all formatting is supported).</span>
<code>experimental_strftime(input: <a href="timestamp.html">timestamp</a>, extract_format: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">From `input`, extracts and formats the time as identified in `extract_format` using standard `strftime` notation (though not all formatting is supported).</span>
<code>experimental_strftime(input: <a href="timestamp.html">timestamptz</a>, extract_format: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">From `input`, extracts and formats the time as identified in `extract_format` using standard `strftime` notation (though not all formatting is supported).</span>
<code>experimental_strptime(input: <a href="string.html">string</a>, format: <a href="string.html">string</a>) &rarr; <a href="timestamp.html">timestamptz</a></code> | <span class="funcdesc">Returns `input` as a timestamptz using `format` (which uses standard `strptime` formatting).</span>
<code>extract(element: <a href="string.html">string</a>, input: <a href="date.html">date</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Extracts `element` from `input`.

Compatible elements: year, quarter, month, week, dayofweek, dayofyear,
hour, minute, second, millisecond, microsecond, epoch</span>
<code>extract(element: <a href="string.html">string</a>, input: <a href="timestamp.html">timestamp</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Extracts `element` from `input`.

Compatible elements: year, quarter, month, week, dayofweek, dayofyear,
hour, minute, second, millisecond, microsecond, epoch</span>
<code>extract(element: <a href="string.html">string</a>, input: <a href="timestamp.html">timestamptz</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Extracts `element` from `input`.

Compatible elements: year, quarter, month, week, dayofweek, dayofyear,
hour, minute, second, millisecond, microsecond, epoch</span>
<code>extract_duration(element: <a href="string.html">string</a>, input: <a href="interval.html">interval</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Extracts `element` from `input`.
Compatible elements: hour, minute, second, millisecond, microsecond.</span>
<code>now() &rarr; <a href="timestamp.html">timestamp</a></code> | <span class="funcdesc">Returns the current transaction's timestamp.</span>
<code>now() &rarr; <a href="timestamp.html">timestamptz</a></code> | <span class="funcdesc">Returns the current transaction's timestamp.</span>
<code>statement_timestamp() &rarr; <a href="timestamp.html">timestamp</a></code> | <span class="funcdesc">Returns the current statement's timestamp.</span>
<code>statement_timestamp() &rarr; <a href="timestamp.html">timestamptz</a></code> | <span class="funcdesc">Returns the current statement's timestamp.</span>
<code>transaction_timestamp() &rarr; <a href="timestamp.html">timestamp</a></code> | <span class="funcdesc">Returns the current transaction's timestamp.</span>
<code>transaction_timestamp() &rarr; <a href="timestamp.html">timestamptz</a></code> | <span class="funcdesc">Returns the current transaction's timestamp.</span>

### ID Generation Functions

Function &rarr; Returns | Description
--- | ---
<code>experimental_uuid_v4() &rarr; <a href="bytes.html">bytes</a></code> | <span class="funcdesc">Returns a UUID.</span>
<code>unique_rowid() &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Returns a unique ID used by CockroachDB to generate unique row IDs if a Primary Key isn't defined for the table. The value is a combination of the  insert timestamp and the ID of the node executing the statement, which  guarantees this combination is globally unique.</span>
<code>uuid_v4() &rarr; <a href="bytes.html">bytes</a></code> | <span class="funcdesc">Returns a UUID.</span>

### INET Functions

Function &rarr; Returns | Description
--- | ---
<code>abbrev(val: inet) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Converts the combined IP address and prefix length to an abbreviated display format as text.For INET types, this will omit the prefix length if it's not the default (32 or IPv4, 128 for IPv6)

For example, `abbrev('192.168.1.2/24')` returns `'192.168.1.2/24'`</span>
<code>broadcast(val: inet) &rarr; inet</code> | <span class="funcdesc">Gets the broadcast address for the network address represented by the value.

For example, `broadcast('192.168.1.2/24')` returns `'192.168.1.255/24'`</span>
<code>family(val: inet) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Extracts the IP family of the value; 4 for IPv4, 6 for IPv6.

For example, `family('::1')` returns `6`</span>
<code>host(val: inet) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Extracts the address part of the combined address/prefixlen value as text.

For example, `host('192.168.1.2/16')` returns `'192.168.1.2'`</span>
<code>hostmask(val: inet) &rarr; inet</code> | <span class="funcdesc">Creates an IP host mask corresponding to the prefix length in the value.

For example, `hostmask('192.168.1.2/16')` returns `'0.0.255.255'`</span>
<code>masklen(val: inet) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Retrieves the prefix length stored in the value.

For example, `masklen('192.168.1.2/16')` returns `16`</span>
<code>netmask(val: inet) &rarr; inet</code> | <span class="funcdesc">Creates an IP network mask corresponding to the prefix length in the value.

For example, `netmask('192.168.1.2/16')` returns `'255.255.0.0'`</span>
<code>set_masklen(val: inet, prefixlen: <a href="int.html">int</a>) &rarr; inet</code> | <span class="funcdesc">Sets the prefix length of `val` to `prefixlen`.

For example, `set_masklen('192.168.1.2', 16)` returns `'192.168.1.2/16'`.</span>
<code>text(val: inet) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Converts the IP address and prefix length to text.</span>

### Math and Numeric Functions

Function &rarr; Returns | Description
--- | ---
<code>abs(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code> | <span class="funcdesc">Calculates the absolute value of `val`.</span>
<code>abs(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates the absolute value of `val`.</span>
<code>abs(val: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the absolute value of `val`.</span>
<code>acos(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates the inverse cosine of `val`.</span>
<code>asin(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates the inverse sine of `val`.</span>
<code>atan(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates the inverse tangent of `val`.</span>
<code>atan2(x: <a href="float.html">float</a>, y: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates the inverse tangent of `x`/`y`.</span>
<code>cbrt(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code> | <span class="funcdesc">Calculates the cube root (∛) of `val`.</span>
<code>cbrt(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates the cube root (∛) of `val`.</span>
<code>ceil(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code> | <span class="funcdesc">Calculates the smallest integer greater than `val`.</span>
<code>ceil(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates the smallest integer greater than `val`.</span>
<code>ceiling(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code> | <span class="funcdesc">Calculates the smallest integer greater than `val`.</span>
<code>ceiling(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates the smallest integer greater than `val`.</span>
<code>cos(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates the cosine of `val`.</span>
<code>cot(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates the cotangent of `val`.</span>
<code>crc32c(<a href="bytes.html">bytes</a>...) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the CRC-32 hash using the Castagnoli polynomial.</span>
<code>crc32c(<a href="string.html">string</a>...) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the CRC-32 hash using the Castagnoli polynomial.</span>
<code>crc32ieee(<a href="bytes.html">bytes</a>...) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the CRC-32 hash using the IEEE polynomial.</span>
<code>crc32ieee(<a href="string.html">string</a>...) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the CRC-32 hash using the IEEE polynomial.</span>
<code>degrees(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Converts `val` as a radian value to a degree value.</span>
<code>div(x: <a href="decimal.html">decimal</a>, y: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code> | <span class="funcdesc">Calculates the integer quotient of `x`/`y`.</span>
<code>div(x: <a href="float.html">float</a>, y: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates the integer quotient of `x`/`y`.</span>
<code>div(x: <a href="int.html">int</a>, y: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the integer quotient of `x`/`y`.</span>
<code>exp(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code> | <span class="funcdesc">Calculates *e* ^ `val`.</span>
<code>exp(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates *e* ^ `val`.</span>
<code>floor(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code> | <span class="funcdesc">Calculates the largest integer not greater than `val`.</span>
<code>floor(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates the largest integer not greater than `val`.</span>
<code>fnv32(<a href="bytes.html">bytes</a>...) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the 32-bit FNV-1 hash value of a set of values.</span>
<code>fnv32(<a href="string.html">string</a>...) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the 32-bit FNV-1 hash value of a set of values.</span>
<code>fnv32a(<a href="bytes.html">bytes</a>...) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the 32-bit FNV-1a hash value of a set of values.</span>
<code>fnv32a(<a href="string.html">string</a>...) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the 32-bit FNV-1a hash value of a set of values.</span>
<code>fnv64(<a href="bytes.html">bytes</a>...) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the 64-bit FNV-1 hash value of a set of values.</span>
<code>fnv64(<a href="string.html">string</a>...) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the 64-bit FNV-1 hash value of a set of values.</span>
<code>fnv64a(<a href="bytes.html">bytes</a>...) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the 64-bit FNV-1a hash value of a set of values.</span>
<code>fnv64a(<a href="string.html">string</a>...) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the 64-bit FNV-1a hash value of a set of values.</span>
<code>isnan(val: <a href="decimal.html">decimal</a>) &rarr; <a href="bool.html">bool</a></code> | <span class="funcdesc">Returns true if `val` is NaN, false otherwise.</span>
<code>isnan(val: <a href="float.html">float</a>) &rarr; <a href="bool.html">bool</a></code> | <span class="funcdesc">Returns true if `val` is NaN, false otherwise.</span>
<code>ln(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code> | <span class="funcdesc">Calculates the natural log of `val`.</span>
<code>ln(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates the natural log of `val`.</span>
<code>log(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code> | <span class="funcdesc">Calculates the base 10 log of `val`.</span>
<code>log(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates the base 10 log of `val`.</span>
<code>mod(x: <a href="decimal.html">decimal</a>, y: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code> | <span class="funcdesc">Calculates `x`%`y`.</span>
<code>mod(x: <a href="float.html">float</a>, y: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates `x`%`y`.</span>
<code>mod(x: <a href="int.html">int</a>, y: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates `x`%`y`.</span>
<code>pi() &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Returns the value for pi (3.141592653589793).</span>
<code>pow(x: <a href="decimal.html">decimal</a>, y: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code> | <span class="funcdesc">Calculates `x`^`y`.</span>
<code>pow(x: <a href="float.html">float</a>, y: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates `x`^`y`.</span>
<code>pow(x: <a href="int.html">int</a>, y: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates `x`^`y`.</span>
<code>power(x: <a href="decimal.html">decimal</a>, y: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code> | <span class="funcdesc">Calculates `x`^`y`.</span>
<code>power(x: <a href="float.html">float</a>, y: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates `x`^`y`.</span>
<code>power(x: <a href="int.html">int</a>, y: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates `x`^`y`.</span>
<code>radians(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Converts `val` as a degree value to a radians value.</span>
<code>random() &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Returns a random float between 0 and 1.</span>
<code>round(input: <a href="decimal.html">decimal</a>, decimal_accuracy: <a href="int.html">int</a>) &rarr; <a href="decimal.html">decimal</a></code> | <span class="funcdesc">Keeps `decimal_accuracy` number of figures to the right of the zero position  in `input using half away from zero rounding. If `decimal_accuracy` is not in the range -2^31...(2^31-1), the results are undefined.</span>
<code>round(input: <a href="float.html">float</a>, decimal_accuracy: <a href="int.html">int</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Keeps `decimal_accuracy` number of figures to the right of the zero position  in `input` using half to even (banker's) rounding.</span>
<code>round(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code> | <span class="funcdesc">Rounds `val` to the nearest integer, half away from zero: ROUND(+/-2.4) = +/-2, ROUND(+/-2.5) = +/-3.</span>
<code>round(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Rounds `val` to the nearest integer using half to even (banker's) rounding.</span>
<code>sign(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code> | <span class="funcdesc">Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** for negative.</span>
<code>sign(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** for negative.</span>
<code>sign(val: <a href="int.html">int</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** for negative.</span>
<code>sin(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates the sine of `val`.</span>
<code>sqrt(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code> | <span class="funcdesc">Calculates the square root of `val`.</span>
<code>sqrt(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates the square root of `val`.</span>
<code>tan(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Calculates the tangent of `val`.</span>
<code>to_hex(val: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Converts `val` to its hexadecimal representation.</span>
<code>trunc(val: <a href="decimal.html">decimal</a>) &rarr; <a href="decimal.html">decimal</a></code> | <span class="funcdesc">Truncates the decimal values of `val`.</span>
<code>trunc(val: <a href="float.html">float</a>) &rarr; <a href="float.html">float</a></code> | <span class="funcdesc">Truncates the decimal values of `val`.</span>

### String and Byte Functions

Function &rarr; Returns | Description
--- | ---
<code>ascii(val: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the ASCII value for the first character in `val`.</span>
<code>btrim(input: <a href="string.html">string</a>, trim_chars: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Removes any characters included in `trim_chars` from the beginning or end of `input` (applies recursively). 

For example, `btrim('doggie', 'eod')` returns `ggi`.</span>
<code>btrim(val: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Removes all spaces from the beginning and end of `val`.</span>
<code>concat(<a href="string.html">string</a>...) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Concatenates a comma-separated list of strings.</span>
<code>concat_ws(<a href="string.html">string</a>...) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Uses the first argument as a separator between the concatenation of the subsequent arguments. 

For example `concat_ws('!','wow','great')` returns `wow!great`.</span>
<code>from_ip(val: <a href="bytes.html">bytes</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Converts the byte string representation of an IP to its character string representation.</span>
<code>from_uuid(val: <a href="bytes.html">bytes</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Converts the byte string representation of a UUID to its character string representation.</span>
<code>initcap(val: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Capitalizes the first letter of `val`.</span>
<code>left(input: <a href="bytes.html">bytes</a>, return_set: <a href="int.html">int</a>) &rarr; <a href="bytes.html">bytes</a></code> | <span class="funcdesc">Returns the first `return_set` bytes from `input`.</span>
<code>left(input: <a href="string.html">string</a>, return_set: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Returns the first `return_set` characters from `input`.</span>
<code>length(val: <a href="bytes.html">bytes</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the number of bytes in `val`.</span>
<code>length(val: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the number of characters in `val`.</span>
<code>lower(val: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Converts all characters in `val` to their lower-case equivalents.</span>
<code>ltrim(input: <a href="string.html">string</a>, trim_chars: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Removes any characters included in `trim_chars` from the beginning (left-hand side) of `input` (applies recursively). 

For example, `ltrim('doggie', 'od')` returns `ggie`.</span>
<code>ltrim(val: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Removes all spaces from the beginning (left-hand side) of `val`.</span>
<code>md5(<a href="bytes.html">bytes</a>...) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Calculates the MD5 hash value of a set of values.</span>
<code>md5(<a href="string.html">string</a>...) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Calculates the MD5 hash value of a set of values.</span>
<code>octet_length(val: <a href="bytes.html">bytes</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the number of bytes in `val`.</span>
<code>octet_length(val: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the number of bytes used to represent `val`.</span>
<code>overlay(input: <a href="string.html">string</a>, overlay_val: <a href="string.html">string</a>, start_pos: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Replaces characters in `input` with `overlay_val` starting at `start_pos` (begins at 1). 

For example, `overlay('doggie', 'CAT', 2)` returns `dCATie`.</span>
<code>overlay(input: <a href="string.html">string</a>, overlay_val: <a href="string.html">string</a>, start_pos: <a href="int.html">int</a>, end_pos: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Deletes the characters in `input` between `start_pos` and `end_pos` (count starts at 1), and then insert `overlay_val` at `start_pos`.</span>
<code>regexp_extract(input: <a href="string.html">string</a>, regex: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Returns the first match for the Regular Expression `regex` in `input`.</span>
<code>regexp_replace(input: <a href="string.html">string</a>, regex: <a href="string.html">string</a>, replace: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Replaces matches for the Regular Expression `regex` in `input` with the Regular Expression `replace`.</span>
<code>regexp_replace(input: <a href="string.html">string</a>, regex: <a href="string.html">string</a>, replace: <a href="string.html">string</a>, flags: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Replaces matches for the regular expression `regex` in `input` with the regular expression `replace` using `flags`.

CockroachDB supports the following flags:

| Flag           | Description                                                       |
|----------------|-------------------------------------------------------------------|
| **c**          | Case-sensitive matching                                           |
| **i**          | Global matching (match each substring instead of only the first). |
| **m** or **n** | Newline-sensitive (see below)                                     |
| **p**          | Partial newline-sensitive matching (see below)                    |
| **s**          | Newline-insensitive (default)                                     |
| **w**          | Inverse partial newline-sensitive matching (see below)            |

| Mode | `.` and `[^...]` match newlines | `^` and `$` match line boundaries|
|------|----------------------------------|--------------------------------------|
| s    | yes                              | no                                   |
| w    | yes                              | yes                                  |
| p    | no                               | no                                   |
| m/n  | no                               | yes                                  |</span>
<code>repeat(input: <a href="string.html">string</a>, repeat_counter: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Concatenates `input` `repeat_counter` number of times.

For example, `repeat('dog', 2)` returns `dogdog`.</span>
<code>replace(input: <a href="string.html">string</a>, find: <a href="string.html">string</a>, replace: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Replaces all occurrences of `find` with `replace` in `input`</span>
<code>reverse(val: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Reverses the order of the string's characters.</span>
<code>right(input: <a href="bytes.html">bytes</a>, return_set: <a href="int.html">int</a>) &rarr; <a href="bytes.html">bytes</a></code> | <span class="funcdesc">Returns the last `return_set` bytes from `input`.</span>
<code>right(input: <a href="string.html">string</a>, return_set: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Returns the last `return_set` characters from `input`.</span>
<code>rtrim(input: <a href="string.html">string</a>, trim_chars: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Removes any characters included in `trim_chars` from the end (right-hand side) of `input` (applies recursively). 

For example, `rtrim('doggie', 'ei')` returns `dogg`.</span>
<code>rtrim(val: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Removes all spaces from the end (right-hand side) of `val`.</span>
<code>sha1(<a href="bytes.html">bytes</a>...) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Calculates the SHA1 hash value of a set of values.</span>
<code>sha1(<a href="string.html">string</a>...) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Calculates the SHA1 hash value of a set of values.</span>
<code>sha256(<a href="bytes.html">bytes</a>...) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Calculates the SHA256 hash value of a set of values.</span>
<code>sha256(<a href="string.html">string</a>...) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Calculates the SHA256 hash value of a set of values.</span>
<code>sha512(<a href="bytes.html">bytes</a>...) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Calculates the SHA512 hash value of a set of values.</span>
<code>sha512(<a href="string.html">string</a>...) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Calculates the SHA512 hash value of a set of values.</span>
<code>split_part(input: <a href="string.html">string</a>, delimiter: <a href="string.html">string</a>, return_index_pos: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Splits `input` on `delimiter` and return the value in the `return_index_pos`  position (starting at 1). 

For example, `split_part('123.456.789.0','.',3)`returns `789`.</span>
<code>strpos(input: <a href="string.html">string</a>, find: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">Calculates the position where the string `find` begins in `input`. 

For example, `strpos('doggie', 'gie')` returns `4`.</span>
<code>substr(input: <a href="string.html">string</a>, regex: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Returns a substring of `input` that matches the regular expression `regex`.</span>
<code>substr(input: <a href="string.html">string</a>, regex: <a href="string.html">string</a>, escape_char: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Returns a substring of `input` that matches the regular expression `regex` using `escape_char` as your escape character instead of `\`.</span>
<code>substr(input: <a href="string.html">string</a>, start_pos: <a href="int.html">int</a>, end_pos: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Returns a substring of `input` between `start_pos` and `end_pos` (count starts at 1).</span>
<code>substr(input: <a href="string.html">string</a>, substr_pos: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Returns a substring of `input` starting at `substr_pos` (count starts at 1).</span>
<code>substring(input: <a href="string.html">string</a>, regex: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Returns a substring of `input` that matches the regular expression `regex`.</span>
<code>substring(input: <a href="string.html">string</a>, regex: <a href="string.html">string</a>, escape_char: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Returns a substring of `input` that matches the regular expression `regex` using `escape_char` as your escape character instead of `\`.</span>
<code>substring(input: <a href="string.html">string</a>, start_pos: <a href="int.html">int</a>, end_pos: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Returns a substring of `input` between `start_pos` and `end_pos` (count starts at 1).</span>
<code>substring(input: <a href="string.html">string</a>, substr_pos: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Returns a substring of `input` starting at `substr_pos` (count starts at 1).</span>
<code>to_english(val: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">This function enunciates the value of its argument using English cardinals.</span>
<code>to_hex(val: <a href="bytes.html">bytes</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Converts `val` to its hexadecimal representation.</span>
<code>to_ip(val: <a href="string.html">string</a>) &rarr; <a href="bytes.html">bytes</a></code> | <span class="funcdesc">Converts the character string representation of an IP to its byte string representation.</span>
<code>to_uuid(val: <a href="string.html">string</a>) &rarr; <a href="bytes.html">bytes</a></code> | <span class="funcdesc">Converts the character string representation of a UUID to its byte string representation.</span>
<code>translate(input: <a href="string.html">string</a>, find: <a href="string.html">string</a>, replace: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">In `input`, replaces the first character from `find` with the first character in `replace`; repeat for each character in `find`. 

For example, `translate('doggie', 'dog', '123');` returns `1233ie`.</span>
<code>upper(val: <a href="string.html">string</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Converts all characters in `val` to their to their upper-case equivalents.</span>

### System Info Functions

Function &rarr; Returns | Description
--- | ---
<code>cluster_logical_timestamp() &rarr; <a href="decimal.html">decimal</a></code> | <span class="funcdesc">This function is used only by CockroachDB's developers for testing purposes.</span>
<code>crdb_internal.cluster_id() &rarr; uuid</code> | <span class="funcdesc">Returns the cluster ID.</span>
<code>crdb_internal.force_error(errorCode: <a href="string.html">string</a>, msg: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">This function is used only by CockroachDB's developers for testing purposes.</span>
<code>crdb_internal.force_log_fatal(msg: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">This function is used only by CockroachDB's developers for testing purposes.</span>
<code>crdb_internal.force_panic(msg: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">This function is used only by CockroachDB's developers for testing purposes.</span>
<code>crdb_internal.force_retry(val: <a href="interval.html">interval</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">This function is used only by CockroachDB's developers for testing purposes.</span>
<code>crdb_internal.force_retry(val: <a href="interval.html">interval</a>, txnID: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">This function is used only by CockroachDB's developers for testing purposes.</span>
<code>crdb_internal.no_constant_folding(input: anyelement) &rarr; anyelement</code> | <span class="funcdesc">This function is used only by CockroachDB's developers for testing purposes.</span>
<code>crdb_internal.set_vmodule(vmodule_string: <a href="string.html">string</a>) &rarr; <a href="int.html">int</a></code> | <span class="funcdesc">This function is used for internal debugging purposes. Incorrect use can severely impact performance.</span>
<code>current_database() &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Returns the current database.</span>
<code>current_schema() &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Returns the current schema. This function is provided for compatibility with PostgreSQL. For a new CockroachDB application, consider using current_database() instead.</span>
<code>current_schemas(include_pg_catalog: <a href="bool.html">bool</a>) &rarr; <a href="string.html">string</a>[]</code> | <span class="funcdesc">Returns the current search path for unqualified names.</span>
<code>current_user() &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Returns the current user. This function is provided for compatibility with PostgreSQL.</span>
<code>version() &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Returns the node's version of CockroachDB.</span>

### Compatibility Functions

Function &rarr; Returns | Description
--- | ---
<code>format_type(type_oid: oid, typemod: <a href="int.html">int</a>) &rarr; <a href="string.html">string</a></code> | <span class="funcdesc">Returns the SQL name of a data type that is identified by its type OID and possibly a type modifier. Currently, the type modifier is ignored.</span>
<code>generate_series(start: <a href="int.html">int</a>, end: <a href="int.html">int</a>) &rarr; setof tuple{int}</code> | <span class="funcdesc">Produces a virtual table containing the integer values from `start` to `end`, inclusive.</span>
<code>generate_series(start: <a href="int.html">int</a>, end: <a href="int.html">int</a>, step: <a href="int.html">int</a>) &rarr; setof tuple{int}</code> | <span class="funcdesc">Produces a virtual table containing the integer values from `start` to `end`, inclusive, by increment of `step`.</span>
<code>oid(int: <a href="int.html">int</a>) &rarr; oid</code> | <span class="funcdesc">Converts an integer to an OID.</span>
<code>unnest(input: anyelement[]) &rarr; anyelement</code> | <span class="funcdesc">Returns the input array as a set of rows</span>

