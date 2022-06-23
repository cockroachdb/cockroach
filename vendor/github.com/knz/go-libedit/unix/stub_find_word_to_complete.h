/*-
 * Copyright (c) 1997 The NetBSD Foundation, Inc.
 * All rights reserved.
 *
 * This code is derived from software contributed to The NetBSD Foundation
 * by Jaromir Dolecek.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE NETBSD FOUNDATION, INC. AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE FOUNDATION OR CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

// These functions are auxiliaries for the code in
// stub_fn_complete.i.

static wchar_t *
find_word_to_complete(const wchar_t * cursor, const wchar_t * buffer,
    const wchar_t * word_break, const wchar_t * special_prefixes, size_t * length)
{
	/* We now look backwards for the start of a filename/variable word */
	const wchar_t *ctemp = cursor;
	int cursor_at_quote;
	size_t len;
	wchar_t *temp;

	/* if the cursor is placed at a slash or a quote, we need to find the
	 * word before it
	 */
	if (ctemp > buffer) {
		switch (ctemp[-1]) {
		case '\\':
		case '\'':
		case '"':
			cursor_at_quote = 1;
			ctemp--;
			break;
		default:
			cursor_at_quote = 0;
		}
	} else
		cursor_at_quote = 0;

	while (ctemp > buffer
	    && !wcschr(word_break, ctemp[-1])
	    && (!special_prefixes || !wcschr(special_prefixes, ctemp[-1])))
		ctemp--;

	len = (size_t) (cursor - ctemp - cursor_at_quote);
	temp = malloc((len + 1) * sizeof(*temp));
	if (temp == NULL)
		return NULL;
	(void) wcsncpy(temp, ctemp, len);
	temp[len] = '\0';
	if (cursor_at_quote)
		len++;
	*length = len;
	return temp;
}
