// This is a stub provided for compatibility with macOS Mojave and
// other possible systems that have decided they didn't want to export
// the true fn_complete().
// This stub does not perform filename completion and does not
// offer a selection menu. It also does not offer compatibility
// with readline() (via the completion_type argument).

#include "stub_find_word_to_complete.i"

static char *multibyte_to_singlebyte(wchar_t *line);
static wchar_t *singlebyte_to_multibyte(char *line);

static
int fn_complete(
	EditLine *el,
	char *(*complet_func)(const char *, int), // ignored
	char **(*attempted_completion_function)(const char *, int, int),
	const wchar_t *word_break,
	const wchar_t *special_prefixes, // ignored
	const char *(*app_func)(const char *), // ignored
	size_t query_items,
	int *completion_type, // ignored
	int *over, // ignored
	int *point, // ignored
	int *end, // ignored
	const wchar_t *(*find_word_start_func)(const wchar_t *, const wchar_t *), // ignored
	wchar_t *(*dequoting_func)(const wchar_t *), // ignored
	char *(*quoting_func)(const char *) // ignored
	)
{
	char *completion_prefix;
	size_t completion_prefix_len;
	wchar_t *temp;
	int cur_off;
	char **matches;
	int retval = CC_NORM;

	if (!attempted_completion_function)
		/* nothing to do. We don't support file completion here. */
		goto out;

	const LineInfoW *li = el_wline(el);

	/*
	 * isolate the completion prefix.
	 */
	temp = find_word_to_complete(
		li->cursor,
		li->buffer, word_break, special_prefixes,
		&completion_prefix_len);
	if (temp == NULL)
		/* nothing to do. */
		goto out;

	completion_prefix = multibyte_to_singlebyte(temp);

	/*
	 * compute the completion matches.
	 */

	/* cursor offset from begin of the line. */
	cur_off = (int)(li->cursor - li->buffer);
	/* compute the completion matches. */
	matches = (*attempted_completion_function)(
		completion_prefix, cur_off - (int)completion_prefix_len, cur_off);

	if (!matches)
		/* nothing to do any more. */
		goto out;

	int single_match = matches[2] == NULL &&
		(matches[1] == NULL || strcmp(matches[0], matches[1]) == 0);

	if (matches[0][0] != '\0') {
		/* clear the completion prefix. */
		el_deletestr(el, (int) completion_prefix_len);

		/* compute the completion to display. */
		wchar_t *completion = singlebyte_to_multibyte(matches[0]);
		if (!completion)
			goto out;

		/* print it. */
		el_winsertstr(el, completion);
		free(completion);

		if (single_match)
			/* insert a space after the match. */
			el_winsertstr(el, L" ");

		/* show the completion result upon returning. */
		retval = CC_REFRESH;
	}
	if (!single_match && matches[0][0])
		/* some common match but not complete match.
		 * inform the user the input is still incomplete. */
		el_beep(el);

out:
	/*
	 * release resources and complete.
	 */
	if (matches) {
		for (size_t i = 0; matches[i]; i++)
			free(matches[i]);
		free(matches);
	}
	if (completion_prefix) {
		free(completion_prefix);
	}
	if (temp) {
		free(temp);
	}
	return retval;
}

char *multibyte_to_singlebyte(wchar_t *s) {
	char *buf = NULL;
	size_t i = 0;
	size_t bufsz = 0;
	do {
		/* the +1 below is to make space for the final NUL byte */
		if (i + MB_LEN_MAX + 1 >= bufsz) {
			/* out of space. */
			bufsz = (bufsz == 0 ? 1024 : bufsz * 2);
			char *newbuf = realloc(buf, bufsz);
			if (newbuf == NULL) {
				free(buf);
				return NULL;
			}
			buf = newbuf;
		}
		if (!*s)
			/* nothing remaining to convert. */
			break;

		/* convert one character */
		int l = wctomb(buf + i, *s);
		if (l < 0) {
			/* reset the converter */
			wctomb(NULL, L'\0');
			free(buf);
			return NULL;
		}
		i += l;
		s++;
	} while(1);
	buf[i] = '\0';
	return buf;
}

wchar_t *singlebyte_to_multibyte(char *s) {
	/* compute the multibyte len. */
	size_t len = mbstowcs(NULL, s, 0);
	if (len == (size_t)-1)
		return NULL;

	/* make space. len does not count the final NUL so we need to add it here. */
	len++;
	wchar_t * r = malloc(len * sizeof(wchar_t));
	if (r)
		/* do the actual conversion. */
		mbstowcs(r, s, len);
	return r;
}
