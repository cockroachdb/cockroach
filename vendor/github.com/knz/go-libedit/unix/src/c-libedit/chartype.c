/*	$NetBSD: chartype.c,v 1.31 2017/01/09 02:54:18 christos Exp $	*/

/*-
 * Copyright (c) 2009 The NetBSD Foundation, Inc.
 * All rights reserved.
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

/*
 * chartype.c: character classification and meta information
 */
#include "config.h"
#if !defined(lint) && !defined(SCCSID)
__RCSID("$NetBSD: chartype.c,v 1.31 2017/01/09 02:54:18 christos Exp $");
#endif /* not lint && not SCCSID */

#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#include "el.h"

#define CT_BUFSIZ ((size_t)1024)

static int ct_conv_cbuff_resize(ct_buffer_t *, size_t);
static int ct_conv_wbuff_resize(ct_buffer_t *, size_t);

static int
ct_conv_cbuff_resize(ct_buffer_t *conv, size_t csize)
{
	void *p;

	if (csize <= conv->csize)
		return 0;

	conv->csize = csize;

	p = el_realloc(conv->cbuff, conv->csize * sizeof(*conv->cbuff));
	if (p == NULL) {
		conv->csize = 0;
		el_free(conv->cbuff);
		conv->cbuff = NULL;
		return -1;
	}
	conv->cbuff = p;
	return 0;
}

static int
ct_conv_wbuff_resize(ct_buffer_t *conv, size_t wsize)
{
	void *p;

	if (wsize <= conv->wsize)
		return 0;

	conv->wsize = wsize;

	p = el_realloc(conv->wbuff, conv->wsize * sizeof(*conv->wbuff));
	if (p == NULL) {
		conv->wsize = 0;
		el_free(conv->wbuff);
		conv->wbuff = NULL;
		return -1;
	}
	conv->wbuff = p;
	return 0;
}


char *
ct_encode_string(const wchar_t *s, ct_buffer_t *conv)
{
	char *dst;
	ssize_t used;

	if (!s)
		return NULL;

	dst = conv->cbuff;
	for (;;) {
		used = (ssize_t)(dst - conv->cbuff);
		if ((conv->csize - (size_t)used) < 5) {
			if (ct_conv_cbuff_resize(conv,
			    conv->csize + CT_BUFSIZ) == -1)
				return NULL;
			dst = conv->cbuff + used;
		}
		if (!*s)
			break;
		used = ct_encode_char(dst, (size_t)5, *s);
		if (used == -1) /* failed to encode, need more buffer space */
			abort();
		++s;
		dst += used;
	}
	*dst = '\0';
	return conv->cbuff;
}

wchar_t *
ct_decode_string(const char *s, ct_buffer_t *conv)
{
	size_t len;

	if (!s)
		return NULL;

	len = mbstowcs(NULL, s, (size_t)0);
	if (len == (size_t)-1)
		return NULL;

	if (conv->wsize < ++len)
		if (ct_conv_wbuff_resize(conv, len + CT_BUFSIZ) == -1)
			return NULL;

	mbstowcs(conv->wbuff, s, conv->wsize);
	return conv->wbuff;
}


libedit_private wchar_t **
ct_decode_argv(int argc, const char *argv[], ct_buffer_t *conv)
{
	size_t bufspace;
	int i;
	wchar_t *p;
	wchar_t **wargv;
	ssize_t bytes;

	/* Make sure we have enough space in the conversion buffer to store all
	 * the argv strings. */
	for (i = 0, bufspace = 0; i < argc; ++i)
		bufspace += argv[i] ? strlen(argv[i]) + 1 : 0;
	if (conv->wsize < ++bufspace)
		if (ct_conv_wbuff_resize(conv, bufspace + CT_BUFSIZ) == -1)
			return NULL;

	wargv = el_malloc((size_t)(argc + 1) * sizeof(*wargv));

	for (i = 0, p = conv->wbuff; i < argc; ++i) {
		if (!argv[i]) {   /* don't pass null pointers to mbstowcs */
			wargv[i] = NULL;
			continue;
		} else {
			wargv[i] = p;
			bytes = (ssize_t)mbstowcs(p, argv[i], bufspace);
		}
		if (bytes == -1) {
			el_free(wargv);
			return NULL;
		} else
			bytes++;  /* include '\0' in the count */
		bufspace -= (size_t)bytes;
		p += bytes;
	}
	wargv[i] = NULL;

	return wargv;
}


libedit_private size_t
ct_enc_width(wchar_t c)
{
	/* UTF-8 encoding specific values */
	if (c < 0x80)
		return 1;
	else if (c < 0x0800)
		return 2;
	else if (c < 0x10000)
		return 3;
	else if (c < 0x110000)
		return 4;
	else
		return 0; /* not a valid codepoint */
}

libedit_private ssize_t
ct_encode_char(char *dst, size_t len, wchar_t c)
{
	ssize_t l = 0;
	if (len < ct_enc_width(c))
		return -1;
	l = wctomb(dst, c);

	if (l < 0) {
		wctomb(NULL, L'\0');
		l = 0;
	}
	return l;
}

libedit_private const wchar_t *
ct_visual_string(const wchar_t *s, ct_buffer_t *conv)
{
	wchar_t *dst;
	ssize_t used;

	if (!s)
		return NULL;

	if (ct_conv_wbuff_resize(conv, CT_BUFSIZ) == -1)
		return NULL;

	used = 0;
	dst = conv->wbuff;
	while (*s) {
		used = ct_visual_char(dst,
		    conv->wsize - (size_t)(dst - conv->wbuff), *s);
		if (used != -1) {
			++s;
			dst += used;
			continue;
		}

		/* failed to encode, need more buffer space */
		used = dst - conv->wbuff;
		if (ct_conv_wbuff_resize(conv, conv->wsize + CT_BUFSIZ) == -1)
			return NULL;
		dst = conv->wbuff + used;
	}

	if (dst >= (conv->wbuff + conv->wsize)) { /* sigh */
		used = dst - conv->wbuff;
		if (ct_conv_wbuff_resize(conv, conv->wsize + CT_BUFSIZ) == -1)
			return NULL;
		dst = conv->wbuff + used;
	}

	*dst = L'\0';
	return conv->wbuff;
}



libedit_private int
ct_visual_width(wchar_t c)
{
	int t = ct_chr_class(c);
	switch (t) {
	case CHTYPE_ASCIICTL:
		return 2; /* ^@ ^? etc. */
	case CHTYPE_TAB:
		return 1; /* Hmm, this really need to be handled outside! */
	case CHTYPE_NL:
		return 0; /* Should this be 1 instead? */
	case CHTYPE_PRINT:
		return wcwidth(c);
	case CHTYPE_NONPRINT:
		if (c > 0xffff) /* prefer standard 4-byte display over 5-byte */
			return 8; /* \U+12345 */
		else
			return 7; /* \U+1234 */
	default:
		return 0; /* should not happen */
	}
}


libedit_private ssize_t
ct_visual_char(wchar_t *dst, size_t len, wchar_t c)
{
	int t = ct_chr_class(c);
	switch (t) {
	case CHTYPE_TAB:
	case CHTYPE_NL:
	case CHTYPE_ASCIICTL:
		if (len < 2)
			return -1;   /* insufficient space */
		*dst++ = '^';
		if (c == '\177')
			*dst = '?'; /* DEL -> ^? */
		else
			*dst = c | 0100;    /* uncontrolify it */
		return 2;
	case CHTYPE_PRINT:
		if (len < 1)
			return -1;  /* insufficient space */
		*dst = c;
		return 1;
	case CHTYPE_NONPRINT:
		/* we only use single-width glyphs for display,
		 * so this is right */
		if ((ssize_t)len < ct_visual_width(c))
			return -1;   /* insufficient space */
		*dst++ = '\\';
		*dst++ = 'U';
		*dst++ = '+';
#define tohexdigit(v) "0123456789ABCDEF"[v]
		if (c > 0xffff) /* prefer standard 4-byte display over 5-byte */
			*dst++ = tohexdigit(((unsigned int) c >> 16) & 0xf);
		*dst++ = tohexdigit(((unsigned int) c >> 12) & 0xf);
		*dst++ = tohexdigit(((unsigned int) c >>  8) & 0xf);
		*dst++ = tohexdigit(((unsigned int) c >>  4) & 0xf);
		*dst   = tohexdigit(((unsigned int) c      ) & 0xf);
		return c > 0xffff ? 8 : 7;
		/*FALLTHROUGH*/
	/* these two should be handled outside this function */
	default:            /* we should never hit the default */
		return 0;
	}
}




libedit_private int
ct_chr_class(wchar_t c)
{
	if (c == '\t')
		return CHTYPE_TAB;
	else if (c == '\n')
		return CHTYPE_NL;
	else if (c < 0x100 && iswcntrl(c))
		return CHTYPE_ASCIICTL;
	else if (iswprint(c))
		return CHTYPE_PRINT;
	else
		return CHTYPE_NONPRINT;
}
