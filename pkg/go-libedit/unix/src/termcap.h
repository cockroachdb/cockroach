#ifndef FAKE_TERMCAP_H
#define FAKE_TERMCAP_H

// This header file is provided so as to avoid a dependency on the
// ncurses development headers. These are the only functions used by
// libedit.

extern char * tgetstr (const char *, char **);
extern char * tgoto (const char *, int, int);
extern int tgetent (char *, const char *);
extern int tgetflag (const char *);
extern int tgetnum (const char *);
extern int tputs (const char *, int, int (*)(int));


#endif
