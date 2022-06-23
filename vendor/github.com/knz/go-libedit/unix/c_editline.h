// Copyright 2017 Raphael 'kena' Poss
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

#ifndef GO_LIBEDIT_H
#define GO_LIBEDIT_H

#include <histedit.h>
#include <stdio.h>

typedef char* pchar;

EditLine* go_libedit_init(int id, char *appName, void **sigcfg,
			  FILE* fin, FILE* fout, FILE *ferr);
void go_libedit_close(EditLine *el, void *sigcfg);
void go_libedit_rebind_ctrls(EditLine *el);

extern char *go_libedit_emptycstring;
extern const char* go_libedit_mode_read;
extern const char* go_libedit_mode_write;
extern const char* go_libedit_mode_append;
extern const char *go_libedit_locale1;
extern const char *go_libedit_locale2;

struct clientdata* go_libedit_get_clientdata(EditLine *el);
int go_libedit_set_clientdata(EditLine *el, struct clientdata cd);
void go_libedit_set_string_array(char **ar, int p, char *s);

void *go_libedit_gets(EditLine *el, char *lprompt, char *rprompt,
		      void *sigcfg, int *count, int *interrupted, int wc);

History* go_libedit_setup_history(EditLine *el, int maxEntries, int dedup);
int go_libedit_read_history(History *h, char *filename);
int go_libedit_write_history(History *h, char *filename);
int go_libedit_add_history(History *h, char *line);

// Go-generated via //export
char *go_libedit_prompt_left(EditLine *el);
char *go_libedit_prompt_right(EditLine *el);
char **go_libedit_getcompletions(int instance, char *word);


#endif
