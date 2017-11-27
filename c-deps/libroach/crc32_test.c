// Copyright 2017 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Nikhil Benesch (nikhil.benesch@gmail.com)

#define CRC32_TEST

#include "crc32.c"
#include "testutil.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

static int ntests;
static int nfailed;

struct test_case {
  uint32_t expected;
  char* data;
};

static struct test_case test_cases[] = {
  {0x0, ""},
  {0xe8b7be43, "a"},
  {0x9e83486d, "ab"},
  {0x352441c2, "abc"},
  {0xed82cd11, "abcd"},
  {0x8587d865, "abcde"},
  {0x4b8e39ef, "abcdef"},
  {0x312a6aa6, "abcdefg"},
  {0xaeef2a50, "abcdefgh"},
  {0x8da988af, "abcdefghi"},
  {0x3981703a, "abcdefghij"},
  {0x6b9cdfe7, "Discard medicine more than two years old."},
  {0xc90ef73f, "He who has a shady past knows that nice guys finish last."},
  {0xb902341f, "I wouldn't marry him with a ten foot pole."},
  {0x42080e8, "Free! Free!/A trip/to Mars/for 900/empty jars/Burma Shave"},
  {0x154c6d11, "The days of the digital watch are numbered.  -Tom Stoppard"},
  {0x4c418325, "Nepal premier won't resign."},
  {0x33955150, "For every action there is an equal and opposite government program."},
  {0x26216a4b, "His money is twice tainted: 'taint yours and 'taint mine."},
  {0x1abbe45e, "There is no reason for any individual to have a computer in their home. -Ken Olsen, 1977"},
  {0xc89a94f7, "It's a tiny change to the code and not completely disgusting. - Bob Manchek"},
  {0xab3abe14, "size:  a.out:  bad magic"},
  {0xbab102b6, "The major problem is with sendmail.  -Mark Horton"},
  {0x999149d7, "Give me a rock, paper and scissors and I will move the world.  CCFestoon"},
  {0x6d52a33c, "If the enemy is within range, then so are you."},
  {0x90631e8d, "It's well we cannot hear the screams/That we create in others' dreams."},
  {0x78309130, "You remind me of a TV show, but that's all right: I watch it anyway."},
  {0x7d0a377f, "C is as portable as Stonehedge!!"},
  {0x8c79fd79, "Even if I could be Shakespeare, I think I should still choose to be Faraday. - A. Huxley"},
  {0xa20b7167, "The fugacity of a constituent in a mixture of gases at a given temperature is proportional to its mole fraction.  Lewis-Randall Rule"},
  {0x8e0bb443, "How can you write a big system without C++?  -Paul Glick"},
};

static size_t cross_check_lens[] = {
  0, 1, 2, 3, 4, 5, 10, 16, 50, 63, 64, 65, 100, 127, 128, 129, 255, 256, 257,
  300, 312, 384, 416, 448, 480, 500, 501, 502, 503, 504, 505, 512, 513, 1000,
  1024, 2000, 4030, 4031, 4032, 4033, 4036, 4040, 4048, 4096, 5000, 10000,
  1 << 20,
};

static void
test_golden(const char* name, crc32_fn_t crc32_fn) {
  for (int i = 0; i < ARRAY_SIZE(test_cases); i++) {
    ntests++;

    struct test_case *tc = &test_cases[i];
    uint32_t actual = crc32_fn(0, (unsigned char *)tc->data, strlen(tc->data));
    if (actual != tc->expected) {
      nfailed++;
      printf("test_golden:%s:%02d: expected 0x%08x, actual 0x%08x\n",
        name, i, tc->expected, actual);
    }
  }
}

static void
test_cross_check(const char* name, crc32_fn_t crc32_fn1, crc32_fn_t crc32_fn2) {
  for (int i = 0; i < ARRAY_SIZE(cross_check_lens); i++) {
    ntests++;

    size_t len = cross_check_lens[i];
    unsigned char* buf = make_bytes(len);
    uint32_t crc1 = crc32_fn1(0, buf, len);
    uint32_t crc2 = crc32_fn2(0, buf, len);
    if (crc1 != crc2) {
      nfailed++;
      printf("test_cross_check:%s:%02d: 0x%08x != 0x%08x (%zu bytes)\n",
        name, i, crc1, crc2, len);
    }
    free(buf);
  }
}

static uint32_t
crc32_zlib(uint32_t crc, const unsigned char* buf, size_t len) {
  return crc32((unsigned long) crc, buf, (unsigned long) len);
}

int main() {
  test_golden("simple", crc32ieee_simple);
  test_golden("table", crc32ieee_table);
  test_golden("slicing8", crc32ieee_slicing8);
  test_golden("clmul", crc32ieee_clmul);
  test_golden("default", crc32ieee);

  test_cross_check("table vs. simple", crc32ieee_table, crc32ieee_simple);
  test_cross_check("slicing8 vs. simple", crc32ieee_slicing8, crc32ieee_simple);
  test_cross_check("clmul vs. simple", crc32ieee_clmul, crc32ieee_simple);
  test_cross_check("default vs. simple", crc32ieee, crc32ieee_simple);
  test_cross_check("default vs. zlib", crc32ieee, crc32_zlib);

  printf("%s (%d tests, %d failures)\n",
    (nfailed > 0 ? "FAIL" : "PASS"), ntests, nfailed);
  return nfailed > 0;
}
