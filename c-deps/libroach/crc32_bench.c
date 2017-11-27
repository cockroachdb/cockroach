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

#include "crc32.h"
#include "testutil.h"

#include <err.h>
#include <inttypes.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

struct experiment {
    size_t len;
    size_t offset;
    uint64_t rounds;
};

struct experiment experiments[] = {
    // Experiments to match Go's hash/crc32 package.
    {15, 0, 30000000},
    {15, 1, 30000000},
    {40, 0, 30000000},
    {40, 1, 30000000},
    {512, 0, 20000000},
    {512, 1, 20000000},
    {1 << 10, 0, 20000000},
    {1 << 10, 1, 20000000},
    {4 << 10, 0, 5000000},
    {4 << 10, 1, 5000000},
    {32 << 10, 0, 500000},
    {32 << 10, 1, 500000},

    // Experiments to match Fastmail's CRC32 roundup.
    // See: https://github.com/robn/crc32-bench.
    {16, 0, 100000000},
    {24, 0, 100000000},
    {32, 0, 100000000},
    {64, 0, 100000000},
    {128, 0, 100000000},
    {256, 0, 100000000},
    {64 << 20, 0, 1000},
};

enum output_formats {
    FORMAT_DEFAULT,
    FORMAT_GO,
};

static int64_t
benchmark(const unsigned char* buf, size_t len, uint64_t rounds) {
    struct timeval start, end;

    gettimeofday(&start, 0);
    for (int i = 0; i < rounds; i++) {
        volatile uint32_t crc = crc32ieee(0, buf, len);
    }
    gettimeofday(&end, 0);

    struct timeval tv;
    timersub(&end, &start, &tv);

    return ((int64_t) tv.tv_sec * 1000000 + tv.tv_usec);
}

static void __attribute__((noreturn))
usage(void) {
    fprintf(stderr, "usage: crc32_bench [--format=<go|default>]\n");
    exit(1);
}

int
main(int argc, char* argv[]) {
    enum output_formats format = FORMAT_DEFAULT;
    static struct option long_options[] = {
        {"format", required_argument, NULL, 'f'},
        {0, 0, 0, 0},
    };
    int ch;
    while ((ch = getopt_long(argc, argv, "f:", long_options, NULL)) != -1) {
        if (ch == 'f') {
            if (strncmp(optarg, "go", 2) == 0) {
                format = FORMAT_GO;
            } else if (strncmp(optarg, "default", 7) != 0) {
                usage();
            }
        } else {
            usage();
        }
    }
    if (argc - optind != 0) {
        warnx("unexpected positional arguments");
        usage();
    }
    argv += optind;

    for (size_t i = 0; i < ARRAY_SIZE(experiments); i++) {
        struct experiment *e = &experiments[i];
        unsigned char* buf = make_bytes(e->len + e->offset);
        uint64_t total_us = benchmark(buf + e->offset, e->len, e->rounds);
        double avg_ns = total_us * 1e3 / e->rounds;
        double throughput = (e->len * e->rounds) * 1e6 / total_us / (1 << 20);
        char* len_units = (format == FORMAT_DEFAULT) ? "B" : "";
        if (e->len >= 1024) {
            e->len /= 1024;
            len_units = "kB";
        }
        if (e->len >= 1024) {
            e->len /= 1024;
            len_units = "mB";
        }
        if (format == FORMAT_GO) {
            printf("BenchmarkCRC32/poly=IEEE/size=%zu%s/align=%zu-8\t%9" PRIu64"\t%0.1f ns/op\t%0.2f MB/s\n",
                e->len, len_units, e->offset, e->rounds, avg_ns, throughput);
        } else {
            printf("%zu%s x %" PRIu64 ": %.2fs total, %.2f avg\n",
                e->len, len_units, e->rounds,
                total_us / 1e6, avg_ns);
        }
        free(buf);
    }
    return 0;
}
