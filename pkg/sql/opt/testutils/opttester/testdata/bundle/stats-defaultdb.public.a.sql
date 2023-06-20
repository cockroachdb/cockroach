ALTER TABLE public.a INJECT STATISTICS '[
    {
        "columns": [
            "a"
        ],
        "created_at": "2021-07-23 21:17:16.83267",
        "distinct_count": 1000,
        "histo_buckets": [
            {
                "distinct_range": 0,
                "num_eq": 1,
                "num_range": 0,
                "upper_bound": "1"
            },
            {
                "distinct_range": 4.919568895628329,
                "num_eq": 1,
                "num_range": 5,
                "upper_bound": "1000"
            }
        ],
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 1000
    },
    {
        "columns": [
            "b"
        ],
        "created_at": "2021-07-23 21:17:16.83267",
        "distinct_count": 1000,
        "histo_buckets": [
            {
                "distinct_range": 0,
                "num_eq": 1,
                "num_range": 0,
                "upper_bound": "8"
            },
            {
                "distinct_range": 998,
                "num_eq": 1,
                "num_range": 998,
                "upper_bound": "1007"
            }
        ],
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 1000
    }
]';
