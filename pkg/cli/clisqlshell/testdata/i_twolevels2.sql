SELECT 123;

-- check the relative include
\ir i_twolevels3.sql

-- check that disabling echo here spills into the first level.
\unset echo
