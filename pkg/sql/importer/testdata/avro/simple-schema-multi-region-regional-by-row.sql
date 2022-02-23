-- noinspection SqlDialectInspectionForFile

-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE public.simple (
    i integer PRIMARY KEY,
    s text,
    b bytea
) LOCALITY REGIONAL BY ROW;

