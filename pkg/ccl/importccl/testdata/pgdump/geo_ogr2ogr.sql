BEGIN;
CREATE TABLE "public"."HydroNode" (    "fid" SERIAL,    CONSTRAINT "HydroNode_pk" PRIMARY KEY ("fid") );
SELECT AddGeometryColumn('public','HydroNode','geom',4326,'POINT',2);
ALTER TABLE "public"."HydroNode" ADD COLUMN "id" VARCHAR(38) NOT NULL;
ALTER TABLE "public"."HydroNode" ADD COLUMN "hydroNodeCategory" VARCHAR(20) NOT NULL;
INSERT INTO "public"."HydroNode" ("geom" , "fid" , "id", "hydroNodeCategory") VALUES ('0101000020E6100000B81E85EB735B1C41333333F31D8F3241', 1, 'id41F90EF5-E9DE-48D0-977F-7599CDA3B5BA', 'source');
SELECT setval(pg_get_serial_sequence('"public"."HydroNode"', 'fid'), max("fid")) FROM "public"."HydroNode";
COMMIT;
