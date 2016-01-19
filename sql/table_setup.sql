CREATE DATABASE otbp
  WITH ENCODING='UTF8'
       OWNER=postgres
       CONNECTION LIMIT=-1;


\c otbp

CREATE SCHEMA gis AUTHORIZATION postgres;

CREATE EXTENSION postgis;

DROP TABLE IF EXISTS "gis"."places";
CREATE TABLE "gis"."places" (gid serial,
  "statefp" varchar(2),
  "placefp" varchar(5),
  "placens" varchar(8),
  "geoid" varchar(7),
  "name" varchar(100),
  "namelsad" varchar(100),
  "lsad" varchar(2),
  "classfp" varchar(2),
  "pcicbsa" varchar(1),
  "pcinecta" varchar(1),
  "mtfcc" varchar(5),
  "funcstat" varchar(1),
  "aland" float8,
  "awater" float8,
  "intptlat" varchar(11),
  "intptlon" varchar(12)
);

ALTER TABLE "gis"."places" ADD PRIMARY KEY (gid);
SELECT AddGeometryColumn('gis','places','geom','4269','MULTIPOLYGON',2);
CREATE INDEX places_geom_index ON gis.places USING GIST (geom);
CREATE INDEX ON gis.places(gid);

DROP TABLE IF EXISTS "gis"."roads";
CREATE TABLE "gis"."roads" (gid serial,
  "linearid" varchar(22),
  "fullname" varchar(100),
  "rttyp" varchar(1),
  "mtfcc" varchar(5)
);
ALTER TABLE "gis"."roads" ADD PRIMARY KEY (gid);
SELECT AddGeometryColumn('gis','roads','geom','4269','MULTILINESTRING',2);
CREATE INDEX roads_geom_index ON gis.roads USING GIST (geom);
CREATE INDEX roads_linearid_index ON gis.roads(linearid);

CREATE TABLE gis.user_routes(
  route_id text,
  step_id integer,
  step_name text,
  entry_type character varying(10),
  starting_point geometry,
  geom_length_meters double precision DEFAULT 0,
  geom_centroid geometry,
  geom_extent geometry,
  geom geometry(LineString,4269),
  last_accessed timestamp without time zone
);
CREATE INDEX ON gis.user_routes USING GIST(geom);
CREATE INDEX ON gis.user_routes(route_id);

CREATE VIEW gis.user_routes_geoserver AS
SELECT route_id, step_id, geom
FROM gis.user_routes
WHERE entry_type = 'STEP';

VACUUM FULL gis.places;
VACUUM FULL gis.roads;
