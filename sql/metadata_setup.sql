DROP TABLE IF EXISTS gis.roads_intersection;

SELECT  r1.linearid AS r1id,
        r1.fullname AS r1name,
        r2.linearid AS r2id,
        r2.fullname AS r2name,
        ST_AsText(ST_Intersection(r1.geom, r2.geom)) AS intersection_point,
        ST_Length(r1.geom, true) AS r1len,
        ST_Length(r2.geom, true) AS r2len,
        ST_Intersection(r1.geom, r2.geom) AS geom
INTO TABLE gis.roads_intersection
FROM gis.roads r1
INNER JOIN gis.roads r2 ON ((ST_Touches(r1.geom, r2.geom) OR ST_Intersects(r1.geom, r2.geom))
    AND GeometryType(ST_Intersection(r1.geom, r2.geom)) = 'POINT'
    AND r1.linearid != r2.linearid
    AND r2.rttyp NOT IN ('I')
    AND NOT ST_Equals(r1.geom, r2.geom))
WHERE r1.rttyp NOT IN ('I');

DROP TABLE IF EXISTS gis.places_intersection;
DROP TABLE IF EXISTS gis.temp_max_length;
CREATE INDEX ON gis.roads_intersection(r1id);
CREATE INDEX ON gis.roads_intersection(r2id);
CREATE INDEX ON gis.roads_intersection USING GIST (geom);
VACUUM FULL;

SELECT p.gid, MAX(ST_Length(ST_Intersection(p.geom, r.geom), true)) as max_length
INTO TABLE gis.temp_max_length
FROM gis.places p
INNER JOIN gis.roads r ON ST_Intersects(p.geom, r.geom)
INNER JOIN gis.roads_intersection ri ON (r.linearid = ri.r1id OR r.linearid = ri.r2id)
WHERE p.lsad = '25'
AND r.rttyp NOT IN ('I', 'M')
GROUP BY p.gid;

CREATE INDEX ON gis.temp_max_length(gid);
CREATE INDEX ON gis.temp_max_length(max_length);
VACUUM FULL;

SELECT p.gid, statefp, placens, name, r.linearid, r.fullname,
    ST_AsText(ST_Centroid(ST_Intersection(p.geom, r.geom))) as location,
    ST_Centroid(ST_Intersection(p.geom, r.geom)) as geom
INTO TABLE gis.places_intersection
FROM gis.places p
INNER JOIN gis.roads r ON ST_Intersects(p.geom, r.geom)
INNER JOIN gis.temp_max_length ml
    ON (ST_Length(ST_Intersection(p.geom, r.geom), true) = ml.max_length AND p.gid = ml.gid)
WHERE p.lsad = '25'
AND r.rttyp NOT IN ('I', 'M');

VACUUM FULL;
