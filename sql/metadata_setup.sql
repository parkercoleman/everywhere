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
    --AND r2.rttyp NOT IN ('I')
    AND NOT ST_Equals(r1.geom, r2.geom));
--WHERE r1.rttyp NOT IN ('I');

DROP TABLE IF EXISTS gis.places_intersection;
CREATE INDEX ON gis.roads_intersection(r1id);
CREATE INDEX ON gis.roads_intersection(r2id);
CREATE INDEX ON gis.roads_intersection USING GIST (geom);
VACUUM FULL;

DROP TABLE IF EXISTS gis.places_intersection;

WITH places AS(
	SELECT p.*, (CASE WHEN pp.geom IS NULL THEN ST_Centroid(p.geom) ELSE pp.geom END) AS centroid
	FROM gis.places p
	LEFT JOIN gis.places_point pp ON upper(p.name) = upper(pp.name) AND p.statefp = pp.state_fips
), distances AS (
	SELECT p.gid, p.statefp, p.placens, p.name, ri.r1id, ri.r2id, St_Distance(p.centroid, ri.geom) AS distance, ri.geom, MIN(St_Distance(p.centroid, ri.geom))  OVER (PARTITION BY p.gid)
	FROM places p
	INNER JOIN gis.roads_intersection ri ON ST_DWithin(ri.geom, p.geom, 0.3)
)

SELECT DISTINCT d.gid, d.name, MIN(r.linearid::bigint)::text AS linearid, ST_AsText(d.geom) AS location
INTO TABLE gis.places_intersection
FROM distances d
INNER JOIN gis.roads r ON d.r1id = r.linearid
WHERE d.distance = d.min
GROUP BY d.gid, d.name, location;

VACUUM FULL;
