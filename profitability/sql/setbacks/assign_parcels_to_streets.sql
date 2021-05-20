ALTER TABLE diss.gp16 DROP COLUMN IF EXISTS way_id;
ALTER TABLE diss.gp16 ADD COLUMN way_id bigint;
ALTER TABLE diss.gp16 DROP COLUMN IF EXISTS seg_id;
ALTER TABLE diss.gp16 ADD COLUMN seg_id bigint;

CREATE IND
EX segments_gix ON diss.osm_segments USING GIST (geog);

-- use a window function to get top 1 without a correlated subquery
-- https://stackoverflow.com/questions/4556653/is-there-something-equivalent-to-argmax-in-sql
CREATE TEMPORARY TABLE nearest_street AS SELECT outside.way_id, outside.seg_id, outside.gid, outside.setback FROM
    (SELECT inside.*, ROW_NUMBER() OVER (PARTITION BY inside.gid ORDER BY inside.setback) AS rnk FROM 
        (SELECT s.way_id, s.seg_id, b.gid, ST_Distance(b.geog, s.geog) AS setback
            FROM diss.osm_segments s
            LEFT JOIN diss.gp16 b
            ON (ST_DWithin(s.geog, b.geog, 50))) inside
        ) outside
     WHERE outside.rnk = 1; -- ROW_NUMBER is one-based

UPDATE diss.gp16 SET way_id = n.way_id, seg_id = n.seg_id
    FROM nearest_street n
    WHERE (n.gid = gp16.gid);
