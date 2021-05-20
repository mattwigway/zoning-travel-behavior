-- Assign each of the MS Buildings buildings to a parcel

ALTER TABLE diss.buildings DROP COLUMN IF EXISTS parcel_gid;
ALTER TABLE diss.buildings ADD COLUMN parcel_gid int;

UPDATE diss.buildings b SET parcel_gid = p.gid
    FROM diss.gp16 p
    -- http://lin-ear-th-inking.blogspot.com/2007/06/subtleties-of-ogc-covers-spatial.html
    -- cast to geometry to avoid what appears to be a postgis bug: no spatial operator found for opfamily 17091 strategy 7
    WHERE p.geog && b.geog -- force spatial index usage, ST_Covers can't use indices due to geometry casts
        AND _ST_Covers(p.geog::geometry, ST_Centroid(b.geog)::geometry);

CREATE INDEX buildings_parcel_gid_idx ON diss.buildings (parcel_gid);