-- Table of distances from every building to every nearby street segment
CREATE TABLE diss.building_segment_distance AS
    SELECT way_id, seg_id, ogc_fid AS building_id, parcel_gid, ST_Distance(b.geog, s.geog) AS setback_meters
    FROM diss.osm_segments s
    CROSS JOIN diss.buildings b
    WHERE ST_DWithin(b.geog, s.geog, 30); -- 50 meters
