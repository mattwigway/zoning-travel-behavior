CREATE TABLE diss.setback_buffers AS
    SELECT way_id, seg_id, median_setback_meters, ST_Multi(ST_Buffer(geog, median_setback_meters)::geometry)::geography AS geog
    FROM diss.osm_segments;

ALTER TABLE diss.setback_buffers ALTER COLUMN geog TYPE geography(MultiPolygon,4326);
