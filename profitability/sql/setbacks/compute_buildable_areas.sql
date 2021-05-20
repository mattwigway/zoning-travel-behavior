-- Create geometries of the areas that are not within setbacks

CREATE TABLE diss.buildable_areas AS
SELECT p.gid, p.objectid, p.scaguid16, p.apn,
    -- -1.524m interior buffer, 5 foot setback all around, && is spatial index bbox overlap operator
    ST_Multi(
        ST_Difference(
            ST_MakeValid(
                ST_Buffer(p.geog, -1.524)::geometry
            ),
            -- ST_Difference with any geom null returns null. use coalesce so there is always a second geometry - in the gulf of guinea
            -- if no streets are near the parcel
            COALESCE((SELECT ST_MakeValid(ST_Union(b.geog::geometry)) FROM diss.setback_buffers b WHERE p.geog && b.geog), ST_GeomFromText('POLYGON((0 0, 0.1 0.1, 0 0.1, 0 0))', 4326))
        )
    )::geography AS geog
    FROM diss.gp16 p;

ALTER TABLE diss.buildable_areas ALTER COLUMN geog TYPE geography(MultiPolygon,4326);

CREATE INDEX buildable_areas_gix ON diss.buildable_areas USING GIST (geog)
