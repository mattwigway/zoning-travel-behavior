ALTER TABLE diss.gp16 ADD COLUMN tract VARCHAR;
UPDATE diss.gp16 p SET tract = f.geoid
    FROM diss.ca_tracts f
    WHERE p.geog && f.geog AND ST_Contains(f.geog::geometry, ST_Centroid(p.geog)::geometry);
