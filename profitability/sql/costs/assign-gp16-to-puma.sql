ALTER TABLE diss.gp16 ADD COLUMN puma VARCHAR;

-- do in separate query so we only have one puma per building
UPDATE diss.gp16 p SET puma = f.puma
    FROM diss.puma_fixed_effects f
    WHERE ST_Contains(f.geometry, ST_Centroid(p.geog::geometry));
