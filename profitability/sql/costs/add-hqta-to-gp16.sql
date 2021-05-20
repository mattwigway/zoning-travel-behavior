ALTER TABLE diss.gp16 ADD COLUMN hqta BOOL;
UPDATE diss.gp16 SET hqta = FALSE;
UPDATE diss.gp16 p SET hqta = TRUE
    FROM diss.hqta_split h
    WHERE h.geom && p.geog AND ST_Contains(h.geom, ST_Centroid(p.geog)::geometry);
