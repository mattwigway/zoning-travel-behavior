CREATE TABLE diss.hqta_split AS SELECT ST_Subdivide(geog::geometry, 50) AS geom FROM diss.hqta;
ALTER TABLE diss.hqta_split ADD COLUMN gid serial;
ALTER TABLE diss.hqta_split ADD PRIMARY KEY (gid);
CREATE INDEX hqta_split_gix ON diss.hqta_split USING GIST (geom);
