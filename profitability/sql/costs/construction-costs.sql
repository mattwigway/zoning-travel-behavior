CREATE TABLE diss.building_costs AS
    SELECT p.gid, p.clean_apn, p.puma, p.scag_zn_co, p.lu16, p.county,
        c.name AS prototype,
        c.total_fixed_cost,
        ST_Area(p.geog) - (c.footprint_x * c.footprint_y) AS landscaped_area_sqm,
        CASE
            WHEN p.Building_PropertyLandUseStndCode = 'VL101' THEN 0 -- vacant, no demolition
            ELSE RSMEANS_DISPOSAL_COST -- RSMeans + disposal costs for 1600 SF 1 story SFH
        END AS demo_cost

        FROM diss.gp16 p
        CROSS JOIN diss.construction_cost c
        -- codes: https://gisdata-scag.opendata.arcgis.com/datasets/landuse-combined-los-angeles
        WHERE p.scag_zn_co IN (
            '1110', -- single family residential
            '1111', -- high dens SF residential
            '1112', -- med dens SF residential
            '1113', -- low dens SF residential
            '1150'  -- rural residential
        )
        AND p.Building_PropertyLandUseStndCode IN ('VL101', 'RR101');

ALTER TABLE diss.building_costs ADD COLUMN landscape_cost DOUBLE PRECISION;
UPDATE diss.building_costs SET landscape_cost = landscaped_area_sqm * 10.7639 * RSMEANS_COST_SF; -- 10.7639 sqm -> sf, $COST/SF (from RSMeans)

ALTER TABLE diss.building_costs ADD COLUMN total_cost DOUBLE PRECISION;
UPDATE diss.building_costs SET total_cost = total_fixed_cost + demo_cost + landscape_cost;

ALTER TABLE diss.building_costs ADD PRIMARY KEY (gid, prototype);
CREATE INDEX building_costs_apn_idx ON diss.building_costs (clean_apn);
CREATE INDEX building_costs_gid_idx ON diss.building_costs (gid);
CREATE INDEX building_costs_prototype_idx ON diss.building_costs (prototype);
