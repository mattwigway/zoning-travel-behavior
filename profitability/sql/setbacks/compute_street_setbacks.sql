-- compute the setbacks for each street

ALTER TABLE diss.osm_segments DROP COLUMN IF EXISTS median_setback_meters;
ALTER TABLE diss.osm_segments ADD COLUMN median_setback_meters double precision;

CREATE TEMPORARY TABLE osm_segment_setbacks AS
    -- use 25th pctile, not sensitive to outliers and should behave even if second lot on side street is selected
    SELECT way_id, seg_id, percentile_cont(0.20) WITHIN GROUP (ORDER BY setback_meters) AS median_setback_meters
    FROM diss.parcel_segment_distance
    WHERE setback_meters IS NOT NULL
    GROUP BY way_id, seg_id;

UPDATE diss.osm_segments SET median_setback_meters = oss.median_setback_meters
    FROM osm_segment_setbacks oss
    WHERE osm_segments.way_id = oss.way_id AND osm_segments.seg_id = oss.seg_id;
