-- Get the distance from each parcel to each nearby street based on the buildings on that parcel
-- Each parcel is implicitly assigned to all streets within 15 meters of any building on the parcel.
CREATE TABLE diss.parcel_segment_distance AS
    SELECT way_id, seg_id, parcel_gid, min(setback_meters) AS setback_meters
    FROM diss.building_segment_distance
    GROUP BY (way_id, seg_id, parcel_gid);
