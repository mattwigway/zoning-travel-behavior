-- Create a noded OSM network, with separate features for each line segment
CREATE TEMPORARY TABLE highway_nodes AS SELECT node_id, way_id, sequence_id
    FROM diss.way_nodes wn
    LEFT JOIN diss.ways w
    ON (wn.way_id = w.id)
    WHERE w.tags ? 'highway' AND LOWER(w.tags->'highway') NOT IN ('service', 'motorway', 'path', 'steps', 'footway', 'cycleway', 'bridleway', 'pedestrian');

CREATE TEMPORARY TABLE node_intersections AS SELECT node_id, count(*) > 1 AS multi
    FROM highway_nodes
    -- INNER JOIN implicitly drops things that aren't highways
    GROUP BY node_id;

ALTER TABLE highway_nodes DROP COLUMN IF EXISTS multi;
ALTER TABLE highway_nodes ADD COLUMN multi bool;
UPDATE highway_nodes SET multi = ni.multi
    FROM node_intersections ni
    WHERE (highway_nodes.node_id = ni.node_id);

-- can't use window function in update, so make a (second) temp table... disk space is cheap
CREATE TEMPORARY TABLE highway_node_segments AS SELECT *, sum(multi::int) OVER (PARTITION BY way_id ORDER BY sequence_id) AS seg_id
    FROM highway_nodes;

CREATE TABLE diss.osm_segments AS SELECT w.way_id, w.seg_id, ST_Makeline(ns.geom ORDER BY ns.sequence_id)::geography as geog
    FROM
    (SELECT DISTINCT way_id, seg_id FROM highway_node_segments) w
    LEFT JOIN
    (
        SELECT way_id, node_id, wn.seg_id, wn.sequence_id, wn.multi, n.geom
            FROM highway_node_segments wn
            LEFT JOIN diss.nodes n
            ON (n.id = wn.node_id)
    ) ns
    ON (w.way_id = ns.way_id AND (ns.seg_id = w.seg_id OR (ns.seg_id = w.seg_id + 1 AND ns.multi)))
    GROUP BY w.way_id, w.seg_id
    HAVING count(*) > 1; -- discard singletons at end of way

ALTER TABLE diss.osm_segments ADD PRIMARY KEY (way_id, seg_id);
ALTER TABLE diss.osm_segments ALTER COLUMN geog TYPE geography(LineString,4326);
