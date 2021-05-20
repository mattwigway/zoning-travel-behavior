-- ZTrans is first loaded into an intermediate SQLite database as this is much faster, and some manipulations take
-- place there before loading to Postgres

BEGIN;
ALTER TABLE ZTrans_PropertyInfo ADD COLUMN n_properties_transacted int2;
CREATE TABLE pcount AS SELECT TransId, count(*) AS n FROM ZTrans_PropertyInfo GROUP BY TransId;
UPDATE ZTrans_PropertyInfo SET n_properties_transacted = pcount.n 
    FROM pcount
    WHERE pcount.TransId = ZTrans_PropertyInfo.TransId;
DROP TABLE pcount;
COMMIT;