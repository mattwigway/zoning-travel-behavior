ALTER TABLE diss.gp16 ADD COLUMN clean_apn VARCHAR;
-- SLOW!
--UPDATE diss.gp16 SET clean_apn = REGEXP_REPLACE(apn, '[^0-9a-zA-Z]', '', 'g');
UPDATE diss.gp16 SET clean_apn = REPLACE(apn, '-', '');
CREATE INDEX gp16_county_clean_apn_idx ON diss.gp16 (lower(county), clean_apn);

ALTER TABLE diss.zasmt ADD COLUMN clean_apn VARCHAR;
ALTER TABLE diss.zasmt ADD COLUMN ventura_apn_suffix VARCHAR;
-- SLOW!
--UPDATE diss.zasmt SET clean_apn = REGEXP_REPLACE("Main_AssessorParcelNumber", '[^0-9a-zA-Z]', '', 'g');
UPDATE diss.zasmt SET clean_apn = REPLACE("Main_AssessorParcelNumber", '-', '');

-- Ventura County has APN suffixes that indicate mineral rights, etc.
UPDATE diss.zasmt SET ventura_apn_suffix = SUBSTRING(clean_apn, 10, 1) WHERE "Main_County" = 'VENTURA';
UPDATE diss.zasmt SET clean_apn = SUBSTRING(clean_apn, 1, 9) WHERE "Main_County" = 'VENTURA';

CREATE INDEX zasmt_county_clean_apn_idx ON diss.zasmt (lower("Main_County"), clean_apn);