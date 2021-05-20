ALTER TABLE diss.gp16 ADD COLUMN Building_YearBuilt int4;
ALTER TABLE diss.gp16 ADD COLUMN Building_TotalBedrooms int4;
ALTER TABLE diss.gp16 ADD COLUMN Building_PropertyLandUseStndCode varchar;
ALTER TABLE diss.gp16 ADD COLUMN Building_NoOfUnits int4;
ALTER TABLE diss.gp16 ADD COLUMN Main_ImportParcelID BIGINT;
ALTER TABLE diss.gp16 ADD COLUMN Building_TotalCalculatedBathCount double PRECISION;

UPDATE diss.gp16 SET
    Building_YearBuilt = z."Building_YearBuilt",
    Building_TotalBedrooms = z."Building_TotalBedrooms",
    Building_PropertyLandUseStndCode = z."Building_PropertyLandUseStndCode",
    Building_NoOfUnits = z."Building_NoOfUnits",
    Main_ImportParcelID = z."Main_ImportParcelID",
    Building_TotalCalculatedBathCount = z."Building_TotalCalculatedBathCount"
    FROM diss.zasmt z
    WHERE lower(gp16.county) = lower(z."Main_County") AND gp16.clean_apn = z.clean_apn;
