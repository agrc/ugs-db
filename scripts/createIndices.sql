USE [UGSWaterChemistry]
GO

CREATE INDEX DataSource_index
ON Results (DataSource)
CREATE INDEX ParamGroup_index
ON Results (ParamGroup)
CREATE INDEX Param_index
ON Results (Param)
CREATE INDEX SampleDate_index
ON Results (SampleDate)

CREATE INDEX StateCode_index
ON Stations (StateCode)
CREATE INDEX CountyCode_index
ON Stations (CountyCode)
CREATE INDEX StationType_index
ON Stations (StationType)
CREATE INDEX StationId_index
ON Stations (StationId)
CREATE INDEX HUC8_index
ON Stations (HUC8)
CREATE INDEX OrgId_index
ON Stations (OrgId)

GO
