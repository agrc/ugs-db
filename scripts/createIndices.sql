USE [UGSWaterChemistry]

CREATE INDEX DataSource_index
ON Results (DataSource)
CREATE INDEX ParamGroup_index
ON Results (ParamGroup)
CREATE INDEX Param_index
ON Results (Param)
CREATE INDEX SampleDate_index
ON Results (SampleDate)
CREATE INDEX StationId_index
ON Results (StationId)

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
CREATE SPATIAL INDEX [FDO_Shape] ON [ugswaterchemistry].[Stations]
(
    [Shape]
)USING  GEOMETRY_AUTO_GRID
WITH (BOUNDING_BOX =(138131.8383, 3955431.5674, 832275.2803, 4750305.616),
CELLS_PER_OBJECT = 2000, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
