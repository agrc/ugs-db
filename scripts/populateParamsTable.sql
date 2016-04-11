USE [UGSWaterChemistry]

TRUNCATE TABLE [UGSWaterChemistry].[ugswaterchemistry].[Params]

INSERT INTO [UGSWaterChemistry].[ugswaterchemistry].[Params]
SELECT DISTINCT Param
FROM [UGSWaterChemistry].[ugswaterchemistry].[Results]
WHERE Param IS NOT NULL
