TRUNCATE TABLE [UGSWaterChemistry].[dbo].[Params]

INSERT INTO [UGSWaterChemistry].[dbo].[Params]
SELECT DISTINCT Param
FROM [UGSWaterChemistry].[dbo].[Results]
WHERE Param IS NOT NULL
