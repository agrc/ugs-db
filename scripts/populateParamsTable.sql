USE UGSWaterChemistry

GO
TRUNCATE TABLE [dbo].[Params]
GO

GO
INSERT INTO [dbo].[Params]
SELECT DISTINCT Param
FROM [dbo].[Results]
GO