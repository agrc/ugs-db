USE [UGSWaterChemistry]
SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON

IF OBJECT_ID('dbo.Results', 'U') IS NOT NULL
	DROP TABLE [dbo].[Results]

IF OBJECT_ID('dbo.Stations', 'U') IS NOT NULL
	DROP TABLE [dbo].[Stations]

IF OBJECT_ID('dbo.Params', 'U') IS NOT NULL
	DROP TABLE [dbo].[Params]

CREATE TABLE [dbo].[Results](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[AnalysisDate] [date] NULL,
	[AnalytMeth] [nvarchar](150) NULL,
	[AnalytMethId] [nvarchar](50) NULL,
	[AutoQual] [nvarchar](50) NULL,
	[CAS_Reg] [nvarchar](50) NULL,
	[Chrg] [float] NULL,
	[DataSource] [nvarchar](6) NULL,
	[DetectCond] [nvarchar](50) NULL,
	[IdNum] [nvarchar](8) NULL,
	[LabComments] [nvarchar](500) NULL,
	[LabName] [nvarchar](100) NULL,
	[Lat_Y] [decimal](9, 6) NULL,
	[LimitType] [nvarchar](250) NULL,
	[Lon_X] [decimal](9, 6) NULL,
	[MDL] [float] NULL,
	[MDLUnit] [nvarchar](50) NULL,
	[MethodDescript] [nvarchar](100) NULL,
	[OrgId] [nvarchar](50) NULL,
	[OrgName] [nvarchar](150) NULL,
	[Param] [nvarchar](500) NULL,
	[ParamGroup] [nvarchar](50) NULL,
	[ProjectId] [nvarchar](50) NULL,
	[QualCode] [nvarchar](50) NULL,
	[ResultComment] [nvarchar](1500) NULL,
	[ResultStatus] [nvarchar](50) NULL,
	[ResultValue] [float] NULL,
	[SampComment] [nvarchar](500) NULL,
	[SampDepth] [float] NULL,
	[SampDepthRef] [nvarchar](50) NULL,
	[SampDepthU] [nvarchar](50) NULL,
	[SampEquip] [nvarchar](75) NULL,
	[SampFrac] [nvarchar](50) NULL,
	[SampleDate] [date] NULL,
	[SampleTime] [time](7) NULL,
	[SampleId] [nvarchar](100) NULL,
	[SampMedia] [nvarchar](50) NULL,
	[SampMeth] [nvarchar](50) NULL,
	[SampMethName] [nvarchar](75) NULL,
	[SampType] [nvarchar](75) NULL,
	[StationId] [nvarchar](100) NULL,
	[Unit] [nvarchar](50) NULL,
	[USGSPCode] [nvarchar](50) NULL,
 CONSTRAINT [PK_Result] PRIMARY KEY CLUSTERED
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

CREATE TABLE [dbo].[Stations](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[OrgId] [nvarchar](20) NULL,
	[OrgName] [nvarchar](100) NULL,
	[StationId] [nvarchar](100) NOT NULL,
	[StationName] [nvarchar](100) NULL,
	[StationType] [nvarchar](100) NULL,
	[StationComment] [nvarchar](1500) NULL,
	[HUC8] [nvarchar](8) NULL,
	[Lon_X] [decimal](9, 6) NULL,
	[Lat_Y] [decimal](9, 6) NULL,
	[HorAcc] [float] NULL,
	[HorAccUnit] [nvarchar](10) NULL,
	[HorCollMeth] [nvarchar](100) NULL,
	[HorRef] [nvarchar](10) NULL,
	[Elev] [float] NULL,
	[ElevUnit] [nvarchar](15) NULL,
	[ElevAcc] [float] NULL,
	[ElevAccUnit] [nvarchar](4) NULL,
	[ElevMeth] [nvarchar](100) NULL,
	[ElevRef] [nvarchar](12) NULL,
	[StateCode] [int] NULL,
	[CountyCode] [int] NULL,
	[Aquifer] [nvarchar](100) NULL,
	[FmType] [nvarchar](100) NULL,
	[AquiferType] [nvarchar](100) NULL,
	[ConstDate] [date] NULL,
	[Depth] [float] NULL,
	[DepthUnit] [nvarchar](10) NULL,
	[HoleDepth] [float] NULL,
	[HoleDUnit] [nvarchar](10) NULL,
	[DemElevM] [float] NULL,
	[DataSource] [nvarchar](20) NULL,
	[WIN] [bigint] NULL,
	[Shape] [geometry] NULL,
 CONSTRAINT [PK_Stations] PRIMARY KEY CLUSTERED
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

CREATE TABLE [dbo].[Params](
	[Param] [nvarchar](500) NULL
) ON [PRIMARY]
