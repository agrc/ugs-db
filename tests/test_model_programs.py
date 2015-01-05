#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_model_programs
----------------------------------

Tests for `resultmodels` and `stationmodels` module.
"""

import datetime
import dbseeder.resultmodels as resultmodel
import dbseeder.stationmodels as stationmodel
import unittest
from dbseeder.services import Normalizer


class TestWqpModels(unittest.TestCase):

    def test_result_model_hydration(self):
        mdl = 2
        resultvalue = 141
        sampdepth = 0
        analysisdate = datetime.datetime(2014, 02, 24, 0, 0)
        sampledate = datetime.datetime(2014, 02, 24, 0, 0)
        # time parsing gives current date
        sampletime = (datetime.datetime.now().
                      replace(hour=11, minute=40, second=0, microsecond=0))

        csv_data = {'ActivityIdentifier': 'SampleId',
                    'CharacteristicName': 'Param',
                    'PrecisionValue': '',
                    'ResultAnalyticalMethod/MethodIdentifierContext': '',
                    'SampleAquifer': '',
                    'StatisticalBaseCode': '',
                    'ResultWeightBasisText': '',
                    'ActivityStartTime/Time': '11:40:00',
                    'ResultDetectionConditionText': 'DetectCond',
                    'ResultSampleFractionText': 'SampFrac',
                    'ActivityStartTime/TimeZoneCode': 'MST',
                    'ActivityStartDate': '2014-02-24',
                    'ActivityEndTime/Time': '',
                    'ActivityConductingOrganizationText': '',
                    'OrganizationIdentifier': 'OrgId',
                    'ActivityBottomDepthHeightMeasure/MeasureUnitCode': '',
                    'AnalysisStartDate': '2014-02-24',
                    'DetectionQuantitationLimitTypeName': 'LimitType',
                    'MethodDescriptionText': 'MethodDescript',
                    'ResultAnalyticalMethod/MethodIdentifier': 'AnalytMethId',
                    'SampleCollectionMethod/MethodName': 'SampMethName',
                    'ResultTemperatureBasisText': '',
                    'ResultDepthHeightMeasure/MeasureValue': '',
                    'ResultStatusIdentifier': 'ResultStatus',
                    'PreparationStartDate': '',
                    'USGSPCode': 'USGSPCode',
                    'ResultMeasureValue': '141',
                    'ActivityTypeCode': 'SampType',
                    'SampleCollectionMethod/MethodIdentifierContext': '',
                    'MeasureQualifierCode': 'QualCode',
                    'ActivityDepthHeightMeasure/MeasureValue': '0',
                    'ResultParticleSizeBasisText': '',
                    'ResultAnalyticalMethod/MethodName': 'AnalytMeth',
                    'ResultDepthAltitudeReferencePointText': '',
                    'ActivityDepthAltitudeReferencePointText': 'SampDepthRef',
                    'ResultCommentText': 'ResultComment',
                    'SampleTissueAnatomyName': '',
                    'SubjectTaxonomicName': '',
                    'ActivityTopDepthHeightMeasure/MeasureUnitCode': '',
                    'ActivityMediaName': 'Water',
                    'DetectionQuantitationLimitMeasure/MeasureUnitCode': 'mdlunit',
                    'ResultValueTypeName': 'Actual',
                    'OrganizationFormalName': 'OrgName',
                    'ActivityCommentText': 'SampComment',
                    'MonitoringLocationIdentifier': 'StationId',
                    'ProjectIdentifier': 'ProjectId',
                    'ResultLaboratoryCommentText': 'LabComments',
                    'ActivityEndTime/TimeZoneCode': '',
                    'HydrologicCondition': '',
                    'ResultMeasure/MeasureUnitCode': 'unit',
                    'ActivityTopDepthHeightMeasure/MeasureValue': '',
                    'ResultDepthHeightMeasure/MeasureUnitCode': '',
                    'DetectionQuantitationLimitMeasure/MeasureValue': '2',
                    'ActivityEndDate': '',
                    'LaboratoryName': 'LabName',
                    'HydrologicEvent': '',
                    'ResultTimeBasisText': '',
                    'ActivityBottomDepthHeightMeasure/MeasureValue': '',
                    'SampleCollectionMethod/MethodIdentifier': 'SampMeth',
                    'ActivityMediaSubdivisionName': 'SampMedia',
                    'SampleCollectionEquipmentName': 'SampEquip',
                    'ActivityDepthHeightMeasure/MeasureUnitCode': 'SampDepthU'}
        expected = [analysisdate,
                    'AnalytMeth',
                    'AnalytMethId',
                    None,
                    None,
                    None,
                    'WQP',
                    'DetectCond',
                    None,  # : idNum
                    'LabComments',
                    'LabName',
                    None,  # : lay y
                    'LimitType',
                    None,  # lon x,
                    mdl,
                    'mdlunit',
                    'MethodDescript',
                    'OrgId',
                    'OrgName',
                    'Param',
                    None,  # paramgroup
                    'ProjectId',
                    'QualCode',
                    'ResultComment',
                    'ResultStatus',
                    resultvalue,
                    'SampComment',
                    sampdepth,
                    'SampDepthRef',
                    'SampDepthU',
                    'SampEquip',
                    'SampFrac',
                    sampledate,
                    sampletime,
                    'SampleId',
                    'SampMedia',
                    'SampMeth',
                    'SampMethName',
                    'SampType',
                    'StationId',
                    'unit',
                    'USGSPCode'
                    ]

        schema_map = resultmodel.WqpResult.build_schema_map('Results')
        actual = resultmodel.WqpResult(
            csv_data, Normalizer(), schema_map).row
        self.assertListEqual(actual, expected)

    def test_station_model_hydration(self):
        lon_x = -114.323546838
        lat_y = 42.5661737512
        elev = 0
        depth = 1
        holedepth = 2
        elevac = 3
        horacc = 4
        statecode = 16
        countycode = 83
        constdate = datetime.datetime(2014, 06, 19, 0, 0)

        csv_data = {'DrainageAreaMeasure/MeasureUnitCode': 'ha',
                    'MonitoringLocationTypeName': 'StationType',
                    'HorizontalCoordinateReferenceSystemDatumName': 'HorRef',
                    'DrainageAreaMeasure/MeasureValue': '2774',
                    'StateCode': str(statecode),
                    'MonitoringLocationIdentifier': 'StationId',
                    'MonitoringLocationName': 'StationName',
                    'VerticalMeasure/MeasureValue': str(elev),
                    'FormationTypeText': 'FmType',
                    'VerticalAccuracyMeasure/MeasureUnitCode': 'ElevAccUnit',
                    'VerticalCoordinateReferenceSystemDatumName': 'ElevRef',
                    'AquiferTypeName': 'AquiferType',
                    'HorizontalAccuracyMeasure/MeasureUnitCode': 'HorAccUnit',
                    'ContributingDrainageAreaMeasure/MeasureUnitCode': '',
                    'WellHoleDepthMeasure/MeasureValue': str(holedepth),
                    'WellDepthMeasure/MeasureValue': str(depth),
                    'LongitudeMeasure': '-114.323546838',
                    'AquiferName': 'Aquifer',
                    'HorizontalAccuracyMeasure/MeasureValue': str(horacc),
                    'HUCEightDigitCode': 'HUC8',
                    'LatitudeMeasure': '42.5661737512',
                    'ContributingDrainageAreaMeasure/MeasureValue': '',
                    'OrganizationFormalName': 'OrgName',
                    'WellDepthMeasure/MeasureUnitCode': 'DepthUnit',
                    'OrganizationIdentifier': 'OrgId',
                    'HorizontalCollectionMethodName': 'HorCollMeth',
                    'VerticalAccuracyMeasure/MeasureValue': str(elevac),
                    'VerticalCollectionMethodName': 'ElevMeth',
                    'MonitoringLocationDescriptionText': 'StationComment',
                    'CountryCode': 'US',
                    'VerticalMeasure/MeasureUnitCode': 'ElevUnit',
                    'CountyCode': str(countycode),
                    'ConstructionDateText': '2014-06-19',
                    'WellHoleDepthMeasure/MeasureUnitCode': 'HoleDUnit',
                    'SourceMapScaleNumeric': ''}

        expected = [
            'OrgId',
            'OrgName',
            'StationId',
            'StationName',
            'StationType',
            'StationComment',
            'HUC8',
            lon_x,
            lat_y,
            horacc,
            'HorAccUnit',
            'HorCollMeth',
            'HorRef',
            elev,
            'ElevUnit',
            elevac,
            'ElevAccUnit',
            'ElevMeth',
            'ElevRef',
            statecode,
            countycode,
            'Aquifer',
            'FmType',
            'AquiferType',
            constdate,
            depth,
            'DepthUnit',
            holedepth,
            'HoleDUnit',
            None,
            'WQP',
            None,
            (227191.93568276422, 4717996.363612308)
        ]

        schema_map = stationmodel.WqpStation.build_schema_map('Stations')
        actual = stationmodel.WqpStation(
            csv_data, Normalizer(), schema_map).row

        self.assertListEqual(actual, expected)

    def test_table_normalization(self):
        csv_row = {'ActivityIdentifier': '',
                   'CharacteristicName': '.alpha.-Endosulfan',
                   'PrecisionValue': '',
                   'ResultAnalyticalMethod/MethodIdentifierContext': '',
                   'SampleAquifer': '',
                   'StatisticalBaseCode': '',
                   'ResultWeightBasisText': '',
                   'ActivityStartTime/Time': '',
                   'ResultDetectionConditionText': '',
                   'ResultSampleFractionText': '',
                   'ActivityStartTime/TimeZoneCode': '',
                   'ActivityStartDate': '',
                   'ActivityEndTime/Time': '',
                   'ActivityConductingOrganizationText': '',
                   'OrganizationIdentifier': '',
                   'ActivityBottomDepthHeightMeasure/MeasureUnitCode': '',
                   'AnalysisStartDate': '',
                   'DetectionQuantitationLimitTypeName': '',
                   'MethodDescriptionText': '',
                   'ResultAnalyticalMethod/MethodIdentifier': '',
                   'SampleCollectionMethod/MethodName': '',
                   'ResultTemperatureBasisText': '',
                   'ResultDepthHeightMeasure/MeasureValue': '',
                   'ResultStatusIdentifier': '',
                   'PreparationStartDate': '',
                   'USGSPCode': '',
                   'ResultMeasureValue': '',
                   'ActivityTypeCode': '',
                   'SampleCollectionMethod/MethodIdentifierContext': '',
                   'MeasureQualifierCode': '',
                   'ActivityDepthHeightMeasure/MeasureValue': '',
                   'ResultParticleSizeBasisText': '',
                   'ResultAnalyticalMethod/MethodName': '',
                   'ResultDepthAltitudeReferencePointText': '',
                   'ActivityDepthAltitudeReferencePointText': '',
                   'ResultCommentText': '',
                   'SampleTissueAnatomyName': '',
                   'SubjectTaxonomicName': '',
                   'ActivityTopDepthHeightMeasure/MeasureUnitCode': '',
                   'ActivityMediaName': '',
                   'DetectionQuantitationLimitMeasure/MeasureUnitCode': '',
                   'ResultValueTypeName': '',
                   'OrganizationFormalName': '',
                   'ActivityCommentText': '',
                   'MonitoringLocationIdentifier': '',
                   'ProjectIdentifier': '',
                   'ResultLaboratoryCommentText': '',
                   'ActivityEndTime/TimeZoneCode': '',
                   'HydrologicCondition': '',
                   'ResultMeasure/MeasureUnitCode': '',
                   'ActivityTopDepthHeightMeasure/MeasureValue': '',
                   'ResultDepthHeightMeasure/MeasureUnitCode': '',
                   'DetectionQuantitationLimitMeasure/MeasureValue': '',
                   'ActivityEndDate': '',
                   'LaboratoryName': '',
                   'HydrologicEvent': '',
                   'ResultTimeBasisText': '',
                   'ActivityBottomDepthHeightMeasure/MeasureValue': '',
                   'SampleCollectionMethod/MethodIdentifier': '',
                   'ActivityMediaSubdivisionName': '',
                   'SampleCollectionEquipmentName': '',
                   'ActivityDepthHeightMeasure/MeasureUnitCode': ''}

        schema_map = resultmodel.WqpResult.build_schema_map('Results')
        actual = resultmodel.WqpResult(
            csv_row, Normalizer(), schema_map).row

        expected = [None,  # analysisdate
                    None,  # analytmeth
                    None,  # analythmethid
                    None,  # autoqual
                    None,  # cas reg
                    None,  # chrg
                    'WQP',  # datasource
                    None,  # detectcond
                    None,  # idnum
                    None,  # lab comments
                    None,  # lab name
                    None,  # lat y
                    None,  # limit type
                    None,  # lon x
                    None,  # mdl
                    None,  # mdlunit
                    None,  # method descript
                    None,  # orgid
                    None,  # orgname
                    '.alpha.-Endosulfan',  # param
                    'Organics, pesticide',  # paramgroup
                    None,  # projectid
                    None,  # qualcode
                    None,  # r result comment
                    None,  # result status
                    None,  # result value
                    None,  # sampcomment
                    None,  # sampdepth
                    None,  # sampdepthref
                    None,  # sampdepthu
                    None,  # sampequp
                    None,  # sampfrac
                    None,  # sample date
                    None,  # sample time
                    None,  # sample id
                    None,  # sampmedia
                    None,  # sampmeth
                    None,  # sampmethname
                    None,  # samptype
                    None,  # station id
                    None,  # unit
                    None   # usgspcode
                    ]

        self.assertListEqual(actual, expected)


class TestSdwisModels(unittest.TestCase):

    def test_station_model_hydration(self):
        db_row = [750,
                  'HANNA WATER & SEWER IMPROVEMENT DISTRICT',
                  3382,
                  'STOCKMORE WELL                          ',
                  'WL',
                  40.460074,
                  -110.826317,
                  15.0,
                  '018',
                  '003',
                  2126.71,
                  1.84,
                  '003',
                  '003',
                  0,
                  None]

        schema_map = stationmodel.SdwisStation.build_schema_map('Stations')
        patient = stationmodel.SdwisStation(
            db_row, Normalizer(), schema_map)
        actual = patient.row
        expected = ['750',
                    'HANNA WATER & SEWER IMPROVEMENT DISTRICT',
                    '3382',
                    'STOCKMORE WELL',
                    'WL',
                    None,
                    None,
                    -110.826317,
                    40.460074,
                    15.0,
                    None,
                    '018',
                    '003',
                    2126.71,
                    None,
                    1.84,
                    None,
                    '003',
                    '003',
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    0.0,
                    None,
                    None,
                    None,
                    None,
                    'SDWIS',
                    None,
                    (514725.5552380322, 4478837.450786671)]
        self.assertListEqual(expected, actual)

    def test_result_model_hydration(self):
        db_row = [None,
                  'UT00007   ',
                  0.1,
                  'MG/L     ',
                  1748,
                  'SUMMIT CHATEAU IN BRIAN HEAD',
                  'NITRATE-NITRITE                         ',
                  0.0,
                  datetime.datetime(2014, 4, 23, 0, 0),
                  datetime.datetime(1, 1, 1, 14, 10),
                  'K201400801',
                  'WL',
                  9032,
                  '         ',
                  37.732475,
                  -112.871236,
                  None,
                  3908822]

        schema_map = resultmodel.SdwisResult.build_schema_map('Results')
        actual = resultmodel.SdwisResult(
            db_row, Normalizer(), schema_map).row
        expected = [None,
                    None,
                    None,
                    None,
                    None,  # : cas_reg
                    None,
                    'SDWIS',
                    None,
                    3908822L,  # : idnum
                    None,
                    'UT00007',  # : lab comments
                    37.732475,
                    None,
                    -112.871236,
                    0.1,
                    'MG/L',
                    None,
                    '1748',  # : orgid
                    'SUMMIT CHATEAU IN BRIAN HEAD',
                    'NITRATE-NITRITE',
                    None,
                    None,
                    None,
                    None,
                    None,
                    0.0,  # : result value
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    datetime.datetime(2014, 04, 23, 0, 0),
                    datetime.datetime(1, 1, 1, 14, 10),
                    'K201400801',
                    None,
                    None,
                    None,
                    'WL',
                    '9032',
                    None,
                    None]

        self.assertListEqual(actual, expected)

    def test_sdwis_normalization(self):
        db_row = [None,
                  None,
                  None,
                  None,
                  None,
                  None,
                  '.alpha.-Endosulfan',
                  None,
                  None,
                  None,
                  None,
                  None,
                  None,
                  None,
                  None,
                  None,
                  None,
                  None]

        schema_map = resultmodel.SdwisResult.build_schema_map('Results')
        actual = resultmodel.SdwisResult(
            db_row, Normalizer(), schema_map).row
        expected = [None,  # analysisdate
                    None,  # analytmeth
                    None,  # analythmethid
                    None,  # autoqual
                    None,  # cas reg
                    None,  # chrg
                    'SDWIS',  # datasource
                    None,  # detectcond
                    None,  # idnum
                    None,  # lab comments
                    None,  # lab name
                    None,  # lat y
                    None,  # limit type
                    None,  # lon x
                    None,  # mdl
                    None,  # mdlunit
                    None,  # method descript
                    None,  # orgid
                    None,  # orgname
                    '.alpha.-Endosulfan',  # param
                    'Organics, pesticide',  # paramgroup
                    None,  # projectid
                    None,  # qualcode
                    None,  # r result comment
                    None,  # result status
                    None,  # result value
                    None,  # sampcomment
                    None,  # sampdepth
                    None,  # sampdepthref
                    None,  # sampdepthu
                    None,  # sampequp
                    None,  # sampfrac
                    None,  # sample date
                    None,  # sample time
                    None,  # sample id
                    None,  # sampmedia
                    None,  # sampmeth
                    None,  # sampmethname
                    None,  # samptype
                    None,  # station id
                    None,  # unit
                    None   # usgspcode
                    ]

        self.assertListEqual(actual, expected)


class TestDogmModels(unittest.TestCase):

    def test_ogm_station_model_hydration(self):
        gdb_data = ['UDOGM',
                    'Utah Division Of Oil Gas And Mining',
                    'UDOGM-0035',
                    'WILLOW CREEK; 1',
                    None,
                    'ft',
                    'MD',
                    'UPDES',
                    39.72882937000003,
                    -110.85612099299999,
                    512329.9142,
                    4397670.5318
                    ]

        schema_map = stationmodel.OgmStation.build_schema_map('Stations')
        model = stationmodel.OgmStation(
            gdb_data, Normalizer(), schema_map)

        expected = ['UDOGM',
                    'Utah Division Of Oil Gas And Mining',
                    'UDOGM-0035',
                    'WILLOW CREEK; 1',
                    'MD',
                    'UPDES',
                    None,
                    -110.85612099299999,
                    39.72882937000003,
                    None,
                    None,
                    None,
                    None,
                    None,
                    'ft',
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    'DOGM',
                    None,
                    (512329.9142,
                     4397670.5318)]

        self.assertListEqual(expected, model.row)

    def test_ogm_result_model_hydration(self):
        analysisdate = datetime.datetime(2014, 11, 17, 0, 0)
        sampledate = datetime.datetime(2008, 11, 17, 0, 0)
        sampletime = datetime.datetime(1899, 12, 30, 11, 10)
        resultvalue = 0
        mdl = 1

        gdb_data = ['StationId',
                    'Param',
                    'SampleId',
                    sampledate,
                    analysisdate,
                    'AnalytMeth',
                    'MDLUnit',
                    resultvalue,
                    sampletime,
                    mdl,
                    'Unit',
                    'SampComment']
        expected = [analysisdate,
                    'AnalytMeth',
                    None,  # analythmethid
                    None,  # autoqual
                    None,  # cas reg
                    None,  # chrg
                    'DOGM',  # datasource
                    None,  # detectcond
                    None,  # idnum
                    None,  # lab comments
                    None,  # lab name
                    None,  # lat y
                    None,  # limit type
                    None,  # lon x
                    mdl,  # mdl
                    'MDLUnit',  # mdlunit
                    None,  # method descript
                    None,  # orgid
                    None,  # orgname
                    'Param',  # param
                    None,  # paramgroup
                    None,  # projectid
                    None,  # qualcode
                    None,  # r result comment
                    None,  # result status
                    resultvalue,
                    'SampComment',
                    None,  # sampdepth
                    None,  # sampdepthref
                    None,  # sampdepthu
                    None,  # sampequp
                    None,  # sampfrac
                    sampledate,
                    sampletime,
                    'SampleId',  # sample id
                    None,  # sampmedia
                    None,  # sampmeth
                    None,  # sampmethname
                    None,  # samptype
                    'StationId',
                    'unit',
                    None  # usgspcode
                    ]
        schema_map = resultmodel.OgmResult.build_schema_map('Results')
        model = resultmodel.OgmResult(
            gdb_data, Normalizer(), schema_map)
        actual = model.row

        self.assertListEqual(expected, actual)

    def test_gdb_datasoure_normalization(self):
        gdb_data = [None,
                    '.alpha.-Endosulfan',
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None]
        expected = [None,
                    None,
                    None,  # analythmethid
                    None,  # autoqual
                    None,  # cas reg
                    None,  # chrg
                    'DOGM',  # datasource
                    None,  # detectcond
                    None,  # idnum
                    None,  # lab comments
                    None,  # lab name
                    None,  # lat y
                    None,  # limit type
                    None,  # lon x
                    None,
                    None,
                    None,  # method descript
                    None,  # orgid
                    None,  # orgname
                    '.alpha.-Endosulfan',  # param
                    'Organics, pesticide',  # paramgroup
                    None,  # projectid
                    None,  # qualcode
                    None,  # r result comment
                    None,  # result status
                    None,
                    None,
                    None,  # sampdepth
                    None,  # sampdepthref
                    None,  # sampdepthu
                    None,  # sampequp
                    None,  # sampfrac
                    None,
                    None,
                    None,  # sample id
                    None,  # sampmedia
                    None,  # sampmeth
                    None,  # sampmethname
                    None,  # samptype
                    None,
                    None,
                    None  # usgspcode
                    ]

        schema_map = resultmodel.OgmResult.build_schema_map('Results')
        model = resultmodel.OgmResult(gdb_data, Normalizer(), schema_map)
        actual = model.row

        self.assertListEqual(expected, actual)


class TestDwrModels(unittest.TestCase):

    def test_result_model_hydration(self):
        sampledate = datetime.datetime(2008, 11, 17, 0, 0)
        result_value = 0
        idnum = 0

        gdb_data = [sampledate,
                    'usgspcode',
                    result_value,
                    'param',
                    'unit',
                    'sampfrac',
                    'orgid',
                    'orgname',
                    'stationid',
                    0,
                    0,
                    'sampmedia',
                    'sampleid',
                    idnum
                    ]
        expected = [None,  # analysisdate
                    None,  # analytmeth
                    None,  # analythmethid
                    None,  # autoqual
                    None,  # cas reg
                    None,  # chrg
                    'DWR',  # datasource
                    None,  # detectcond
                    idnum,  # idnum
                    None,  # lab comments
                    None,  # lab name
                    0,  # lat y
                    None,  # limit type
                    0,  # lon x
                    None,  # mdl
                    None,  # mdlunit
                    None,  # method descript
                    'orgid',  # orgid
                    'orgname',  # orgname
                    'param',
                    None,  # paramgroup
                    None,  # projectid
                    None,  # qualcode
                    None,  # r result comment
                    None,  # result status
                    result_value,
                    None,  # sampcomment
                    None,  # sampdepth
                    None,  # sampdepthref
                    None,  # sampdepthu
                    None,  # sampequp
                    'sampfrac',  # sampfrac
                    sampledate,  # sample date
                    None,  # sample time
                    'sampleid',  # sample id
                    'sampmedia',  # sampmedia
                    None,  # sampmeth
                    None,  # sampmethname
                    None,  # samptype
                    'stationid',
                    'unit',
                    'usgspcode'  # usgspcode
                    ]

        schema_map = resultmodel.DwrResult.build_schema_map('Results')
        model = resultmodel.DwrResult(
            gdb_data, Normalizer(), schema_map)
        actual = model.row

        self.assertListEqual(expected, actual)

    def test_station_model_hydration(self):
        county_code = 0
        x = 1
        y = 2
        hole_depth = 3
        depth = 4
        lat = 5
        lon = 6
        state_code = 7
        win = 8
        gdb_data = [win,
                    'orgid',
                    'orgname',
                    'stationid',
                    lat,
                    lon,
                    state_code,
                    county_code,
                    depth,
                    hole_depth,
                    'huc8',
                    'stationname',
                    'stationtype',
                    x,
                    y]

        schema_map = stationmodel.DwrStation.build_schema_map('Stations')
        model = stationmodel.DwrStation(
            gdb_data, Normalizer(), schema_map)

        expected = ['orgid',  # orgid
                    'orgname',  # orgname
                    'stationid',  # station id
                    'stationname',  # stationname
                    'stationtype',  # stationtype
                    None,  # station comment
                    'huc8',  # huc8,
                    lon,  # lon x
                    lat,  # lay y,
                    None,  # horacc
                    None,  # horaccunit
                    None,  # horcallmeth
                    None,  # hor ref
                    None,  # elev
                    None,  # elev unit
                    None,  # elev acc
                    None,  # elev acc unit
                    None,  # elev meth
                    None,  # elev ref
                    state_code,  # state code
                    county_code,  # county code
                    None,  # aquifer
                    None,  # fm type
                    None,  # aquifer type
                    None,  # constdate
                    depth,  # depth
                    None,  # depth unit
                    hole_depth,  # hole depth
                    None,  # hold d unit
                    None,  # dem elev
                    'DWR',  # datasource
                    win,  # win
                    (x, y)  # shape
                    ]

        self.assertListEqual(expected, model.row)

    def test_gdb_datasoure_normalization(self):
        gdb_result_data = [
            None,
            None,
            None,
            '.alpha.-Endosulfan',
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None
        ]

        schema_map = resultmodel.DwrResult.build_schema_map('Results')
        model = resultmodel.DwrResult(
            gdb_result_data, Normalizer(), schema_map)
        expected = [None,  # analysisdate
                    None,  # analytmeth
                    None,  # analythmethid
                    None,  # autoqual
                    None,  # cas reg
                    None,  # chrg
                    'DWR',  # datasource
                    None,  # detectcond
                    None,  # idnum
                    None,  # lab comments
                    None,  # lab name
                    None,  # lat y
                    None,  # limit type
                    None,  # lon x
                    None,  # mdl
                    None,  # mdlunit
                    None,  # method descript
                    None,  # orgid
                    None,  # orgname
                    '.alpha.-Endosulfan',  # param
                    'Organics, pesticide',  # paramgroup
                    None,  # projectid
                    None,  # qualcode
                    None,  # r result comment
                    None,  # result status
                    None,  # result value
                    None,  # sampcomment
                    None,  # sampdepth
                    None,  # sampdepthref
                    None,  # sampdepthu
                    None,  # sampequp
                    None,  # sampfrac
                    None,  # sample date
                    None,  # sample time
                    None,  # sample id
                    None,  # sampmedia
                    None,  # sampmeth
                    None,  # sampmethname
                    None,  # samptype
                    None,  # station id
                    None,  # unit
                    None   # usgspcode
                    ]

        self.assertListEqual(model.row, expected)


class TestUgsModels(unittest.TestCase):

    def test_result_model_hydration(self):
        sampledate = datetime.datetime(2008, 11, 17, 0, 0)
        analysisdate = datetime.datetime(2008, 11, 18, 0, 0)
        # time parsing gives current date
        sampletime = (datetime.datetime.now().
                      replace(hour=11, minute=40, second=0, microsecond=0))
        result_value = 0
        idnum = 1
        mdl = 2

        gdb_data = [result_value,
                    analysisdate,
                    'orgid',
                    'orgname',
                    sampledate,
                    sampletime,
                    'detectcond',
                    'unit',
                    'mdlunit',
                    'analytmethid',
                    'analytmeth',
                    'sampmedia',
                    'sampfrac',
                    'stationid',
                    mdl,
                    idnum,
                    'labname',
                    'sampcomment',
                    'casreg'
                    ]
        expected = [analysisdate,  # analysisdate
                    'analytmeth',  # analytmeth
                    'analytmethid',  # analythmethid
                    None,  # autoqual
                    'casreg',  # cas reg
                    None,  # chrg
                    'UGS',  # datasource
                    'detectcond',  # detectcond
                    idnum,  # idnum
                    None,  # lab comments
                    'labname',  # lab name
                    None,  # lat y
                    None,  # limit type
                    None,  # lon x
                    mdl,  # mdl
                    'mdlunit',  # mdlunit
                    None,  # method descript
                    'orgid',  # orgid
                    'orgname',  # orgname
                    None,  # param
                    None,  # paramgroup
                    None,  # projectid
                    None,  # qualcode
                    None,  # r result comment
                    None,  # result status
                    result_value,
                    'sampcomment',  # sampcomment
                    None,  # sampdepth
                    None,  # sampdepthref
                    None,  # sampdepthu
                    None,  # sampequp
                    'sampfrac',  # sampfrac
                    sampledate,  # sample date
                    sampletime,  # sample time
                    None,  # sample id
                    'sampmedia',  # sampmedia
                    None,  # sampmeth
                    None,  # sampmethname
                    None,  # samptype
                    'stationid',
                    'unit',  # unit
                    None  # usgspcode
                    ]

        schema_map = resultmodel.UgsResult.build_schema_map('Results')
        model = resultmodel.UgsResult(
            gdb_data, Normalizer(), schema_map)
        actual = model.row

        self.assertListEqual(actual, expected)

    def test_station_model_hydration(self):
        county_code = 0
        lat = 37.6258
        lon = -113.12605
        state_code = 3
        gdb_data = ['orgid',
                    'datasource',
                    'huc8',
                    state_code,
                    county_code,
                    'orgname',
                    lat,
                    lon,
                    'stationname',
                    'stationcomment',
                    'stationid']

        schema_map = stationmodel.UgsStation.build_schema_map('Stations')
        model = stationmodel.UgsStation(
            gdb_data, Normalizer(), schema_map)

        expected = ['orgid',  # orgid
                    'orgname',  # orgname
                    'stationid',  # station id
                    'stationname',  # stationname
                    None,  # stationtype
                    'stationcomment',  # station comment
                    'huc8',  # huc8,
                    lon,  # lon x
                    lat,  # lay y,
                    None,  # horacc
                    None,  # horaccunit
                    None,  # horcallmeth
                    None,  # hor ref
                    None,  # elev
                    None,  # elev unit
                    None,  # elev acc
                    None,  # elev acc unit
                    None,  # elev meth
                    None,  # elev ref
                    state_code,  # state code
                    county_code,  # county code
                    None,  # aquifer
                    None,  # fm type
                    None,  # aquifer type
                    None,  # constdate
                    None,  # depth
                    None,  # depth unit
                    None,  # hole depth
                    None,  # hold d unit
                    None,  # dem elev
                    'UGS',  # datasource
                    None,  # win
                    (312382.9355031499, 4166423.73346324)  # shape
                    ]

        self.assertListEqual(expected, model.row)
