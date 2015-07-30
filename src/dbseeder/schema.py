#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
schema.py
----------------------------------
the schema mapping for all programs
'''
from collections import OrderedDict


station = OrderedDict([
    ('OrgId', {
        'alias': 'Organization Id',
        'type': 'String',
        'length': 20
    }),
    ('OrgName', {
        'alias': 'Organization Name',
        'type': 'String',
        'length': 100
    }),
    ('StationId', {
        'alias': 'Monitoring Location Id',
        'type': 'String',
        'length': 100,
        'actions': ['strip_wxp']
    }),
    ('StationName', {
        'alias': 'Monitoring Location Name',
        'type': 'String',
        'length': 100
    }),
    ('StationType', {
        'alias': 'Monitoring Location Type',
        'type': 'String',
        'length': 100
    }),
    ('StationComment', {
        'alias': 'Monitoring Location Description',
        'type': 'String',
        'length': 1500
    }),
    ('HUC8', {
        'alias': 'HUC 8 Digit Code',
        'type': 'String',
        'length': 8
    }),
    ('Lon_X', {
        'alias': 'Latitude',
        'type': 'Double'
    }),
    ('Lat_Y', {
        'alias': 'Longitude',
        'type': 'Double'
    }),
    ('HorAcc', {
        'alias': 'Horizontal Accuracy',
        'type': 'Double'
    }),
    ('HorAccUnit', {
        'alias': 'Horizontal Accuracy Unit',
        'type': 'String',
        'length': 10
    }),
    ('HorCollMeth', {
        'alias': 'Horizontal Collection Method',
        'type': 'String',
        'length': 100
    }),
    ('HorRef', {
        'alias': 'Horizontal Reference Datum',
        'type': 'String',
        'length': 10
    }),
    ('Elev', {
        'alias': 'Elevation',
        'type': 'Double'
    }),
    ('ElevUnit', {
        'alias': 'Elevation Unit',
        'type': 'String',
        'length': 15
    }),
    ('ElevAcc', {
        'alias': 'Elevation Accuracy',
        'type': 'Double'
    }),
    ('ElevAccUnit', {
        'alias': 'Elevation Accuracy Units',
        'type': 'String',
        'length': 4
    }),
    ('ElevMeth', {
        'alias': 'Elevation Collection Method',
        'type': 'String',
        'length': 100
    }),
    ('ElevRef', {
        'alias': 'Elevation Reference Datum',
        'type': 'String',
        'length': 12
    }),
    ('StateCode', {
        'alias': 'State Code',
        'type': 'Short Int'
    }),
    ('CountyCode', {
        'alias': 'County Code',
        'type': 'Short Int'
    }),
    ('Aquifer', {
        'alias': 'Aquifer',
        'type': 'String',
        'length': 100
    }),
    ('FmType', {
        'alias': 'Formation Type',
        'type': 'String',
        'length': 100
    }),
    ('AquiferType', {
        'alias': 'Aquifer Type',
        'type': 'String',
        'length': 100
    }),
    ('ConstDate', {
        'alias': 'Construction Date',
        'type': 'Date',
        'length': 8
    }),
    ('Depth', {
        'alias': 'Well Depth',
        'type': 'Double'
    }),
    ('DepthUnit', {
        'alias': 'Well Depth Units',
        'type': 'String',
        'length': 10
    }),
    ('HoleDepth', {
        'alias': 'Hole Depth',
        'type': 'Double'
    }),
    ('HoleDUnit', {
        'alias': 'Hole Depth Units',
        'type': 'String',
        'length': 10
    }),
    ('demELEVm', {
        'alias': 'DEM Elevation m',
        'type': 'Double'
    }),
    ('DataSource', {
        'alias': 'Database Source',
        'type': 'String',
        'length': 20
    }),
    ('WIN', {
        'alias': 'WR Well Id',
        'type': 'Long Int'
    }),
    ('Shape', {
        'type': 'Geometry'
    })
])

result = OrderedDict([
    ('AnalysisDate', {
        'alias': 'Analysis Start Date',
        'type': 'Date'
    }),
    ('AnalytMeth', {
        'alias': 'Analytical Method Name',
        'type': 'Text',
        'length': 150
    }),
    ('AnalytMethId', {
        'alias': 'Analytical Method Id',
        'type': 'Text',
        'length': 50
    }),
    ('AutoQual', {
        'alias': 'Auto Quality Check',
        'type': 'Text'
    }),
    ('CAS_Reg', {
        'alias': 'CAS Registry',
        'type': 'Text',
        'length': 50
    }),
    ('Chrg', {
        'alias': 'Charge',
        'type': 'Float'
    }),
    ('DataSource', {
        'alias': 'Database Source',
        'type': 'Text'
    }),
    ('DetectCond', {
        'alias': 'Result Detection Condition',
        'type': 'Text',
        'length': 50
    }),
    ('IdNum', {
        'alias': 'Unique Id',
        'type': 'Long Int'
    }),
    ('LabComments', {
        'alias': 'Laboratory Comment',
        'type': 'Text',
        'length': 500
    }),
    ('LabName', {
        'alias': 'Laboratory Name',
        'type': 'Text',
        'length': 100
    }),
    ('Lat_Y', {
        'alias': 'Latitude',
        'type': 'Double'
    }),
    ('LimitType', {
        'alias': 'Detection Limit Type',
        'type': 'Text',
        'length': 250
    }),
    ('Lon_X', {
        'alias': 'Longitude',
        'type': 'Double'
    }),
    ('MDL', {
        'alias': 'Detection Quantitation Limit',
        'type': 'Double'
    }),
    ('MDLUnit', {
        'alias': 'Detection Quantitation Limit Unit',
        'type': 'Text',
        'length': 50
    }),
    ('MethodDescript', {
        'alias': 'Method Description',
        'type': 'Text',
        'length': 100
    }),
    ('OrgId', {
        'alias': 'Organization Id',
        'type': 'Text',
        'length': 50
    }),
    ('OrgName', {
        'alias': 'Organization Name',
        'type': 'Text',
        'length': 150
    }),
    ('Param', {
        'alias': 'Parameter',
        'type': 'Text',
        'length': 500
    }),
    ('ParamGroup', {
        'alias': 'Parameter Group',
        'type': 'Text'
    }),
    ('ProjectId', {
        'alias': 'Project Id',
        'type': 'Text',
        'length': 50
    }),
    ('QualCode', {
        'alias': 'Measure Qualifier Code',
        'type': 'Text',
        'length': 50
    }),
    ('ResultComment', {
        'alias': 'Result Comment',
        'type': 'Text',
        'length': 1500
    }),
    ('ResultStatus', {
        'alias': 'Result Status',
        'type': 'Text',
        'length': 50
    }),
    ('ResultValue', {
        'alias': 'Result Measure Value',
        'type': 'Double'
    }),
    ('SampComment', {
        'alias': 'Sample Comment',
        'type': 'Text',
        'length': 50
    }),
    ('SampDepth', {
        'alias': 'Sample Depth',
        'type': 'Double'
    }),
    ('SampDepthRef', {
        'alias': 'Sample Depth Reference',
        'type': 'Text',
        'length': 50
    }),
    ('SampDepthU', {
        'alias': 'Sample Depth Units',
        'type': 'Text',
        'length': 50
    }),
    ('SampEquip', {
        'alias': 'Collection Equipment',
        'type': 'Text',
        'length': 75
    }),
    ('SampFrac', {
        'alias': 'Result Sample Fraction',
        'type': 'Text',
        'length': 50
    }),
    ('SampleDate', {
        'alias': 'Sample Date',
        'type': 'Date'
    }),
    ('SampleTime', {
        'alias': 'Sample Time',
        'type': 'Time'
    }),
    ('SampleId', {
        'alias': 'Sample Id',
        'type': 'Text',
        'length': 100
    }),
    ('SampMedia', {
        'alias': 'Sample Media',
        'type': 'Text',
        'length': 50
    }),
    ('SampMeth', {
        'alias': 'Collection Method',
        'type': 'Text',
        'length': 50
    }),
    ('SampMethName', {
        'alias': 'Collection Method Name',
        'type': 'Text',
        'length': 75
    }),
    ('SampType', {
        'alias': 'Sample Type',
        'type': 'Text',
        'length': 75
    }),
    ('StationId', {
        'alias': 'Station Id',
        'type': 'Text',
        'length': 50,
        'actions': ['strip_wxp']
    }),
    ('Unit', {
        'alias': 'Result Measure Unit',
        'type': 'Text',
        'length': 50
    }),
    ('USGSPCode', {
        'alias': 'USGS P Code',
        'type': 'Text',
        'length': 50
    })
])
