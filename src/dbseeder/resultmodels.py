#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
resultmodels
----------------------------------

The different program result models. They represent the data
to be inserted into the result table
"""
from models import Schema
from modelbases import Table, WqpTable
from modelextensions import Balanceable


class WqpResult(WqpTable, Balanceable):

    """ORM mapping to result schema to WqpResult table"""

    def __init__(self, row, normalizer, schema_map):
        super(WqpResult, self).__init__(normalizer)

        schema = Schema().result
        self.fields = range(0, len(schema))

        self.row = self._etl_row(row, schema_map, 'Result')


class SdwisResult(Table, Balanceable):

    fields = ['AnalysisDate',
              'LabName',
              'MDL',
              'MDLUnit',
              'OrgId',
              'OrgName',
              'Param',
              'ResultValue',
              'SampleDate',
              'SampleTime',
              'SampleId',
              'SampType',
              'StationId',
              'Unit',
              'Lat_Y',
              'Lon_X',
              'CAS_Reg',
              'IdNum',
              'ParamGroup']

    def __init__(self, row, normalizer, schema_map):
        super(SdwisResult, self).__init__(normalizer)

        self.row = self._etl_row(row, schema_map, 'Result')


class OgmResult(Table, Balanceable):

    """docstring for OgmResult"""

    fields = ['StationId',
              'Param',
              'SampleId',
              'SampleDate',
              'AnalysisDate',
              'AnalytMeth',
              'MDLUnit',
              'ResultValue',
              'SampleTime',
              'MDL',
              'Unit',
              'SampComment']

    def __init__(self, row, normalizer, schema_map):
        super(OgmResult, self).__init__(normalizer)

        #: add paramgroup in ctor so `Type.fields` works for reads
        #: since paragroup does not exist in source data
        self.fields = self.fields + ['ParamGroup']

        self.row = self._etl_row(row, schema_map, 'Result')


class DwrResult(Table, Balanceable):

    fields = ['SampleDate',
              'USGSPCode',
              'ResultValue',
              'Param',
              'Unit',
              'SampFrac',
              'OrgId',
              'OrgName',
              'StationId',
              'Lat_Y',
              'Lon_X',
              'SampMedia',
              'SampleId',
              'IdNum']

    def __init__(self, row, normalizer, schema_map):
        super(DwrResult, self).__init__(normalizer)

        #: add paramgroup in ctor so `Type.fields` works for reads
        #: since paragroup does not exist in source data
        self.fields = self.fields + ['ParamGroup']

        self.row = self._etl_row(row, schema_map, 'Result')


class UgsResult(Table, Balanceable):

    fields = ['ResultValue',
              'AnalysisDate',
              'OrgId',
              'OrgName',
              'SampleDate',
              'SampleTime',
              'DetectCond',
              'Unit',
              'MDLUnit',
              'AnalytMethId',
              'AnalytMeth',
              'SampMedia',
              'SampFrac',
              'StationId',
              'MDL',
              'IdNum',
              'LabName',
              'SampComment',
              'CAS_Reg']

    def __init__(self, row, normalizer, schema_map):
        super(UgsResult, self).__init__(normalizer)

        self.row = self._etl_row(row, schema_map, 'Result')
