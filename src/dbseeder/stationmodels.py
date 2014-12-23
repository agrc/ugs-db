#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
stationmodels
----------------------------------

The different program station models. They represent the data
to be inserted into the station table
"""
from models import Schema
from modelbases import Table, WqpTable


class WqpStation(WqpTable):

    """ORM mapping from chemistry schema to WqpStation feature class"""

    def __init__(self, row, normalizer, schema_map):
        super(WqpStation, self).__init__(normalizer)

        schema = Schema().station
        self.fields = range(0, len(schema))

        self.row = self._etl_row(row, schema_map, 'Station')


class SdwisStation(Table):

    fields = ['OrgId',
              'OrgName',
              'StationId',
              'StationName',
              'StationType',
              'Lat_Y',
              'Lon_X',
              'HorAcc',
              'HorCollMeth',
              'HorRef',
              'Elev',
              'ElevAcc',
              'ElevMeth',
              'ElevRef',
              'Depth',
              'DepthUnit']

    def __init__(self, row, normalizer):
        super(SdwisStation, self).__init__(normalizer)

        schema = Schema().station

        schema_map = Table.build_schema_map(schema)
        self.row = self._etl_row(row, schema_map, 'Station')


class OgmStation(Table):

    """docstring for OgmStation"""

    fields = ['OrgId',
              'OrgName',
              'StationId',
              'StationName',
              'Elev',
              'ElevUnit',
              'StationType',
              'StationComment',
              'Lat_Y',
              'Lon_X',
              'UTM_X',
              'UTM_Y']

    def __init__(self, row, normalizer, schema_map):
        super(OgmStation, self).__init__(normalizer)

        self.row = self._etl_row(row, schema_map, 'Station')


class DwrStation(Table):

    fields = ['WIN',
              'OrgId',
              'OrgName',
              'StationId',
              'Lat_Y',
              'Lon_X',
              'StateCode',
              'CountyCode',
              'Depth',
              'HoleDepth',
              'HUC8',
              'StationName',
              'StationType',
              'X_UTM',
              'Y_UTM']

    def __init__(self, row, normalizer):
        super(DwrStation, self).__init__(normalizer)

        self.schema = Schema().station

        self.schema_map = Table.build_schema_map(self.schema)
        self.row = self._etl_row(row, self.schema_map, 'Station')


class UgsStation(Table):

    fields = ['OrgId',
              'DataSource',
              'HUC8',
              'StateCode',
              'CountyCode',
              'OrgName',
              'Lat_Y',
              'Lon_X',
              'StationName',
              'StationComment',
              'StationId']

    def __init__(self, row, normalizer):
        super(UgsStation, self).__init__(normalizer)

        self.schema = Schema().station

        self.schema_map = Table.build_schema_map(self.schema)
        self.row = self._etl_row(row, self.schema_map, 'Station')
