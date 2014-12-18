#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
dbseeder
----------------------------------

The main entry point for the package. Handles command line
operations for seeding/updating packages.
"""
import arcpy
import os
from models import Field, Schema, TableInfo
from programs import Sdwis, Wqp, Dogm, Udwr, Ugs
from services import ConsolePrompt


class Seeder(object):

    def __init__(self, parent_folder='./', gdb_name='WQP.gdb'):
        super(Seeder, self).__init__()
        #: the number of records to query for sdwis
        self.count = None
        #: the parent location of the gdb
        self.parent_folder = parent_folder
        #: the name of the gdb to seed
        self.gdb_name = gdb_name
        #: the combination of the parent_folder and the gdb_name
        self.location = os.path.join(self.parent_folder, self.gdb_name)

    def _create_gdb(self):
        arcpy.CreateFileGDB_management(self.parent_folder,
                                       self.gdb_name,
                                       'CURRENT')

    def _create_feature_classes(self, types):
        """creates feature classes for the given types"""

        results_table = TableInfo(self.location, 'Results')
        station_points = TableInfo(self.location, 'Stations')
        schema = Schema()

        if 'Results' in types:
            arcpy.CreateTable_management(self.location,
                                         results_table.name)

            self._add_fields(results_table, schema.result)

        if 'Stations' in types:
            sr = arcpy.SpatialReference(26912)
            arcpy.CreateFeatureclass_management(self.location,
                                                station_points.name,
                                                'POINT',
                                                spatial_reference=sr)

            self._add_fields(station_points, schema.station)

    def _add_fields(self, table, schema):
        """adds fields to the table"""

        for schema_info in schema:
            field = Field(schema_info)
            arcpy.AddField_management(table.location,
                                      field.field_name,
                                      field.field_type,
                                      field_length=field.field_length,
                                      field_alias=field.field_alias)

    def create_relationship(self):
        origin = os.path.join(self.location, 'Stations')
        desitination = os.path.join(self.location, 'Results')
        key = 'StationId'

        arcpy.CreateRelationshipClass_management(origin,
                                                 desitination,
                                                 'Stations_Have_Results',
                                                 'COMPOSITE',
                                                 'Results',
                                                 'Stations',
                                                 'FORWARD',
                                                 'ONE_TO_MANY',
                                                 'NONE',
                                                 key,
                                                 key)

    def update(self):
        pass

    def field_lengths(self, types):
        if types[0].lower() == 'wqp':
            seed_data = 'C:\\Projects\\GitHub\\ugs-chemistry\\scripts\\dbseeder\\data'
            program = Wqp(self.location, arcpy.da.InsertCursor)

            return program.field_lengths(seed_data, types[1])

    def seed(self, folder, types):
        """
            #: folder - parent folder to seed data
            #: types - the type of data to seed [Stations, Results]
        """
        gdb_exists = os.path.exists(self.location)
        prompt = ConsolePrompt()

        if not gdb_exists:
            print 'creating gdb'
            self._create_gdb()
            print 'creating gdb: done'

            print 'creating feature classes'
            self._create_feature_classes(types)
            print 'creating feature classes: done'
        else:
            if not prompt.query_yes_no('gdb already exists. Seeed missing feature classes?'):
                if not prompt.query_yes_no('seed data? Could cause duplication'):
                    raise SystemExit('stopping')
                else:
                    self._seed(folder, types)
                    return
            else:
                print 'creating feature classes'
                self._create_feature_classes(types)
                print 'creating feature classes: done'

        self._seed(folder, types)

    def _seed(self, folder, types):
        wqp = Wqp(self.location, arcpy.da.InsertCursor)
        wqp.seed(folder, types)

        sdwis = Sdwis(self.location, arcpy.da.InsertCursor)
        sdwis.count = self.count
        sdwis.seed(types)

        dogm = Dogm(
            self.location, arcpy.da.SearchCursor, arcpy.da.InsertCursor)
        dogm.seed(folder, types)

        dwr = Udwr(
            self.location, arcpy.da.SearchCursor, arcpy.da.InsertCursor)
        dwr.seed(folder, types)

        ugs = Ugs(self.location, arcpy.da.SearchCursor, arcpy.da.InsertCursor)
        ugs.seed(folder, types)
