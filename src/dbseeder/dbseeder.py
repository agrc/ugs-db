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
            seed_data = 'C:\\Projects\\GitHub\\ugs-db\\tests\\data'
            program = Wqp(self.location, arcpy.da.InsertCursor)

            return program.field_lengths(seed_data, types[1])

    def seed(self, folder, types):
        """
            #: folder - parent folder to seed data
            #: types - the type of data to seed [Stations, Results]
        """
        gdb_exists = os.path.exists(self.location)
        prompt = ConsolePrompt()
        seed_program = {
            'WQP': True,
            'SDWIS': True,
            'DOGM': True,
            'DWR': True,
            'UGS': True
        }

        if not gdb_exists:
            print 'creating gdb'
            self._create_gdb()
            print 'creating gdb: done'

            print 'creating feature classes'
            self._create_feature_classes(types)
            print 'creating feature classes: done'
        else:
            if prompt.query_yes_no('gdb already exists. Do you want to start fresh?'):
                print 'deleting {}'.format(self.location)
                from shutil import rmtree
                rmtree(self.location)

                print 'creating gdb'
                self._create_gdb()
                print 'creating gdb: done'

                print 'creating feature classes'
                self._create_feature_classes(types)
                print 'creating feature classes: done'
            elif prompt.query_yes_no('Is it missing feature classes?'):
                print 'creating feature classes'
                self._create_feature_classes(types)
                print 'creating feature classes: done'

            elif prompt.query_yes_no('Do you want to import the data anyway? This could cause duplication.'):
                pass

        self._seed(folder, types, seed_program)

    def _seed(self, folder, types, seed):

        if seed['WQP']:
            print 'Seeding WQP'
            wqp = Wqp(self.location, arcpy.da.InsertCursor)
            wqp.seed(folder, types)

        if seed['SDWIS']:
            print 'Seeding SDWIS'
            sdwis = Sdwis(self.location, arcpy.da.InsertCursor)
            sdwis.count = self.count
            sdwis.seed(types)

        if seed['DOGM']:
            print 'Seeding DOGM'
            dogm = Dogm(
                self.location, arcpy.da.SearchCursor, arcpy.da.InsertCursor)
            dogm.seed(folder, types)

        if seed['DWR']:
            print 'Seeding DWR'
            dwr = Udwr(
                self.location, arcpy.da.SearchCursor, arcpy.da.InsertCursor)
            dwr.seed(folder, types)

        if seed['UGS']:
            print 'Seeding UGS'
            ugs = Ugs(self.location, arcpy.da.SearchCursor, arcpy.da.InsertCursor)
            ugs.seed(folder, types)

if __name__ == '__main__':
    from cProfile import Profile

    print 'profiling Seeder'

    location = 'c:\\temp\\'
    gdb = 'master.gdb'
    seed_data = 'C:\\Projects\\GitHub\\ugs-db\\tests\\data'
    types = ['Results', 'Stations']
    seed_program = {
        'WQP': True,
        'SDWIS': True,
        'DOGM': True,
        'DWR': True,
        'UGS': True
    }

    seeder = Seeder(location, gdb)

    if os.path.exists(seeder.location):
        from shutil import rmtree
        rmtree(seeder.location)

    seeder._create_gdb()
    seeder._create_feature_classes(types)
    seeder.count = 10

    profiler = Profile()

    profiler.runctx('seeder._seed(seed_data, types, seed_program)', locals(), globals())
    profiler.dump_stats('.pstat')

    # from pyprof2calltree import convert, visualize
    # visualize(profiler.getstats())
    # convert(profiler.getstats(), 'profiling_results.kgrind')
