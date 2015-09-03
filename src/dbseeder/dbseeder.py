#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
dbseeder
----------------------------------
the dbseeder module
'''

import pyodbc
import factory
from os.path import join, dirname
try:
    import secrets
except Exception:
    import secrets_sample as secrets
try:
    import arcpy
except Exception:
    import arcpy_mock as arcpy


class Seeder(object):

    def _get_db(self, who):
        db = secrets.dev
        if who == 'stage':
            db = secrets.stage
        elif who == 'prod':
            db = secrets.prod
        return db

    def create_tables(self, who):
        db = self._get_db(who)

        print('connecting to {} database'.format(who))

        script_dir = dirname(__file__)

        with open(join(script_dir, join('..', '..', 'scripts', 'createTables.sql')), 'r') as f:
            createTables_sql = f.read()
        with open(join(script_dir, join('..', '..', 'scripts', 'createIndices.sql')), 'r') as f:
            createIndices_sql = f.read()

        try:
            c = pyodbc.connect(db['connection_string'])
            cursor = c.cursor()
            cursor.execute(createTables_sql)
            cursor.execute(createIndices_sql)
        except Exception, e:
            raise e
        finally:
            if cursor:
                del cursor
            if c:
                del c

        print('done')

        return True

    def seed(self, source, file_location, who):
        db = self._get_db(who)

        programs = self._parse_source_args(source)

        for program in programs:
            seederClass = factory.create(program)

            seeder = seederClass(file_location, db)
            seeder.seed()

    def post_process(self, who):
        stations_fc = 'Stations'
        stations_identity = 'Stations_identity'
        dem = 'connection_files\SGID10.sde\SGID10.RASTER.DEM_10METER'

        arcpy.env.workspace = dirname(__file__)
        db = r'connection_files\{}.sde'.format(who)

        #: FIPS
        print('creating stations layer')
        stationsLyr = arcpy.MakeFeatureLayer_management(join(db, stations_fc), 'StationsLyr')

        print('identity on counties')
        stationsIdent = arcpy.Identity_analysis(stationsLyr,
                                                r'ReferenceData.gdb\US_Counties',
                                                join('in_memory', stations_identity))

        print('joining to layer')
        arcpy.AddJoin_management(stationsLyr, 'Id', stationsIdent, 'FID_' + stations_fc)

        print('calculating fields')
        arcpy.CalculateField_management(stationsLyr, 'StateCode', '!STATE_FIPS!', 'PYTHON')
        arcpy.CalculateField_management(stationsLyr, 'CountyCode', '!FIPS!', 'PYTHON')

        print('removing join')
        arcpy.RemoveJoin_management(stationsLyr, stations_identity)

        #: Elevation
        print('selecting points with null values')
        arcpy.SelectLayerByAttribute_management(stationsLyr, where_clause='Elev IS NULL')

        print('extracting values to points')
        arcpy.CheckOutExtension('Spatial')
        dem_points = arcpy.sa.ExtractValuesToPoints(stationsLyr, dem, r'in_memory\DEMPoints', add_attributes='VALUE_ONLY')

        print('joining to layer')
        arcpy.AddJoin_management(stationsLyr, 'StationId', dem_points, 'StationId')

        print('calulcating fields')
        arcpy.CalculateField_management(stationsLyr, 'Elev', '!RASTERVALU!', 'PYTHON')
        arcpy.CalculateField_management(stationsLyr, 'ElevUnit', '"meters"')
        arcpy.CalculateField_management(stationsLyr, 'ElevMeth', '"Other"')

    def _parse_source_args(self, source):
        all_sources = ['WQP', 'SDWIS', 'DOGM', 'DWR', 'UGS']
        if not source:
            return all_sources
        else:
            sources = [s.strip() for s in source.split(',')]
            sources = filter(lambda s: s in all_sources, sources)
            if len(sources) > 0:
                return sources
            else:
                return None
