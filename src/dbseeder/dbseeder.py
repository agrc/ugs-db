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
            sql = f.read()

        try:
            c = pyodbc.connect(db['connection_string'])
            cursor = c.cursor()
            cursor.execute(sql)
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
        arcpy.env.workspace = dirname(__file__)
        db = r'connection_files\{}.sde'.format(who)

        #: FIPS
        print('creating stations layer')
        stationsLyr = arcpy.MakeFeatureLayer_management(join(db, stations_fc), 'StationsLyr')

        print('identity on counties')
        stationsIdent = arcpy.Identity_analysis(stationsLyr,
                                                r'ReferenceData.gdb\US_Counties',
                                                r'in_memory\Stations_identity')

        print('joining to layer')
        arcpy.AddJoin_management(stationsLyr, 'Id', stationsIdent, 'FID_' + stations_fc)

        print('calculating fields')
        arcpy.CalculateField_management(stationsLyr, 'StateCode', '!STATE_FIPS!', 'PYTHON')
        arcpy.CalculateField_management(stationsLyr, 'CountyCode', '!FIPS!', 'PYTHON')
        #: Elevation
        #: Extract Values to Points
        #: join to original fc and calc fields

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
