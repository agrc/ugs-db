#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
dbseeder.py
----------------------------------
the main entry point module for database ETL
'''

import pyodbc
import factory
import requests
import sql
from os.path import join, dirname
try:
    import secrets
except ImportError:
    import secrets_sample as secrets
try:
    import arcpy
except ImportError:
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
            create_tables_sql = f.read()
        with open(join(script_dir, join('..', '..', 'scripts', 'createIndices.sql')), 'r') as f:
            create_indices_sql = f.read()

        c = None
        cursor = None
        try:
            c = pyodbc.connect(db['connection_string'])
            cursor = c.cursor()
            cursor.execute(create_tables_sql)
            cursor.execute(create_indices_sql)
            cursor.commit()
        except Exception, e:
            raise e
        finally:
            if cursor is not None:
                del cursor
            if c is not None:
                del c

        print('done')

        return True

    def seed(self, source, file_location, who):
        db = self._get_db(who)

        programs = self._parse_source_args(source)

        for program in programs:
            seederClass = factory.get(program)

            seeder = seederClass(db=db,
                                 update=False,
                                 source=file_location,
                                 secrets=secrets.sdwis,
                                 sql_statements=sql.sql_statements,
                                 update_row=sql.update_row,
                                 insert_rows=sql.insert_rows,
                                 cursor_factory=sql.create_cursor,
                                 arcpy=arcpy)
            seeder.seed()

    def post_process(self, who):
        '''
        Recalculate StateCode and CountyCode for the entire dataset (not sure that we can trust what's there)
        Populate Elev, ElevUnit, & ElevMeth only for records that have missing or bad data
        '''
        stations_fc = 'UGSWaterChemistry.dbo.Stations'
        stations_identity = 'Stations_identity'
        epqs_service_url = r'http://nationalmap.gov/epqs/pqs.php'

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

        print('calculating state')
        arcpy.CalculateField_management(stationsLyr, 'StateCode', '!STATE_FIPS!', 'PYTHON')
        print('calculating county')
        arcpy.CalculateField_management(stationsLyr, 'CountyCode', '!FIPS!', 'PYTHON')

        print('removing join')
        arcpy.RemoveJoin_management(stationsLyr, stations_identity)

        #: Elevation
        print('looping through points with null elevation values')
        connection = pyodbc.connect(self._get_db(who)['connection_string'])
        cursor = connection.cursor()
        cursor.execute('''
            SELECT Lon_X, Lat_Y, Id
            FROM Stations
            WHERE Elev IS NULL OR Elev = 0 OR Elev > 20000
        ''')
        i = 0
        batch_size = 100
        rows = cursor.fetchall()
        total = len(rows)
        for row in rows:
            payload = {'x': row.Lon_X, 'y': row.Lat_Y, 'units': 'Meters', 'output': 'json'}
            r = requests.get(epqs_service_url, params=payload)
            try:
                elev = r.json()['USGS_Elevation_Point_Query_Service']['Elevation_Query']['Elevation']
            except:
                print('error retrieving elevation for Lon: {} & Lat: {}. Skipping'.format(row.Lon_X, row.Lat_Y))
                continue

            unit = 'meters'
            method = 'Other'
            cursor.execute('''
                UPDATE Stations set Elev=?, ElevUnit=?, ElevMeth=?
                WHERE Id=?
            ''', elev, unit, method, row.Id)

            i += 1
            if i % batch_size == 0:
                connection.commit()
                print('{} out of {} completed ({}%)'.format(i, total, (i/float(total)*100.00)))

        self._update_params_table(who)

    def update(self, source, who, location):
        db = self._get_db(who)

        programs = self._parse_source_args(source)

        for program in programs:
            seederClass = factory.get(program)

            seeder = seederClass(db=db,
                                 update=True,
                                 source=location,
                                 secrets=secrets.sdwis,
                                 sql_statements=sql.sql_statements,
                                 update_row=sql.update_row,
                                 insert_rows=sql.insert_rows,
                                 cursor_factory=sql.create_cursor,
                                 arcpy=arcpy)
            seeder.update()

        self._update_params_table(who)

    def _parse_source_args(self, source):
        all_sources = ['WQP', 'SDWIS', 'DOGM', 'UDWR', 'UGS']
        if not source:
            return all_sources
        else:
            sources = [s.strip().upper() for s in source.split(',')]
            sources = filter(lambda s: s in all_sources, sources)
            if len(sources) > 0:
                return sources
            else:
                return None

    def _update_params_table(self, who):
        script_dir = dirname(__file__)
        with open(join(script_dir, join('..', '..', 'scripts', 'populateParamsTable.sql')), 'r') as f:
            sql = f.read()

        try:
            c = pyodbc.connect(self._get_db(who)['connection_string'])
            cursor = c.cursor()
            cursor.execute(sql)
        except Exception, e:
            raise e
        finally:
            if cursor:
                del cursor
            if c:
                del c
