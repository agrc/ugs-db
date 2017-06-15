#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
dbseeder.py
----------------------------------
the main entry point module for database ETL
'''

import pyodbc
from . import factory
import logging
import requests
from . import sql
import sys
from os.path import join, dirname
try:
    from . import ugssecrets
except ImportError:
    from . import ugssecrets_sample as ugssecrets
try:
    import arcpy
except ImportError:
    from . import arcpy_mock as arcpy


class Seeder(object):

    def __init__(self, logger_name=None):
        if logger_name is None:
            self.logger = logging.getLogger('ugs-db')

            detailed_formatter = logging.Formatter(fmt='%(levelname)-7s %(asctime)s %(module)10s:%(lineno)5s %(message)s',
                                                   datefmt='%m-%d %H:%M:%S')
            console_handler = logging.StreamHandler(stream=sys.stdout)
            console_handler.setFormatter(detailed_formatter)
            console_handler.setLevel('INFO')

            self.logger.addHandler(console_handler)
            self.logger.setLevel('INFO')
        else:
            self.logger = logging.getLogger(logger_name)

    def _get_db(self, who):
        db = ugssecrets.dev
        if who == 'stage':
            db = ugssecrets.stage
        elif who == 'prod':
            db = ugssecrets.prod
        return db

    def create_tables(self, who):
        db = self._get_db(who)

        self.logger.info('connecting to {} database'.format(who))

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
        except Exception as e:
            raise e
        finally:
            if cursor is not None:
                del cursor
            if c is not None:
                del c

        self.logger.info('done')

        return True

    def seed(self, source, file_location, who):
        db = self._get_db(who)

        programs = self._parse_source_args(source)

        for program in programs:
            seederClass = factory.get(program)

            seeder = seederClass(self.logger,
                                 db=db,
                                 update=False,
                                 source=file_location,
                                 secrets=ugssecrets.sdwis,
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
        stations_fc = 'UGSWaterChemistry.ugswaterchemistry.Stations'
        stations_identity = 'Stations_identity'
        epqs_service_url = r'http://nationalmap.gov/epqs/pqs.php'

        arcpy.env.workspace = dirname(__file__)
        db = r'connection_files\{}.sde'.format(who)

        #: FIPS
        self.logger.info('creating stations layer')
        stationsLyr = arcpy.MakeFeatureLayer_management(join(db, stations_fc), 'StationsLyr')

        self.logger.info('identity on counties')
        stationsIdent = arcpy.Identity_analysis(stationsLyr,
                                                r'ReferenceData.gdb\US_Counties',
                                                join('in_memory', stations_identity))

        self.logger.info('joining to layer')
        arcpy.AddJoin_management(stationsLyr, 'Id', stationsIdent, 'FID_' + stations_fc.split('.')[2])

        self.logger.info('calculating state')
        arcpy.CalculateField_management(stationsLyr, 'StateCode', '!STATE_FIPS!', 'PYTHON')
        self.logger.info('calculating county')
        arcpy.CalculateField_management(stationsLyr, 'CountyCode', '!FIPS!', 'PYTHON')

        self.logger.info('removing join')
        arcpy.RemoveJoin_management(stationsLyr, stations_identity)

        #: Elevation
        self.logger.info('looping through points with null elevation values')
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
                self.logger.info('error retrieving elevation for Lon: {} & Lat: {}. Skipping'.format(row.Lon_X, row.Lat_Y))
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
                self.logger.info('{} out of {} completed ({}%)'.format(i, total, (i/float(total)*100.00)))

        self._update_params_table(who)

    def update(self, source, who, location, postprocess):
        db = self._get_db(who)

        programs = self._parse_source_args(source)

        for program in programs:
            seederClass = factory.get(program)

            seeder = seederClass(self.logger,
                                 db=db,
                                 update=True,
                                 source=location,
                                 secrets=ugssecrets.sdwis,
                                 sql_statements=sql.sql_statements,
                                 update_row=sql.update_row,
                                 insert_rows=sql.insert_rows,
                                 cursor_factory=sql.create_cursor,
                                 arcpy=arcpy)
            seeder.update()

        self._update_params_table(who)

        if postprocess:
            self.post_process(who)

    def _parse_source_args(self, source):
        all_sources = ['WQP', 'SDWIS', 'DOGM', 'UDWR', 'UGS']
        if not source:
            return all_sources
        else:
            sources = [s.strip().upper() for s in source.split(',')]
            sources = [s for s in sources if s in all_sources]
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
        except Exception as e:
            raise e
        finally:
            if cursor:
                del cursor
            if c:
                del c
