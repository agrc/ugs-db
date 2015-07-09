#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
dbseeder
----------------------------------
the dbseeder module
'''

import pyodbc
from os.path import join, dirname, isdir
from glob import glob


class Seeder(object):

    def create_tables(self, who):
        try:
            import secrets
        except Exception:
            import secrets_sample as secrets

        db = secrets.dev
        if who == 'stage':
            db = secrets.stage
        elif who == 'prod':
            db = secrets.prod

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
        programs = self._parse_source_args(source)

        for program in programs:
            seederClass = Factory(program)

            seeder = seederClass(file_location)
            seeder.seed()

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


class WqpProgram(object):
    '''class for handling wqp csv files'''

    datasouce = 'WQP'

    def __init__(self, file_location):
        super(WqpProgram, self).__init__()

        #: check that file_location exists wqp/results and wqp/stations
        parent_folder = join(file_location, self.datasouce)

        if not isdir(parent_folder):
            raise Exception('Pass in a location to the parent folder that contains WQP. {}'.format(parent_folder))

        results_folder = join(parent_folder, 'Results')
        stations_folder = join(parent_folder, 'Stations')

        if not isdir(results_folder or stations_folder):
            raise Exception('WQP folder is missing Results or Stations child folders containing csv files.')

        #: all is well set the folders for seeding later on
        self.results_folder = results_folder
        self.stations_folder = stations_folder

    def seed(self):
        #: loop over each file
        #: handle stations first so you can validate results
        #: csv query for distinct sampleid's
        #: loop over query results and search for all sampleid's
        #: for each group of sampleid's
        #: etl the resulting rows
        #: normalize chemical units
        #: create charge balance
        #: return rows
        for csv_file in self._get_files(self.stations_folder):
            print(csv_file)

        for csv_file in self._get_files(self.results_folder):
            print(csv_file)

    def _get_files(self, location):
        if not location:
            raise Exception('Pass in a location containing csv files to import.')

        files = glob(join(location, '*.csv'))

        if len(files) < 1:
            raise Exception(location, 'No csv files found.')

        return files


class Factory(object):
    """return the program from the string source"""
    def __init__(self, source):
        if source == 'WQP':
            return WqpProgram
