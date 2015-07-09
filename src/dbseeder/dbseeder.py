#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
dbseeder
----------------------------------
the dbseeder module
'''

import pyodbc
try:
    import secrets
except Exception as e:
    import secrets_sample as secrets

from os.path import join, dirname


class Seeder(object):
    def create_tables(self, who):
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

    def seed(self, source, who):
        print('{}, {}'.format(source, who))

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
