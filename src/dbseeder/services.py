#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
services.py
----------------------------------
modules for acting on items
'''

import datetime
import re
from dateutil.parser import parse
from pyproj import Proj, transform


class Reproject(object):
    '''A utility class for reprojecting points'''

    input_system = Proj(init='epsg:4326')
    ouput_system = Proj(init='epsg:26912')

    @classmethod
    def to_utm(cls, x, y):
        '''reproject x and y from 4326 to 26912'''

        if x > 0:
            x = x * -1

        return transform(cls.input_system, cls.ouput_system, x, y)


class Caster(object):
    '''A utility class for casting data to its defined schema type'''

    wqx_re = re.compile('(_WQX)-')

    @classmethod
    def cast(cls, row, schema):
        '''Given a {string, string} dictionary (row) and the schema
        for the row (result or station) a new {string, string} dictionary
        (row) is returned with the values properly formatted.
        '''

        for field in schema.iteritems():
            def strip_wxp(id):
                return re.sub(cls.wqx_re, '-', id)

            #: if the value is not in the csv skip it
            try:
                value = row[field[0]]
            except KeyError:
                row[field[0]] = None
                continue

            #: try to remove all whitespace from strings
            try:
                value = value.strip()
            except:
                pass

            if field[1]['type'] == 'String':
                cast = str
            elif field[1]['type'] == 'Long Int':
                cast = long
            elif field[1]['type'] == 'Short Int':
                cast = int
            elif field[1]['type'] == 'Double':
                cast = float
            elif field.type == 'Date':
                if isinstance(value, datetime.datetime):
                    cast = lambda x: x
                elif value == '':
                    row[field[0]] = None
                    continue
                else:
                    cast = parse

            try:
                value = cast(value)
            except:
                row[field[0]] = None
                continue

            if value == '':
                row[field[0]] = None
                continue

            try:
                if field[1]['length']:
                    value = value[:field[1]['length']]
            except KeyError:
                pass

            try:
                if field[1]['actions']:
                    for action in field[1]['actions']:
                        value = locals()[action](value)
            except KeyError:
                pass

            row[field[0]] = value

        return row
