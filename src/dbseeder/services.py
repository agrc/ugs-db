#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
services.py
----------------------------------
modules for acting on items
'''

import datetime
# from dateutil.parser import parse
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

    @staticmethod
    def cast(row, schema):
        '''Given a {string, string} dictionary (row) and the schema
        for the row (result or station) a new {string, string} dictionary
        (row) is returned with the values properly formatted.
        '''

        def cast_field(row, field):
            print(field)
            value = row[field[0]]

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
            # elif field.type == 'Date':
            #     if isinstance(value, datetime.datetime):
            #         cast = lambda x: x
            #     elif value == '':
            #         return None
            #     else:
            #         cast = parse

            try:
                value = cast(value)

                if value == '':
                    return {field[0]: None}

                try:
                    if field[1]['length']:
                        value = value[:field[1]['length']]
                except KeyError:
                    pass

                return {field[0]: value}
            except:
                return {field[0]: None}

        row = map(lambda field: cast_field(row, field), schema.items())

        return row[0]
        # if destination_value is None:
        #     return None
        #
        # try:
        #     destination_value = destination_value.strip()
        # except:
        #     pass
        #
        # if destination_field_type == 'TEXT':
        #     cast = str
        # elif destination_field_type == 'LONG':
        #     cast = long
        # elif destination_field_type == 'SHORT':
        #     cast = int
        # elif (destination_field_type == 'FLOAT' or
        #       destination_field_type == 'DOUBLE'):
        #     cast = float
        # elif destination_field_type == 'DATE':
        #     if isinstance(destination_value, datetime.datetime):
        #         cast = lambda x: x
        #     elif destination_value == '':
        #         return None
        #     else:
        #         cast = parse
        #
        # try:
        #     value = cast(destination_value)
        #
        #     if value == '':
        #         return None
        #
        #     return value
        # except:
        #     return None
