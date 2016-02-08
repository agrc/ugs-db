#!usr/bin/env python
# -*- coding: utf-8 -*-

'''
test_sql.py
----------------------------------
test the sql module
'''

import unittest
from dbseeder.sql import update_row


class TestSqlFunctions(unittest.TestCase):
    def test_update_row_with_valid_lat_lon(self):
        row = {
            'Shape': None,
            'Lon_X': -114,
            'Lat_Y': 40
        }
        datasource = 'testing'
        actual = update_row(row, datasource)
        expected = 'geometry::STGeomFromText(\'POINT ({} {})\', 26912)'.format(243900.352024, 4432069.05679)

        self.assertEqual(actual['Shape'], expected)
        self.assertEqual(actual['DataSource'], datasource)

    def test_shape_is_none_with_invalid_lat_lon(self):
        row = {
            'Shape': None,
            'Lon_X': None,
            'Lat_Y': 40
        }
        datasource = 'testing'
        actual = update_row(row, datasource)

        self.assertIsNone(actual['Shape'])
        self.assertEqual(actual['DataSource'], datasource)

    def test_arcpy_shape_xy(self):
        row = {
            'Shape@XY': (-114, 40),
            'Lon_X': 4,
            'Lat_Y': 5
        }

        datasource = 'testing'
        actual = update_row(row, datasource)

        expected = 'geometry::STGeomFromText(\'POINT ({} {})\', 26912)'.format(243900.352024, 4432069.05679)

        self.assertEqual(actual['Shape'], expected)
        self.assertEqual(actual['DataSource'], datasource)
        self.assertFalse('Shape@XY' in actual)
