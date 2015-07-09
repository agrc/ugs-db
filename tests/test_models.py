#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
services
----------------------------------
test the models module
'''

import unittest
import dbseeder.models as model


class TestTable(unittest.TestCase):

    def setUp(self):
        self.patient = model.Table()
        self.schemas = [
            'base_schema',
            'final_schema',
        ]
        self.schema = [{
            'SHAPE@': {
                'type': 'shape',
                'map': 'MAPPED-SHAPE@',
                'order': 0,
                'etl': {
                    'in': 'POLY',
                    'out': 'LINE',
                    'method': 'poly_to_line'
                }
            },
            '*Type': {
                'type': 'string',
                'map': 'MAPPED-Type',
                'value': 'Guzzler',
                'order': 1
            },
            'Status': {
                'type': 'string',
                'map': 'MAPPED-Status',
                'lookup': 'status',
                'order': 2
            },
            '!Status': {
                'type': 'string',
                'map': 'DOUBLE-Status',
                'lookup': 'status_code',
                'order': 3
            }
        }, {
            'SHAPE@': {
                'type': 'shape',
                'map': 'MAPPED-SHAPE@',
                'order': 0
            },
            '*Type': {
                'type': 'string',
                'map': 'MAPPED-Type',
                'value': 'Guzzler',
                'order': 1
            },
            'Completed': {
                'type': 'string',
                'map': 'MAPPED-Completed',
                'order': 2
            },
            '*Status': {
                'type': 'string',
                'map': 'MAPPED-Status',
                'value': 'Complete',
                'order': 3
            }
        }]

        self.patient.set_schema(False, self.schema)

    def test_format_source_table(self):
        is_final = False

        actual = self.patient.format_source_table('{}{}', ['owner', is_final])
        expected = 'owner'

        self.assertEqual(actual, expected)

    def test_format_source_table_final(self):
        is_final = True

        actual = self.patient.format_source_table('{}{}', ['owner', is_final])
        expected = 'ownerFINAL'

        self.assertEqual(actual, expected)

    def test_set_schema(self):
        is_final = False

        actual = self.patient.set_schema(is_final, self.schemas)

        expected = 'base_schema'

        self.assertEqual(actual, expected)

    def test_set_schema_final(self):
        is_final = True

        actual = self.patient.set_schema(is_final, self.schemas)

        expected = 'final_schema'

        self.assertEqual(actual, expected)

    def test_destination_fields(self):
        is_final = False

        self.patient.set_schema(is_final, self.schema)

        actual = self.patient.destination_fields()
        expected = [
            'MAPPED-SHAPE@',
            'MAPPED-Type',
            'MAPPED-Status',
            'DOUBLE-Status',
        ]

        self.assertListEqual(actual, expected)

    def test_destination_fields_final(self):
        is_final = True

        self.patient.set_schema(is_final, self.schema)

        actual = self.patient.destination_fields()
        expected = [
            'MAPPED-SHAPE@',
            'MAPPED-Type',
            'MAPPED-Completed',
            'MAPPED-Status',
        ]

        self.assertListEqual(actual, expected)

    def test_source_fields(self):
        is_final = False

        self.patient.set_schema(is_final, self.schema)

        actual = self.patient.source_fields()
        expected = [
            'SHAPE@',
            'Status',
            'Status',
        ]

        self.assertListEqual(actual, expected)

    def test_source_fields_strip_bang(self):
        is_final = False

        self.patient.set_schema(is_final, self.schema)

        actual = self.patient.source_fields(strip_bang=False)
        expected = [
            'SHAPE@',
            'Status',
            '!Status',
        ]

        self.assertListEqual(actual, expected)

    def test_source_fields_final(self):
        is_final = True

        self.patient.set_schema(is_final, self.schema)

        actual = self.patient.source_fields()
        expected = [
            'SHAPE@',
            'Completed',
        ]

        self.assertListEqual(actual, expected)

    def test_unmapped_fields(self):
        is_final = False

        self.patient.set_schema(is_final, self.schema)

        actual = self.patient.unmapped_fields()
        expected = [(1, '*Type')]

        self.assertListEqual(actual, expected)

    def test_unmapped_fields_final(self):
        is_final = True

        self.patient.set_schema(is_final, self.schema)

        actual = self.patient.unmapped_fields()
        expected = [(1, '*Type'), (3, '*Status')]

        self.assertListEqual(actual, expected)

    def test_etl_fields(self):
        actual = self.patient.etl_fields()
        expected = [(0, {
                    'in': 'POLY',
                    'out': 'LINE',
                    'method': 'poly_to_line'
                    })]

        self.assertListEqual(actual, expected)

    def test_etl_fields_final(self):
        is_final = True

        self.patient.set_schema(is_final, self.schema)

        actual = self.patient.etl_fields()
        expected = []

        self.assertListEqual(actual, expected)

    def test_merge_data(self):
        row = ('shape', 'status', '!status')

        expected = [('SHAPE@', 'MAPPED-SHAPE@', 'shape'),
                    ('*Type', 'MAPPED-Type', None),
                    ('Status', 'MAPPED-Status', 'status'),
                    ('!Status', 'DOUBLE-Status', '!status')]
        actual = self.patient.merge_data(row)

        self.assertEqual(actual, expected)

if __name__ == '__main__':
    unittest.main()
