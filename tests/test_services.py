#!usr/bin/env python
# -*- coding: utf-8 -*-

'''
services
----------------------------------
test the services module
'''

import unittest
from collections import OrderedDict
from dbseeder.services import Caster, Reproject


class TestCaster(unittest.TestCase):
    def test_can_cast_to_string_and_truncate(self):
        number = 12345
        simple_row = {
            'OrgId': number
        }
        schema = OrderedDict([
            ('OrgId', {
                'type': 'String',
                'length': 2
            })
        ])

        actual = Caster.cast(simple_row, schema)
        self.assertEqual(actual, {
            'OrgId': '12'
        })

    def test_can_call_actions(self):
        simple_row = {
            'StationId': 'ABC_WQX-abc'
        }
        schema = OrderedDict([
            ('StationId', {
                'type': 'String',
                'actions': ['strip_wxp']
            })
        ])

        actual = Caster.cast(simple_row, schema)
        self.assertEqual(actual, {
            'StationId': 'ABC-abc'
        })

    def test_output_format(self):
        number = 12345
        simple_row = {
            'OrgId': number,
            'StationId': 'ABC_WQX-abc'
        }
        schema = OrderedDict([
            ('OrgId', {
                'type': 'String',
                'length': 2
            }),
            ('StationId', {
                'type': 'String',
                'actions': ['strip_wxp']
            })
        ])

        actual = Caster.cast(simple_row, schema)
        self.assertEqual(actual, {
            'OrgId': '12',
            'StationId': 'ABC-abc'
        })

        def test_can_call_actions(self):
            simple_row = {
                'StationId': 'ABC_WQX-abc'
            }
            schema = OrderedDict([
                ('StationId', {
                    'type': 'String',
                    'actions': ['strip_wxp']
                })
            ])

            actual = Caster.cast(simple_row, schema)
            self.assertEqual(actual, {
                'StationId': 'ABC-abc'
            })

    def test_returns_none_for_missing_schema_items(self):
        simple_row = {
            'Something': 'does not get removed'
        }
        schema = OrderedDict([
            ('Missing', {
                'type': 'String'
            })
        ])

        actual = Caster.cast(simple_row, schema)
        self.assertEqual(actual, {
            'Something': 'does not get removed',
            'Missing': None
        })


class TestReproject(unittest.TestCase):
    def test_inverts_impropert_longitudes(self):
        actual = Reproject.to_utm(120, 40)
        valid = Reproject.to_utm(-120, 40)

        self.assertEqual(actual, valid)

        actual = Reproject.to_utm(-114, 40)
        expected = (243900.35202192425, 4432069.056632294)
        self.assertAlmostEqual(actual[0], expected[0], places=0)
        self.assertAlmostEqual(actual[1], expected[1], places=0)
