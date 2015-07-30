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
    def test_can_cast_simple_row(self):
        number = 12345
        simple_row = {
            'OrgId': number
        }
        schema = OrderedDict([
            ('OrgId', {
                'alias': 'Organization Id',
                'type': 'String',
                'length': 2
            })
        ])

        actual = Caster.cast(simple_row, schema)
        self.assertEqual(actual, {
            'OrgId': '12'
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
