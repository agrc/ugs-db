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


class TestRreproject(unittest.TestCase):
    def test_inverts_impropert_longitudes(self):
        actual = Reproject.to_utm(120, 40)
        valid = Reproject.to_utm(-120, 40)

        self.assertEqual(actual, valid)
        self.asertEqual(actual, ('whatever this is', 'in utm'))
