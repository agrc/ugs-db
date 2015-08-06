#!usr/bin/env python
# -*- coding: utf-8 -*-

'''
services
----------------------------------
test the services module
'''

import unittest
from collections import OrderedDict
from dbseeder.services import Caster, Reproject, ChargeBalancer, Normalizer
from dbseeder.models import Concentration


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


class TestNormalizer(unittest.TestCase):

    def setUp(self):
        self.patient = Normalizer()

    def test_unit_is_unchanged_if_chemical_is_none(self):
        row = self.patient.normalize_sample({
            'Param': None,
            'Unit': 'unit',
            'ResultValue': 0
        })
        self.assertEqual(row['Unit'], 'unit')


class TestChargeBalancer(unittest.TestCase):

    def setUp(self):
        self.patient = ChargeBalancer()

    def test_calculate_charge_balance_correctly(self):
        # sample = nwisaz.01.92600003
        expected_balance = 0.27
        expected_cations = 10.45
        expected_anions = 10.39

        dep = Concentration()

        dep._set('Bicarbonate', 188, None)
        dep._set('Calcium', 66, None)
        dep._set('Chloride', 57, None)
        dep._set('Magnesium', 27, None)
        dep._set('Nitrate', 0.8, None)
        dep._set('Potassium', 7.4, None)
        dep._set('Sodium', 109, None)
        dep._set('Sulfate', 273, None)

        balance, cations, anions = self.patient.calculate_charge_balance(dep)

        self.assertEqual(balance, expected_balance)
        self.assertEqual(cations, expected_cations)
        self.assertEqual(anions, expected_anions)

        # sample = nwisaz.01.92600006
        expected_balance = -0.02
        expected_cations = 4.21
        expected_anions = 4.21

        dep = Concentration()

        dep._set('Bicarbonate', 139, None)
        dep._set('Calcium', 46, None)
        dep._set('Chloride', 12, None)
        dep._set('Magnesium', 10, None)
        dep._set('Nitrate', 0.5, None)
        dep._set('Sodium plus potassium', 25, None)
        dep._set('Sulfate', 76, None)

        balance, cations, anions = self.patient.calculate_charge_balance(dep)

        self.assertEqual(balance, expected_balance)
        self.assertEqual(cations, expected_cations)
        self.assertEqual(anions, expected_anions)
