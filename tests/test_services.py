#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_services
----------------------------------

Tests for `services` module.
"""
import dbseeder.services as service
import unittest
from dbseeder.models import Concentration
from dbseeder.modelextensions import Normalizable


class TestCaster(unittest.TestCase):

    def test_empty_string_returns_none(self):
        actual = service.Caster.cast('', 'TEXT')
        self.assertIsNone(actual, msg='text')

        actual = service.Caster.cast('', 'LONG')
        self.assertIsNone(actual, msg='long')

        actual = service.Caster.cast('', 'SHORT')
        self.assertIsNone(actual, msg='short')

        actual = service.Caster.cast('', 'DATE')
        self.assertIsNone(actual, msg='date')

        actual = service.Caster.cast('', 'FLOAT')
        self.assertIsNone(actual, msg='float')

        actual = service.Caster.cast('', 'DOUBLE')
        self.assertIsNone(actual, msg='double')


class TestNormalizer(unittest.TestCase):

    def setUp(self):
        self.patient = service.Normalizer()

    def test_unit_is_unchanged_if_chemical_is_none(self):
        amount, unit, chemical = self.patient.normalize_unit(None, 'unit', 0)
        self.assertEqual(unit, 'unit')

    def test_station_id_normalization(self):
        self.patient = Normalizable(self.patient)

        self.patient.normalize_fields['stationid'] = ('UTAHDWQ_WQX-4946750', 0)

        actual = self.patient.normalize(['UTAHDWQ_WQX-4946750', 'Junk'])

        self.assertListEqual(actual, ['UTAHDWQ-4946750', 'Junk'])


class TestChargeBalancer(unittest.TestCase):

    def setUp(self):
        self.patient = service.ChargeBalancer()

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
