#!usr/bin/env python
# -*- coding: utf-8 -*-

'''
services
----------------------------------
test the services module
'''

import unittest
from collections import OrderedDict
from ugsdbseeder.services import Caster, Reproject, ChargeBalancer, Normalizer
from ugsdbseeder.models import Concentration
import datetime


class TestCaster_Cast(unittest.TestCase):
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

    def test_output_format(self):
        number = 12345
        simple_row = {
            'OrgId': number,
            'StationId': 'ABC-abc'
        }
        schema = OrderedDict([
            ('OrgId', {
                'type': 'String',
                'length': 2
            }),
            ('StationId', {
                'type': 'String'
            })
        ])

        actual = Caster.cast(simple_row, schema)
        self.assertEqual(actual, {
            'OrgId': '12',
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

    def test_strips_whitespace(self):
        simple_row = {
            'Something': '    padded    '
        }
        schema = OrderedDict([
            ('Something', {
                'type': 'String'
            })
        ])

        actual = Caster.cast(simple_row, schema)
        self.assertEqual(actual, {
            'Something': 'padded',
        })

    def test_sets_dates_in_future_to_none(self):
        simple_row = {
            'Something': '2060-02-24'
        }
        schema = OrderedDict([
            ('Something', {
                'type': 'Date'
            })
        ])

        actual = Caster.cast(simple_row, schema)
        self.assertEqual(actual, {
            'Something': None,
        })

    def test_sets_dates_prior_to_1800s_to_none(self):
        simple_row = {
            'Something': '1799-02-24'
        }
        schema = OrderedDict([
            ('Something', {
                'type': 'Date'
            })
        ])

        actual = Caster.cast(simple_row, schema)
        self.assertEqual(actual, {
            'Something': None,
        })


class TestCaster_CastForSQL(unittest.TestCase):
    def test_doesnt_touch_shape(self):
        input = {'Shape': 'blah'}
        actual = Caster.cast_for_sql(input)

        self.assertEqual(actual['Shape'], 'blah')

    def test_quotes_strings(self):
        input = {'hello': 'blah', 'hello2': 'blah2'}
        actual = Caster.cast_for_sql(input)

        self.assertEqual(actual['hello'], "'blah'")
        self.assertEqual(actual['hello2'], "'blah2'")

    def test_escape_quotes_in_strings(self):
        input = {'hello': 'bl\'ah', 'hello2': 'bl\'\'ah2'}
        actual = Caster.cast_for_sql(input)

        self.assertEqual(actual['hello'], "'bl''ah'")
        self.assertEqual(actual['hello2'], "'bl''''ah2'")

    def test_casts_dates(self):
        input = {'date': datetime.datetime(2015, 8, 11)}
        actual = Caster.cast_for_sql(input)

        self.assertEqual(actual['date'], "Cast('2015-08-11' as date)")

    def test_casts_old_dates(self):
        input = {'date': datetime.datetime(1800, 8, 11)}
        actual = Caster.cast_for_sql(input)

        self.assertEqual(actual['date'], "Cast('1800-08-11' as date)")

    def test_times(self):
        input = {'time': datetime.time(4, 30, 11)}
        actual = Caster.cast_for_sql(input)

        self.assertEqual(actual['time'], "'04:30:11'")

    def test_null_for_none(self):
        input = {'test': None}
        actual = Caster.cast_for_sql(input)

        self.assertEqual(actual['test'], 'Null')

    def test_convert_numbers_to_strings(self):
        input = {'num': 1.2345}
        actual = Caster.cast_for_sql(input)

        self.assertEqual(actual['num'], '1.2345')


class TestReproject(unittest.TestCase):
    def test_inverts_impropert_longitudes(self):
        actual = Reproject.to_utm(120, 40)
        valid = Reproject.to_utm(-120, 40)

        self.assertEqual(actual, valid)

        actual = Reproject.to_utm(-114, 40)
        expected = (243900.35202192425, 4432069.056632294)
        self.assertAlmostEqual(actual[0], expected[0], places=0)
        self.assertAlmostEqual(actual[1], expected[1], places=0)


class TestNormalizer_NormalizeSample(unittest.TestCase):
    def setUp(self):
        self.patient = Normalizer()

    def test_unit_is_unchanged_if_chemical_is_none(self):
        row = self.patient.normalize_sample({
            'StationId': 'ABC_WQX-abc',
            'Param': None,
            'Unit': 'unit',
            'ResultValue': 0
        })
        self.assertEqual(row['Unit'], 'unit')

    def test_sample_is_expanded(self):
        row = self.patient.normalize_sample({
            'StationId': 'ABC_WQX-abc',
            'Param': None,
            'Unit': 'unit',
            'ResultValue': 0,
            'SampMedia': 'WL'
        })
        self.assertEqual(row['SampMedia'], 'Groundwater')
        self.assertEqual(row['StationId'], 'ABC-abc')

    def test_sample_is_skipped_if_none(self):
        row = self.patient.normalize_sample({
            'StationId': 'ABC_WQX-abc',
            'Param': None,
            'Unit': 'unit',
            'ResultValue': 0,
            'SampMedia': None
        })
        self.assertEqual(row['SampMedia'], None)
        self.assertEqual(row['StationId'], 'ABC-abc')

    def test_sample_is_passed_on_if_not_in_lookup(self):
        row = self.patient.normalize_sample({
            'StationId': 'ABC_WQX-abc',
            'Param': None,
            'Unit': 'unit',
            'ResultValue': 0,
            'SampMedia': 'test'
        })
        self.assertEqual(row['SampMedia'], 'test')
        self.assertEqual(row['StationId'], 'ABC-abc')

    def test_strips_wxp(self):
        row = self.patient.normalize_sample({
            'StationId': 'ABC_WQX-abc',
            'Param': None,
            'Unit': 'unit',
            'ResultValue': 0
        })
        self.assertEqual(row['StationId'], 'ABC-abc')


class TestNormalizer_NormlizeStation(unittest.TestCase):
    def setUp(self):
        self.patient = Normalizer()

    def test_strips_wxp(self):
        row = self.patient.normalize_station({
            'StationId': 'ABC_WQX-abc'
        })
        self.assertEqual(row['StationId'], 'ABC-abc')

    def test_normalizes_station_types(self):
        row = self.patient.normalize_station({
            'StationId': 'ABC_WQX-abc',
            'StationType': 'Cave'
        })
        self.assertEqual(row['StationType'], 'Other Groundwater')


class TestNormalizer_ReorderFilter(unittest.TestCase):
    schema = OrderedDict([
        ('a', {}),
        ('b', {}),
        ('c', {})
    ])

    def setUp(self):
        self.patient = Normalizer()

    def test_reorders_row_according_to_schema(self):
        row = {'a': 1, 'c': 3, 'b': 2}
        expected = {'a': 1, 'b': 2, 'c': 3}

        self.assertEqual(self.patient.reorder_filter(row, self.schema), expected)

    def test_filters_values_not_in_schema(self):
        row = {'a': 1, 'c': 3, 'b': 2, 'd': 5}
        expected = {'a': 1, 'b': 2, 'c': 3}

        self.assertEqual(self.patient.reorder_filter(row, self.schema), expected)


class TestChargeBalancer(unittest.TestCase):

    def setUp(self):
        self.patient = ChargeBalancer

    def test_calculate_charge_balance_correctly(self):
        sampleId = 'nwisaz.01.92600003'
        expected_balance = 0.27
        expected_cations = 10.45
        expected_anions = 10.39

        dep = Concentration()

        dep.set('Bicarbonate', 188, None)
        dep.set('Calcium', 66, None)
        dep.set('Chloride', 57, None)
        dep.set('Magnesium', 27, None)
        dep.set('Nitrate', 0.8, None)
        dep.set('Potassium', 7.4, None)
        dep.set('Sodium', 109, None)
        dep.set('Sulfate', 273, None)

        rows = self.patient.calculate_charge_balance(dep, sampleId)

        self.assertEqual(rows[0]['SampleId'], sampleId)
        self.assertEqual(rows[0]['Param'], 'Charge Balance')
        self.assertEqual(rows[0]['ResultValue'], expected_balance)
        self.assertEqual(rows[1]['SampleId'], sampleId)
        self.assertEqual(rows[1]['Param'], 'Cation Total')
        self.assertEqual(rows[1]['ResultValue'], expected_cations)
        self.assertEqual(rows[2]['SampleId'], sampleId)
        self.assertEqual(rows[2]['Param'], 'Anions Total')
        self.assertEqual(rows[2]['ResultValue'], expected_anions)

        expected_balance = -0.02
        expected_cations = 4.21
        expected_anions = 4.21

        dep = Concentration()

        dep.set('Bicarbonate', 139, None)
        dep.set('Calcium', 46, None)
        dep.set('Chloride', 12, None)
        dep.set('Magnesium', 10, None)
        dep.set('Nitrate', 0.5, None)
        dep.set('Sodium plus potassium', 25, None)
        dep.set('Sulfate', 76, None)

        rows = self.patient.calculate_charge_balance(dep, sampleId)

        self.assertEqual(rows[0]['SampleId'], sampleId)
        self.assertEqual(rows[0]['Param'], 'Charge Balance')
        self.assertEqual(rows[0]['ResultValue'], expected_balance)
        self.assertEqual(rows[1]['SampleId'], sampleId)
        self.assertEqual(rows[1]['Param'], 'Cation Total')
        self.assertEqual(rows[1]['ResultValue'], expected_cations)
        self.assertEqual(rows[2]['SampleId'], sampleId)
        self.assertEqual(rows[2]['Param'], 'Anions Total')
        self.assertEqual(rows[2]['ResultValue'], expected_anions)

    def test_fills_in_other_fields_with_none(self):
        sampleId = 'nwisaz.01.92600003'

        dep = Concentration()

        dep.set('Bicarbonate', 188, None)
        dep.set('Calcium', 66, None)
        dep.set('Chloride', 57, None)
        dep.set('Magnesium', 27, None)
        dep.set('Nitrate', 0.8, None)
        dep.set('Potassium', 7.4, None)
        dep.set('Sodium', 109, None)
        dep.set('Sulfate', 273, None)

        rows = self.patient.calculate_charge_balance(dep, sampleId)

        self.assertEqual(len(rows[0]), 42)
        self.assertIsNone(rows[0]['AnalysisDate'])
