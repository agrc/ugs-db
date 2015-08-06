#!usr/bin/env python
# -*- coding: utf-8 -*-

'''
programs
----------------------------------
test the programs module
'''

import unittest
from dbseeder.programs import WqpProgram
from collections import OrderedDict
from nose.tools import raises
from os.path import join, basename


class TestWqpProgram(unittest.TestCase):
    def setUp(self):
        self.test_get_files_folder = join('tests', 'data', 'WQP', 'get_files')
        self.patient = WqpProgram(self.test_get_files_folder, 'bad db connection')

    def test_get_files_finds_files(self):
        expected_results = [
            'sample_chemistry.csv',
            'sample_chemistry2.csv'
        ]

        expected_stations = [
            'sample_stations.csv'
        ]

        results_folder = map(basename, self.patient._get_files(self.patient.results_folder))
        stations_folder = map(basename, self.patient._get_files(self.patient.stations_folder))

        self.assertItemsEqual(results_folder, expected_results)
        self.assertItemsEqual(stations_folder, expected_stations)

    @raises(Exception)
    def test_get_files_with_empty_location_throws(self):
        self.patient._get_files(None)

    @raises(Exception)
    def test_get_files_with_no_csv_found_throws(self):
        self.patient._get_files(join('tests', 'data', 'WQP', 'empty_folders', 'Stations'))

    @raises(Exception)
    def test_folder_without_required_folders_throws(self):
        self.patient = WqpProgram(join('tests', 'data', 'WQP', 'incorrect_structure'))

    @raises(Exception)
    def test_folder_without_required_child_folders_throws(self):
        self.patient = WqpProgram(join('tests', 'data', 'WQP', 'incorrect_child_structure'))

    def test_get_samples_for_id_returns_correct_list(self):
        self.patient.sample_id_field = 'id'
        config = OrderedDict([('a', 'eh'), ('b', 'bee'), ('c', 'sea')])
        sample_id_set = (1,)
        file_path = join('tests', 'data', 'WQP', 'get_sample_ids.csv')

        rows = self.patient._get_samples_for_id(sample_id_set, file_path, config=config)

        self.assertEqual(len(rows), 2)
        self.assertItemsEqual([
                              {'eh': 'a1', 'bee': 'b1', 'sea': 'c1', 'ActivityIdentifier': '1'},
                              {'eh': 'a2', 'bee': 'b2', 'sea': 'c2', 'ActivityIdentifier': '1'}
                              ], rows)

    def test_get_distict_samples(self):
        self.patient.sample_id_field = 'id'
        rows = self.patient._get_distinct_sample_ids_from(join('tests', 'data', 'WQP', 'distinct_sampleids.csv'))

        self.assertEqual(len(rows), 2)
        self.assertItemsEqual([('1',), ('2',)], rows)

    def test_get_wxps_duplicate_ids(self):
        ids = self.patient._get_wxps_duplicate_ids(join('tests', 'data', 'WQP', 'wqxids.csv'))

        self.assertItemsEqual(set([u'UTAHDWQ-4904410', u'UTAHDWQ-4904640', u'UTAHDWQ-4904610']), ids)

    def test_update_shape_with_valid_lat_lon(self):
        row = {
            'Shape': None,
            'Lon_X': -114,
            'Lat_Y': 40
        }
        actual = self.patient._update_shape(row)['Shape']
        expected = 'geometry::STGeomFromText(\'POINT ({} {})\', 26912)'.format(243900.352024, 4432069.05679)

        self.assertEqual(actual, expected)

    def test_shape_is_none_with_invalid_lat_lon(self):
        row = {
            'Shape': None,
            'Lon_X': None,
            'Lat_Y': 40
        }
        actual = self.patient._update_shape(row)['Shape']

        self.assertIsNone(actual)
