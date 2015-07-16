#!usr/bin/env python
# -*- coding: utf-8 -*-

'''
dbseeder
----------------------------------
test the dbseeder module
'''

import unittest
from dbseeder.dbseeder import Seeder, WqpProgram
from os.path import join, basename


class TestDbSeeder(unittest.TestCase):

    def setUp(self):
        self.patient = Seeder()


class TestParseSourceArgs(unittest.TestCase):

    def setUp(self):
        self.patient = Seeder()
        self.all_sources = ['WQP', 'SDWIS', 'DOGM', 'DWR', 'UGS']

    def test_gets_all_sources_when_given_none(self):
        self.assertEqual(self.patient._parse_source_args(None), self.all_sources)

    def test_gets_all_sources_when_given_empty_string(self):
        self.assertEqual(self.patient._parse_source_args(''), self.all_sources)

    def test_returns_none_when_given_bad_source(self):
        self.assertIsNone(self.patient._parse_source_args('not a source'))

    def test_gets_array_of_sources_when_given_csv(self):
        self.assertEqual(self.patient._parse_source_args('WQP, SDWIS'), ['WQP', 'SDWIS'])
        self.assertEqual(self.patient._parse_source_args(' WQP, SDWIS'), ['WQP', 'SDWIS'])
        self.assertEqual(self.patient._parse_source_args(' WQP , SDWIS '), ['WQP', 'SDWIS'])
        self.assertEqual(self.patient._parse_source_args('WQP, SDWIS, not a source'), ['WQP', 'SDWIS'])

    def test_gets_array_of_sources_when_given_one(self):
        self.assertEqual(self.patient._parse_source_args('WQP'), ['WQP'])
        self.assertEqual(self.patient._parse_source_args('WQP '), ['WQP'])
        self.assertEqual(self.patient._parse_source_args(' WQP'), ['WQP'])
        self.assertEqual(self.patient._parse_source_args(' WQP '), ['WQP'])


class TestWqpProgram(unittest.TestCase):
    def setUp(self):
        self.test_get_files_folder = join('tests', 'data', 'WQP', 'get_files')
        self.patient = WqpProgram(self.test_get_files_folder)

    def test_get_files_finds_files(self):
        expected_results = [
            'sample_chemistry.csv',
            'sample_chemistry2.csv',
            'test.csv'
        ]

        expected_stations = [
            'sample_stations.csv'
        ]

        results_folder = map(basename, self.patient._get_files(self.patient.results_folder))
        stations_folder = map(basename, self.patient._get_files(self.patient.stations_folder))

        self.assertItemsEqual(results_folder, expected_results)
        self.assertItemsEqual(stations_folder, expected_stations)

    def test_get_samples_for_id_returns_correnct_list(self):
        csv_file = None
