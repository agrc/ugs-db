#!usr/bin/env python
# -*- coding: utf-8 -*-

'''
programs
----------------------------------
test the programs module
'''

import unittest
from dbseeder.programs import WqpProgram
from os.path import join, basename


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

    def test_get_samples_for_id_returns_correct_list(self):
        csv_file = None
