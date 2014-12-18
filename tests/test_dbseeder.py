#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_dbseeder
----------------------------------

Tests for `dbseeder` module.
"""
import arcpy
import os
import unittest
from dbseeder.dbseeder import Seeder
from shutil import rmtree


class TestDbSeeder(unittest.TestCase):

    #: thing being tested
    patient = None
    location = None
    parent_folder = None
    gdb_name = 'seed.gdb'

    def setUp(self):
        self.parent_folder = os.path.join(os.getcwd(), 'dbseeder', 'tests')
        self.location = os.path.join(self.parent_folder, 'temp_tests')

        self.tearDown()

        if not os.path.exists(self.location):
            os.makedirs(self.location)

        self.patient = Seeder(self.location, self.gdb_name)
        self.patient.count = 41

    def test_sanity(self):
        self.assertIsNotNone(self.patient)

    def test_gdb_creation(self):
        self.patient._create_gdb()

        gdb = os.path.join(self.location, self.gdb_name)

        self.assertTrue(os.path.exists(gdb))

    def test_fc_creation(self):
        self.patient._create_gdb()
        self.patient._create_feature_classes(['Stations', 'Results'])

        arcpy.env.workspace = self.patient.location

        self.assertEqual(len(arcpy.ListFeatureClasses()), 1)
        self.assertEqual(len(arcpy.ListTables()), 1)

    def _test_update(self):
        # self.patient.chemistry_query_url = self.chemistry_url
        # self.patient.update()
        pass

    def _test_seed(self):
        folder = os.path.join(os.getcwd(), 'dbseeder', 'tests', 'data')
        self.patient.seed(folder, ['Results', 'Stations'])

        arcpy.env.workspace = self.patient.location
        self.assertEqual(
            arcpy.GetCount_management('Stations').getOutput(0), '700')

    def tearDown(self):
        self.patient = None
        del self.patient

        limit = 5000
        i = 0

        while os.path.exists(self.location) and i < limit:
            try:
                rmtree(self.location)
            except:
                i += 1


if __name__ == '__main__':
    unittest.main()
