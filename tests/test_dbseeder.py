#!usr/bin/env python
# -*- coding: utf-8 -*-

'''
dbseeder
----------------------------------
test the dbseeder module
'''

import unittest
from dbseeder.dbseeder import Seeder


class TestDbSeeder(unittest.TestCase):

    def setUp(self):
        self.patient = Seeder()


class TestParseSourceArgs(unittest.TestCase):

    def setUp(self):
        self.patient = Seeder()
        self.all_sources = ['WQP', 'SDWIS', 'DOGM', 'UDWR', 'UGS']

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

    def test_gets_array_of_sources_when_lowercase(self):
        self.assertEqual(self.patient._parse_source_args('wqp'), ['WQP'])
        self.assertEqual(self.patient._parse_source_args('wqp '), ['WQP'])
        self.assertEqual(self.patient._parse_source_args(' wqp'), ['WQP'])
        self.assertEqual(self.patient._parse_source_args(' wqp '), ['WQP'])
        self.assertEqual(self.patient._parse_source_args('wqp, sdwis'), ['WQP', 'SDWIS'])
        self.assertEqual(self.patient._parse_source_args(' wqp, sdwis'), ['WQP', 'SDWIS'])
        self.assertEqual(self.patient._parse_source_args(' wqp , sdwis '), ['WQP', 'SDWIS'])
        self.assertEqual(self.patient._parse_source_args('wqp, sdwis, NOT A SOURCE'), ['WQP', 'SDWIS'])
