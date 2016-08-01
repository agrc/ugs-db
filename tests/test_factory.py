#!usr/bin/env python
# -*- coding: utf-8 -*-

'''
test_factory.py
----------------------------------
test the factory module
'''

import ugsdbseeder.factory
import ugsdbseeder.programs as programs
import unittest


class TestSqlFunctions(unittest.TestCase):
    def test_get_wqp(self):
        actual = ugsdbseeder.factory.get('WQP')

        self.assertIs(actual, programs.WqpProgram)

    def test_get_sdwis(self):
        actual = ugsdbseeder.factory.get('SDWIS')

        self.assertIs(actual, programs.SdwisProgram)

    def test_get_dogm(self):
        actual = ugsdbseeder.factory.get('DOGM')

        self.assertIs(actual, programs.DogmProgram)

    def test_get_udwr(self):
        actual = ugsdbseeder.factory.get('UDWR')

        self.assertIs(actual, programs.UdwrProgram)

    def test_get_ugs(self):
        actual = ugsdbseeder.factory.get('UGS')

        self.assertIs(actual, programs.UgsProgram)
