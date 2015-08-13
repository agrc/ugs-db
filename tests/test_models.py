#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_programs
----------------------------------
Tests for `models` module.
"""

import dbseeder.models as models
import unittest


class TestConcentrationModel(unittest.TestCase):

    patient = None

    def setUp(self):
        self.patient = models.Concentration()

    def test_update_noop_if_detect_cond(self):
        self.patient.set('ca', 1, 'Bad Test')

        self.assertIsNone(self.patient.chemical_amount['ca'])

    def test_multiple_chemicals_become_array(self):
        self.patient.set('ca', 0, None)
        self.patient.set('ca', 1, None)
        self.patient.set('ca', 2, None)

        expected = [0, 1, 2]
        self.assertItemsEqual(
            expected, self.patient.chemical_amount['ca'])

    def test_multiple_chemicals_become_array_with_None(self):
        self.patient.set('ca', 0, None)
        self.patient.set('ca', 1, None)
        self.patient.set('ca', None, None)

        expected = [0, 1]
        self.assertItemsEqual(
            expected, self.patient.chemical_amount['ca'])

    def test_arrays_become_numbers_after_validity_check(self):
        self.patient.set('ca', 0, None)
        self.patient.set('ca', 1, None)
        self.patient.set('ca', 2, None)

        expected = 1

        self.assertEqual(expected, self.patient.calcium)

    def test_false_if_not_all_majors_present(self):
        majors = ['ca',
                  'mg',
                  'na',
                  'cl',
                  'hco3',
                  'so4']

        # put everything in except for 1
        for i in xrange(0, len(majors) - 1):
            self.patient.set(majors[i], 1)
            self.assertFalse(self.patient.has_major_params)

        self.patient.set('no2', 1)
        self.assertFalse(self.patient.has_major_params)

        self.patient.set('co3', 1)
        self.assertFalse(self.patient.has_major_params)

    def test_true_if_majors_present(self):
        majors = ['ca',
                  'mg',
                  'na',
                  'cl',
                  'hco3',
                  'so4']

        # put everything in
        for i in xrange(0, len(majors)):
            self.patient.set(majors[i], 1)

        self.assertTrue(self.patient.has_major_params)

    def test_true_if_majors_plus_k_present(self):
        majors = ['ca',
                  'mg',
                  'na+k',
                  'cl',
                  'hco3',
                  'so4']

        # put everything in
        for i in xrange(0, len(majors)):
            self.patient.set(majors[i], 1)

        self.assertTrue(self.patient.has_major_params)

    def test_gets_correct_potassium_with_sodium_plus_potasium(self):
        self.patient.set('na', 2, None)
        self.patient.set('na+k', 5, None)

        self.assertEqual(3, self.patient.potassium)
        self.assertEqual(2, self.patient.sodium)
        self.assertEqual(0, self.patient.sodium_plus_potassium)

    def test_gets_correct_sodium_with_sodium_plus_potasium(self):
        self.patient.set('k', 2, None)
        self.patient.set('na+k', 5, None)

        self.assertEqual(3, self.patient.sodium)
        self.assertEqual(2, self.patient.potassium)
        self.assertEqual(0, self.patient.sodium_plus_potassium)

    def test_gets_correct_sodium_and_potasium_when_all_three(self):
        self.patient.set('k', 1, None)
        self.patient.set('na', 2, None)
        self.patient.set('na+k', 3, None)

        self.assertEqual(1, self.patient.potassium)
        self.assertEqual(2, self.patient.sodium)
        self.assertEqual(0, self.patient.sodium_plus_potassium)

    def test_concentrations_can_be_appended(self):
        self.patient.set('ca', 1, None)

        another = models.Concentration()
        another.set('mg', 2, None)

        self.patient.append(another)

        self.assertEqual(self.patient.calcium, 1)
        self.assertEqual(self.patient.magnesium, 2)

    def tearDown(self):
        self.patient = None
        del self.patient
