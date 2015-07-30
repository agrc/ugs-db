#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
models
----------------------------------
The basic models
'''


class Concentration(object):

    """
    the model holding the charge balance input values
    """

    def __init__(self):
        super(Concentration, self).__init__()

        #: the chemical map from normalized values to chemical representation
        self.chemical_map = {'calcium': 'ca',
                             'dissolved calcium': 'ca',
                             'dissolved magnesium': 'mg',
                             'dissolved potassium': 'k',
                             'dissolved sodium': 'na',
                             'magnesium': 'mg',
                             'potassium': 'k',
                             'sodium': 'na',
                             'sodium plus potassium': 'na+k',
                             'total calcium': 'ca',
                             'total magnesium': 'mg',
                             'total potassium': 'k',
                             'total sodium': 'na',
                             'bicarbonate': 'hco3',
                             'bicarbonate as hco3': 'hco3',
                             'carbonate': 'co3',
                             'carbonate (co3)': 'co3',
                             'carbonate as co3': 'co3',
                             'chloride': 'cl',
                             'sulfate': 'so4',
                             'nitrate': 'no3',
                             'dissolved nitrate: no3': 'no3',
                             'nitrite': 'no2',
                             'dissolved nitrite: no2': 'no2',
                             'sulfate as so4': 'so4',
                             'bicarbonate based on alkalinity': 'hco3',
                             'carbonate based on alkalinity': 'co3',
                             'nitrate and nitrite as no3': 'no3',
                             'sulphate': 'so4'}

        #: the tracked chemicals for a charge balance and their concentration
        self.chemical_amount = {'ca': None,
                                'mg': None,
                                'na': None,
                                'k': None,
                                'cl': None,
                                'hco3': None,
                                'co3': None,
                                'so4': None,
                                'no2': None,
                                'no3': None,
                                'na+k': None}

    @property
    def calcium(self):
        return self._get_summed_value('ca')

    @property
    def magnesium(self):
        return self._get_summed_value('mg')

    @property
    def chloride(self):
        return self._get_summed_value('cl')

    @property
    def bicarbonate(self):
        return self._get_summed_value('hco3')

    @property
    def sulfate(self):
        return self._get_summed_value('so4')

    @property
    def carbonate(self):
        return self._get_summed_value('co3')

    @property
    def nitrate(self):
        return self._get_summed_value('no3')

    @property
    def nitrite(self):
        return self._get_summed_value('no2')

    @property
    def sodium(self):
        k = self._get_summed_value('k')
        na = self._get_summed_value('na')
        na_k = self._get_summed_value('na+k')

        if na_k is not None and k is not None and na is None:
            return na_k - k

        return na

    @property
    def potassium(self):
        k = self._get_summed_value('k')
        na = self._get_summed_value('na')
        na_k = self._get_summed_value('na+k')

        if na_k is not None and na is not None and k is None:
            return na_k - na

        return k

    @property
    def sodium_plus_potassium(self):
        nak = self._get_summed_value('na+k')
        k = self._get_summed_value('k')
        na = self._get_summed_value('na')

        if nak is not None and na is not None or k is not None:
            return 0

        return nak

    @property
    def has_major_params(self):
        """
        determines if the concentration can be used in a charge balance
        """
        valid_chemicals = 5
        num_of_chemicals = 0

        if self.calcium is not None:
            num_of_chemicals += 1
        if self.magnesium is not None:
            num_of_chemicals += 1
        if self.chloride is not None:
            num_of_chemicals += 1
        if self.bicarbonate is not None:
            num_of_chemicals += 1
        if self.sulfate is not None:
            num_of_chemicals += 1

        valid = num_of_chemicals == valid_chemicals

        return valid and (
            self.sodium is not None or self.sodium_plus_potassium is not None)

    def append(self, concentration, detect_cond=None):
        """
        merges the values of two concentrations into one
        """
        if not concentration.chemical_amount:
            return

        new = concentration.chemical_amount

        for key in new.keys():
            self._set(key, new[key], detect_cond)

    def _set(self, chemical, amount, detect_cond=None):
        """
        sets the chemical value. Handles duplicate chemicals
        """
        # there was a problem with the sample disregard
        if detect_cond:
            return

        # return if amount or chemical is None
        if amount is None or chemical is None:
            return

        chemical = chemical.lower()

        # do we care about this chemical?
        if chemical in self.chemical_map.keys():
            chemical = self.chemical_map[chemical]

        if chemical not in self.chemical_amount.keys():
            return

        # there is more than one sample for this chemical
        if self.chemical_amount[chemical] is not None:
            try:
                self.chemical_amount[chemical].append(amount)
            except AttributeError:
                # turn into a list for summing
                self.chemical_amount[chemical] = [
                    self.chemical_amount[chemical], amount]

            return

        self.chemical_amount[chemical] = amount

    def _get_summed_value(self, key):
        """
        gets value from number or the sum of a list of numbers
        """
        value = self.chemical_amount[key]

        try:
            return sum(value) / float(len(value))
        except TypeError:
            # value is not an array
            pass

        return value
