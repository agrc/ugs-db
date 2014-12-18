#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
modelextensions
----------------------------------

Extensions to the program models. They add additional
functionality
"""
import re
from models import Concentration


class Normalizable(object):

    def __init__(self, normalizer):
        super(Normalizable, self).__init__()

        self.station_id_re = re.compile(re.escape('_WQX'), re.IGNORECASE)
        self.normalize_fields = {
            'chemical': (None, None),
            'unit': (None, None),
            'amount': (None, None),
            'paramgroup': (None, None),
            'stationid': (None, None)
        }

        self.normalizer = normalizer

    def normalize(self, row):
        #: normalize the row
        amount, unit, chemical = self.normalizer.normalize_unit(
            self.normalize_fields['chemical'][0],
            self.normalize_fields['unit'][0],
            self.normalize_fields['amount'][0])

        index = self.normalize_fields['paramgroup'][1]

        if not self.normalize_fields['paramgroup'][0]:
            paramgroup = self.normalizer.calculate_paramgroup(chemical)

            if index > -1:
                row[index] = paramgroup

        index = self.normalize_fields['stationid'][1]

        if (self.station_id_re.search(
                str(self.normalize_fields['stationid'][0]))):
            if index > -1:
                row[index] = self.station_id_re.sub(
                    '', self.normalize_fields['stationid'][0])

        index = self.normalize_fields['amount'][1]
        if index > -1:
            row[index] = amount

        index = self.normalize_fields['unit'][1]
        if index > -1:
            row[index] = unit

        index = self.normalize_fields['chemical'][1]
        if index > -1:
            row[index] = chemical

        return row

    def update_normalize(self, field_name, value, index):
        #: get the unit, resultvalue and param name so we
        #:    can use it later
        #: grab the value and the index for inserting the
        #:    updated values
        field_name = field_name.lower()

        if field_name == 'unit':
            if value:
                value = value.lower()

            self.normalize_fields['unit'] = (value, index)
        elif field_name == 'resultvalue':
            self.normalize_fields['amount'] = (value, index)
        elif field_name == 'param':
            self.normalize_fields['chemical'] = (value, index)
        elif field_name == 'paramgroup':
            self.normalize_fields['paramgroup'] = (value, index)
        elif field_name == 'stationid':
            #: do this to strip the unwanted text
            if value:
                value = self.station_id_re.sub('', value)

            self.normalize_fields['stationid'] = (value, index)


class Balanceable(object):

    """holds the values of the fields required to to charge balances"""

    def __init__(self):
        super(Balanceable, self).__init__()

        #: model that holds charge information for the balancer
        self.concentration = Concentration()

        #: the index of the array where the fields are
        self.field_index = {
            'detectcond': None,
            'resultvalue': None,
            'param': None,
            'sampleid': None
        }

        #: a reference to the normalized chemical row values
        self.row = None

        #: the alias lookup for balance param values
        self.param_alias = {'balance': 'Charge Balance',
                            'anion': 'Anions Total',
                            'cation': 'Cation Total'}

        #: the fields to insert into
        self.balance_fields = ['SampleId', 'Param', 'Unit']

    @property
    def chemical(self):
        if self.row is None:
            return None

        index = self.field_index['param']
        if index is None:
            return None

        return self.row[index]

    @property
    def amount(self):
        if self.row is None:
            return None

        index = self.field_index['resultvalue']
        if index is None:
            return None

        return self.row[index]

    @property
    def detect_cond(self):
        if self.row is None:
            return None

        index = self.field_index['detectcond']
        if index is None:
            return None

        return self.row[index]

    @property
    def sample_id(self):
        if self.row is None:
            return None

        index = self.field_index['sampleid']
        if index is None:
            return None

        value = self.row[index]
        if value is None:
            return None

        if value.lower() == 'n/a':
            return None

        return value

    def set_row_index(self, field_name, index):
        """
        sets the index of the field name
        this is necessary so we can find the fields we are
        looking for later to get the concentrations
        """

        if (field_name is None or
                field_name.lower() not in self.field_index.keys()):
            return

        self.field_index[field_name.lower()] = index

    def balance(self, row):
        self.row = row

        self.concentration._set(self.chemical, self.amount, self.detect_cond)

    def create_rows_from_balance(self, sample_id, balance):
        rows = []

        for key in balance.keys():
            if key not in self.param_alias.keys():
                continue

            rows.append([sample_id, self.param_alias[key], balance[key]])

        return rows
