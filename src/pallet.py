#!/usr/bin/env python
# * coding: utf8 *
'''
pallet.py

A module that contains the pallet for running this project via forklift
'''

from dbseeder.dbseeder import Seeder
from forklift.models import Pallet


class UGSPallet(Pallet):
    def build(self, configuration):
        self.configuration = configuration

    def ship(self):
        configs = {'Production': 'prod',
                   'Staging': 'stage',
                   'Dev': 'dev'}
        Seeder('forklift').update('WQP, SDWIS', configs[self.configuration], None, True)
