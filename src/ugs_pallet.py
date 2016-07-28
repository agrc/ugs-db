#!/usr/bin/env python
# * coding: utf8 *
'''
pallet.py

A module that contains the pallet for running this project via forklift
'''

from ugsdbseeder.ugsdbseeder import Seeder
from forklift.models import Pallet
from time import strftime


class UGSPallet(Pallet):
    def build(self, configuration):
        self.configuration = configuration

    def is_ready_to_ship(self):
        ready = strftime('%A') == 'Monday'
        if not ready:
            self.success = (True, 'This pallet only runs on Mondays.')

        return ready

    def ship(self):
        configs_lookup = {'Production': 'prod',
                          'Staging': 'stage',
                          'Dev': 'dev'}
        Seeder('forklift').update('WQP, SDWIS', configs_lookup[self.configuration], None, True)
