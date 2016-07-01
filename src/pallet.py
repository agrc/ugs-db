#!/usr/bin/env python
# * coding: utf8 *
'''
pallet.py

A module that contains the pallet for running this project via forklift
'''

from dbseeder.dbseeder import Seeder
from forklift.models import Pallet


class UGSPallet(Pallet):
    def ship(self):
        Seeder('forklift').update('WQP, SDWIS', 'stage', None, True)
