#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
factory.py
----------------------------------
helper methods for creating a program
'''

from . import programs


def get(source):
    if source == 'WQP':
        return programs.WqpProgram
    elif source == 'SDWIS':
        return programs.SdwisProgram
    elif source == 'DOGM':
        return programs.DogmProgram
    elif source == 'UDWR':
        return programs.UdwrProgram
    elif source == 'UGS':
        return programs.UgsProgram
