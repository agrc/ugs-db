#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
factory.py
----------------------------------
helper methods for creating a program
'''

import programs


def create(source):
    if source == 'WQP':
        return programs.WqpProgram
