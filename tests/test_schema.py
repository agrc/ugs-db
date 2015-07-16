#!usr/bin/env python
# -*- coding: utf-8 -*-

'''
schema
----------------------------------
test the schema module
'''

import unittest
from dbseeder import schema


class TestSchema(unittest.TestCase):
    def test_able_to_lookup_values(self):
        self.assertEqual(schema.station['OrgId']['length'], 20)
