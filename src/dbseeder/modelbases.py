#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
modelbases
----------------------------------

Base classes use for inheritence. These handle the common
operations for ETL'ing the programs data
"""
import services
from collections import OrderedDict
from models import Field, Schema
from modelextensions import Normalizable, FieldCalcable


class Table(Normalizable, FieldCalcable):

    """
    The base class for all of the program models whose
    schema does not need to be translated.
    """

    def __init__(self, normalizer, calculated_fields=None):
        super(Table, self).__init__(normalizer)

        self.balanceable = False

        if calculated_fields:
            self.calculated_fields = calculated_fields

    @staticmethod
    def build_schema_map(schema):
        schema_index_items = OrderedDict()

        if isinstance(schema, basestring):
            if schema == 'Stations':
                schema = Schema().station
            elif schema == 'Results':
                schema = Schema().result

        for item in schema:
            schema_index_items.update({item['destination']: Field(item)})

        return OrderedDict(schema_index_items)

    def _etl_row(self, row, schema_map, model_type, calculated_fields=None):
        _row = []
        self.balanceable = True

        for i, field_name in enumerate(schema_map):

            #: set the datasource for the program and move on
            if field_name == 'DataSource':
                try:
                    _row.append(self.datasource)
                except AttributeError:
                    _row.append(None)  # must be a result

                continue

            #: not all of the programs have the same schema so append None and move on
            if field_name not in self.fields:
                _row.append(None)

                # dem elevation v not in row but needs key set
                if field_name in self.calculated_fields.keys():
                    #: store the value and the index to we can swap them out later
                    if not calculated_fields or not calculated_fields[field_name][0]:
                        #: if the value already is set for testing, ignore it
                        self.calculated_fields[field_name] = (None, i)

                continue

            try:
                value = row[self.fields.index(field_name)]
            except IndexError:
                value = None

            field = schema_map[field_name]
            value = services.Caster.cast(value, field.field_type)

            # store these values to do later processing
            if field.field_name in self.calculated_fields.keys():
                #: store the value and the index to we can swap them out later
                if not calculated_fields or not calculated_fields[field.field_name][0]:
                    #: if the value already is set for testing, ignore it
                    self.calculated_fields[field.field_name] = (value, i)

            self.update_normalize(field_name, value, i)

            if self.balanceable:
                try:
                    self.set_row_index(field_name, i)
                except:
                    #: not of type balanceable
                    self.balanceable = False

            _row.append(value)

        _row = self.normalize(_row)

        return _row


class WqpTable(Normalizable, FieldCalcable):

    """
    The base class for all of the wqp schema to handle the
    data translations
    """

    datasource = 'WQP'

    def __init__(self, normalizer, calculated_fields=None):
        """
            this base class takes a csv row
            it then pulls all of the values out via the schema map
            and creates a row object that is ordered correctly for
            inserting into the feature class
            normalizer: the service to normalize the goods
        """

        super(WqpTable, self).__init__(normalizer)

        self.balanceable = None

        if calculated_fields:
            self.calculated_fields = calculated_fields

    @staticmethod
    def build_schema_map(schema):
        schema_index_items = OrderedDict()

        if isinstance(schema, basestring):
            if schema == 'Stations':
                schema = Schema().station
            elif schema == 'Results':
                schema = Schema().result

        for item in schema:
            schema_index_items.update({item['index']: Field(item)})

        return OrderedDict(schema_index_items)

    def _etl_row(self, row, schema_map, model_type, calculated_fields=None):
        _row = []
        self.balanceable = True

        for key in schema_map.keys():
            #: the key index maps to the column index in the feature class
            field = schema_map[key]
            source_field_name = field.field_source
            destination_field_type = field.field_type

            #: set the datasource for the program and move on
            if field.field_name == 'DataSource':
                try:
                    _row.append(self.datasource)
                except AttributeError:
                    _row.append(None)  # must be a result

                continue

            #: not all of the programs have the same schema so append None and move on
            if source_field_name not in row:
                _row.append(None)

                # necessesary for param group calculation
                self.update_normalize(field.field_name, None, key)

                # dem elevation v not in row but needs key set
                if field.field_name in self.calculated_fields.keys():
                    #: store the value and the index to we can swap them out later
                    if not calculated_fields or not calculated_fields[field.field_name]:
                        #: if the value already is set for testing, ignore it
                        self.calculated_fields[field.field_name] = (None, key)

                continue

            #: this should be covered by the last check
            try:
                value = row[source_field_name].strip()
            except IndexError:
                value = None

            value = services.Caster.cast(value, destination_field_type)

            # store these values to do later processing
            if field.field_name in self.calculated_fields.keys():
                #: store the value and the index to we can swap them out later
                if not calculated_fields or not calculated_fields[field.field_name]:
                    #: if the value already is set for testing, ignore it
                    self.calculated_fields[field.field_name] = (value, key)

            self.update_normalize(field.field_name, value, key)

            if self.balanceable:
                try:
                    self.set_row_index(field.field_name, key)
                except:
                    #: not of type balanceable
                    self.balanceable = False

            _row.append(value)

        _row = self.normalize(_row)

        return _row
