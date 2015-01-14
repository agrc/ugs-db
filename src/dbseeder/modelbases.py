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
from modelextensions import Normalizable
from services import WebQuery


class Table(Normalizable):

    """
    The base class for all of the program models whose
    schema does not need to be translated.
    """

    def __init__(self, normalizer):
        super(Table, self).__init__(normalizer)

        self.balanceable = False

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

    def _etl_row(self, row, schema_map, model_type, updating=False):
        _row = []
        self.balanceable = True
        calculated_fields = {
            'demELEVm': (None, -1),
            'StateCode': (None, -1),
            'CountyCode': (None, -1),
            'Lat_Y': (None, -1),
            'Lon_X': (None, -1)
        }

        for i, field_name in enumerate(schema_map):

            if field_name == 'DataSource':
                try:
                    _row.append(self.datasource)
                except AttributeError:
                    _row.append(None)  # must be a result

                continue

            if field_name not in self.fields:
                _row.append(None)

                continue

            try:
                value = row[self.fields.index(field_name)]
            except IndexError:
                value = None

            field = schema_map[field_name]
            value = services.Caster.cast(value, field.field_type)

            # store these values to do later processing
            if field.field_name in calculated_fields.keys():
                #: store the value and the index to we can swap them out later
                calculated_fields[field.field_name] = (value, i)

            self.update_normalize(field_name, value, i)

            if self.balanceable:
                try:
                    self.set_row_index(field_name, i)
                except:
                    #: not of type balanceable
                    self.balanceable = False

            _row.append(value)

        _row = self.normalize(_row)
        _row = self.calculate_fields(_row, model_type, calculated_fields, updating)

        return _row

    def calculate_fields(self, row, model_type, field_info, updating=False):
        x = y = None
        query_service = WebQuery()

        if model_type == 'Stations':
            try:
                x, y = services.Project().to_utm(field_info['Lon_X'][0], field_info['Lat_Y'][0])

                if x and y:
                    row.append((x, y))
            except Exception as detail:
                print 'Handling projection error:', detail
                print field_info

                row.append((None, None))

        if not(x and y) or not updating:
            return row

        try:
            elevation = query_service.elevation(x, y)
            row[field_info['demELEVm'][1]] = elevation
        except LookupError as detail:
            #: point is likely not in Utah
            print 'Handling api query error:', detail

        try:
            state_code = query_service.state_code(x, y)
            row[field_info['StateCode'][1]] = state_code
        except LookupError as detail:
            #: point is likely not in Utah
            print 'Handling api query error:', detail

        try:
            county_code = query_service.county_code(x, y)
            row[field_info['CountyCode'][1]] = county_code
        except LookupError as detail:
            #: point is likely not in Utah
            print 'Handling api query error:', detail

        return row


class WqpTable(Normalizable):

    """
    The base class for all of the wqp schema to handle the
    data translations
    """

    datasource = 'WQP'

    def __init__(self, normalizer):
        """
            this base class takes a csv row
            it then pulls all of the values out via the schema map
            and creates a row object that is ordered correctly for
            inserting into the feature class
            normalizer: the service to normalize the goods
        """

        super(WqpTable, self).__init__(normalizer)
        self.balanceable = None

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

    def _etl_row(self, row, schema_map, model_type, updating=False):
        _row = []
        self.balanceable = True
        calculated_fields = {
            'demELEVm': (None, -1),
            'StateCode': (None, -1),
            'CountyCode': (None, -1),
            'Lat_Y': (None, -1),
            'Lon_X': (None, -1)
        }

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

                continue

            #: this should be covered by the last check
            try:
                value = row[source_field_name].strip()
            except IndexError:
                value = None

            value = services.Caster.cast(value, destination_field_type)

            # store these values to do later processing
            if field.field_name in calculated_fields.keys():
                #: store the value and the index to we can swap them out later
                calculated_fields[field.field_name] = (value, key)

            self.update_normalize(field.field_name, value, key)

            if self.balanceable:
                try:
                    self.set_row_index(field.field_name, key)
                except:
                    #: not of type balanceable
                    self.balanceable = False

            _row.append(value)

        _row = self.normalize(_row)
        _row = self.calculate_fields(_row, model_type, calculated_fields, updating)

        return _row

    def calculate_fields(self, row, model_type, field_info, updating=False):
        x = y = None
        query_service = WebQuery()

        if model_type == 'Stations':
            try:
                x, y = services.Project().to_utm(field_info['Lon_X'][0], field_info['Lat_Y'][0])

                if x and y:
                    row.append((x, y))
            except Exception as detail:
                print 'Handling projection error:', detail
                print field_info

                row.append((None, None))

        if not(x and y) or not updating:
            return row

        try:
            elevation = query_service.elevation(x, y)
            row[field_info['demELEVm'][1]] = elevation
        except LookupError as detail:
            #: point is likely not in Utah
            print 'Handling api query error:', detail

        try:
            state_code = query_service.state_code(x, y)
            row[field_info['StateCode'][1]] = state_code
        except LookupError as detail:
            #: point is likely not in Utah
            print 'Handling api query error:', detail

        try:
            county_code = query_service.county_code(x, y)
            row[field_info['CountyCode'][1]] = county_code
        except LookupError as detail:
            #: point is likely not in Utah
            print 'Handling api query error:', detail

        return row
