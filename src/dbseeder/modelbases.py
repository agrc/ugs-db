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

    def _etl_row(self, row, schema_map, model_type):
        _row = []
        self.balanceable = True

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

            self.update_normalize(field_name, value, i)

            if self.balanceable:
                try:
                    self.set_row_index(field_name, i)
                except:
                    #: not of type balanceable
                    self.balanceable = False

            _row.append(value)

        _row = self.normalize(_row)

        if model_type == 'Station':
            has_utm = False
            try:
                utmx_index = self.fields.index('UTM_X')
                utmy_index = self.fields.index('UTM_Y')
                has_utm = True
            except ValueError:
                pass

            try:
                utmx_index = self.fields.index('X_UTM')
                utmy_index = self.fields.index('Y_UTM')
                has_utm = True
            except ValueError:
                pass

            if has_utm:
                x = row[utmx_index]
                y = row[utmy_index]
            else:
                try:
                    x_index = self.fields.index('Lon_X')
                    y_index = self.fields.index('Lat_Y')

                    x, y = services.Project().to_utm(
                        row[x_index], row[y_index])
                except Exception as detail:
                    print 'Handling projection error:', detail

            _row.append((x, y))

        return _row


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

    def _etl_row(self, row, schema_map, model_type):
        _row = []
        self.balanceable = True
        lat = lon = None

        for key in schema_map.keys():
            #: the key index maps to the column index in the feature class
            field = schema_map[key]
            source_field_name = field.field_source
            destination_field_type = field.field_type

            if field.field_name == 'DataSource':
                try:
                    _row.append(self.datasource)
                except AttributeError:
                    _row.append(None)  # must be a result

                continue

            # not all of the programs have the same schema
            if source_field_name not in row:
                _row.append(None)
                self.update_normalize(field.field_name, None, key)

                continue

            try:
                value = row[source_field_name].strip()
            except IndexError:
                value = None

            value = services.Caster.cast(value, destination_field_type)

            if field.field_name == 'Lon_X':
                lon = value
            elif field.field_name == 'Lat_Y':
                lat = value

            self.update_normalize(field.field_name, value, key)

            if self.balanceable:
                try:
                    self.set_row_index(field.field_name, key)
                except:
                    #: not of type balanceable
                    self.balanceable = False

            _row.append(value)

        _row = self.normalize(_row)

        if model_type == 'Station':
            try:
                x, y = services.Project().to_utm(lon, lat)

                if x and y:
                    _row.append((x, y))
            except Exception as detail:
                print 'Handling projection error:', detail

        return _row
