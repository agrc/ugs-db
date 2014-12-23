#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
programs
----------------------------------

The different programs feeding into the UGS
service. They handle seeding, updating the different
data sources.
"""
import ConfigParser
import csv
import cx_Oracle
import glob
import models
import resultmodels as resultmodel
import stationmodels as stationmodel
import os
from services import WebQuery, Normalizer, ChargeBalancer


class Balanceable(object):

    """
    common balanceable things for programs
    """

    def __init__(self):
        super(Balanceable, self).__init__()

        #: the concentrations grouped with their sampleid
        self.samples = {}
        self.balancer = ChargeBalancer()

    def track_concentration(self, etl):
        etl.balance(etl.row)

        if etl.sample_id in self.samples.keys():
            self.samples[etl.sample_id].append(etl.concentration)

            return

        self.samples[etl.sample_id] = etl.concentration

    def write_balance_rows(self, etl, location, curser=None):
        for sample_id in self.samples.keys():
            concentration = self.samples[sample_id]

            if not concentration.has_major_params:
                continue

            balance, cation, anion = (
                self.balancer.calculate_charge_balance(concentration))

            balance = {'balance': balance,
                       'cation': cation,
                       'anion': anion}

            balance_rows = etl.create_rows_from_balance(sample_id, balance)

            for row in balance_rows:
                if curser:
                    try:
                        curser.insertRow(row)
                        continue
                    except Exception as e:
                        raise e

                self._insert_row(row, etl.balance_fields, location)


class Program(object):

    def __init__(self, location, InsertCursor):
        super(Program, self).__init__()

        self.location = location
        self.InsertCursor = InsertCursor
        self.normalizer = Normalizer()

    def _get_default_fields(self, schema_map):
        fields = []
        for item in schema_map:
            fields.append(item)

        return fields

    def _get_fields(self, schema_map):
        return [schema_map[item].field_name for item in schema_map]


class GdbProgram(Program):

    def __init__(self, location, InsertCursor):
        super(GdbProgram, self).__init__(location, InsertCursor)

    def _read_gdb(self, location, fields):
        #: location - the path to the table data
        #: fields - the fields form the data to pull
        try:
            with self.SearchCursor(location, fields) as cursor:
                for row in cursor:
                    yield row
        except RuntimeError as e:
            #: the fields in the feature class
            import arcpy
            actual = set([str(x.name) for x in arcpy.ListFields(location)])

            #: the fields you are trying to use
            input_fields = set(fields)

            missing = input_fields - actual
            print 'the fouled up columns are {}'.format(missing)

            raise e

    def _insert_row(self, row, fields, location):
        with self.InsertCursor(location, fields) as cursor:
            cursor.insertRow(row)


class Wqp(Program, Balanceable):

    csv_location = 'WQP'

    def _insert_rows(self, data, feature_class):
        location = os.path.join(self.location, feature_class)

        print 'inserting into {} WQP type {}'.format(location, feature_class)

        station_ids = {}

        if feature_class == 'Results':
            Type = resultmodel.WqpResult
        elif feature_class == 'Stations':
            Type = stationmodel.WqpStation

        schema_map = Type.build_schema_map(feature_class)
        fields = self._get_fields(schema_map)

        if feature_class == 'Stations':
            fields.append('SHAPE@XY')

        with self.InsertCursor(location, fields) as cursor:
            for row in data:
                etl = Type(row, self.normalizer, schema_map)
                insert_row = etl.row

                station_id = etl.normalize_fields['stationid'][0]

                if station_id:
                    if station_id in station_ids.keys():
                        #: station is already inserted skip it
                        continue

                    station_ids[station_id] = True

                try:
                    cursor.insertRow(insert_row)
                except Exception as e:
                    raise e

                if etl.balanceable and etl.sample_id is not None:
                    self.track_concentration(etl)

        if etl.balanceable:
            with self.InsertCursor(location, etl.balance_fields) as cursor:
                self.write_balance_rows(etl, location, cursor)

    def _csvs_on_disk(self, parent_folder, model_type):
        """
        searches the parent folder for all csv files and returns
        them as a list

        #: parent_folder - the parent folder to the data directory
            this can be a file as well
        #: model_types - [Stations, Results]
        """

        if os.path.isfile(parent_folder):
            # this is mainly for testing purposes
            # or the need to seed from a specific file
            yield parent_folder

        folder = os.path.join(parent_folder, model_type, '*.csv')
        for csv_file in glob.glob(folder):
            yield csv_file

    def _query(self, url):
        data = WebQuery().results(url)

        return data

    def _read_response(self, data):
        reader = csv.DictReader(data, delimiter=',')

        return reader

    def _build_field_length_structure(self, schema):
        """turns the schema doc into a structure that can count fields lengths
            dict[source column] = array[destination column, count]
        """
        results = {}

        for column in schema:
            if column['source'] is None and (
                    column['type'].lower() != 'text' or
                    column['type'].lower() != 'string'):
                continue

            results[column['source']] = [column['destination'], 0]

        return results

    def field_lengths(self, folder, program_type):
        schema = models.Schema()

        if program_type.lower() == 'stations':
            maps = self._build_field_length_structure(schema.station)
        elif program_type.lower() == 'results':
            maps = self._build_field_length_structure(schema.result)
        else:
            raise Exception('flag must be stations or results')

        for csv_file in self._csvs_on_disk(folder, program_type):
            print 'processing {}'.format(csv_file)
            with open(csv_file, 'r') as f:
                data = csv.DictReader(f)
                for row in data:
                    for key in maps.keys():
                        length = len(row[key])
                        if maps[key][1] < length:
                            maps[key][1] = length

        return maps

    def seed(self, folder, model_types):
        """
        seeds the geodata base with the WQP data

        #: folder - the parent folder to the data directory
        #: model_types - [Stations, Results]
        """

        location = os.path.join(folder, self.csv_location)

        for model_type in model_types:
            for csv_file in self._csvs_on_disk(location, model_type):
                with open(csv_file, 'r') as f:
                    print 'processing {}'.format(csv_file)
                    self._insert_rows(csv.DictReader(f), model_type)
                    print 'processing {}: done'.format(csv_file)

    def update(self, model_types):
        for model_type in model_types:
            response = self._query(model_type)
            csv = self._read_response(response)

            self._insert_rows(csv, model_type)


class Sdwis(Program, Balanceable):

    _result_query = """SELECT
        UTV80.TSASAR.ANALYSIS_START_DT AS "AnalysisDate",
        UTV80.TSALAB.LAB_ID_NUMBER AS "LabName",
        UTV80.TSASAR.DETECTN_LIMIT_NUM AS "MDL",
        UTV80.TSASAR.DETECTN_LIM_UOM_CD AS "MDLUnit",
        UTV80.TINWSYS.TINWSYS_IS_NUMBER AS "OrgId",
        UTV80.TINWSYS.NAME AS "OrgName",
        UTV80.TSAANLYT.NAME AS "Param",
        UTV80.TSASAR.CONCENTRATION_MSR AS "ResultValue",
        UTV80.TSASAMPL.COLLLECTION_END_DT AS "SampleDate",
        UTV80.TSASAMPL.COLLCTN_END_TIME AS "SampleTime",
        UTV80.TSASAMPL.LAB_ASGND_ID_NUM AS "SampleId",
        UTV80.TINWSF.TYPE_CODE AS "SampType",
        UTV80.TINWSF.TINWSF_IS_NUMBER AS "StationId",
        UTV80.TSASAR.UOM_CODE AS "Unit",
        UTV80.TINLOC.LATITUDE_MEASURE AS "Lat_Y",
        UTV80.TINLOC.LONGITUDE_MEASURE AS "Lon_X",
        UTV80.TSAANLYT.CAS_REGISTRY_NUM AS "CAS_Reg",
        UTV80.TSASAR.TSASAR_IS_NUMBER AS "IdNum"

        FROM UTV80.TINWSF
        JOIN UTV80.TINWSYS ON
        UTV80.TINWSF.TINWSYS_IS_NUMBER = UTV80.TINWSYS.TINWSYS_IS_NUMBER
        JOIN UTV80.TINLOC ON
        UTV80.TINWSF.TINWSF_IS_NUMBER = UTV80.TINLOC.TINWSF_IS_NUMBER
        JOIN UTV80.TSASMPPT ON
        UTV80.TINWSF.TINWSF_IS_NUMBER = UTV80.TSASMPPT.TINWSF0IS_NUMBER
        JOIN UTV80.TSASAMPL ON
        UTV80.TSASMPPT.TSASMPPT_IS_NUMBER = UTV80.TSASAMPL.TSASMPPT_IS_NUMBER
        JOIN UTV80.TSASAR ON
        UTV80.TSASAMPL.TSASAMPL_IS_NUMBER = UTV80.TSASAR.TSASAMPL_IS_NUMBER
        JOIN UTV80.TSAANLYT ON
        UTV80.TSASAR.TSAANLYT_IS_NUMBER = UTV80.TSAANLYT.TSAANLYT_IS_NUMBER
        JOIN UTV80.TSALAB ON
        UTV80.TSASAMPL.TSALAB_IS_NUMBER = UTV80.TSALAB.TSALAB_IS_NUMBER

        WHERE (UTV80.TINWSF.TYPE_CODE = 'SP' Or
                UTV80.TINWSF.TYPE_CODE = 'WL') AND
                UTV80.TSASAR.CONCENTRATION_MSR IS NOT NULL

        ORDER BY UTV80.TINWSF.ST_ASGN_IDENT_CD"""

    _station_query = """SELECT
        UTV80.TINWSYS.TINWSYS_IS_NUMBER AS "OrgId",
        UTV80.TINWSYS.NAME AS "OrgName",
        UTV80.TINWSF.TINWSF_IS_NUMBER AS "StationId",
        UTV80.TINWSF.NAME AS "StationName",
        UTV80.TINWSF.TYPE_CODE AS "StationType",
        UTV80.TINLOC.LATITUDE_MEASURE AS "Lat_Y",
        UTV80.TINLOC.LONGITUDE_MEASURE AS "Lon_X",
        UTV80.TINLOC.HORIZ_ACCURACY_MSR AS "HorAcc",
        UTV80.TINLOC.HZ_COLLECT_METH_CD AS "HorCollMeth",
        UTV80.TINLOC.HORIZ_REF_DATUM_CD AS "HorRef",
        UTV80.TINLOC.VERTICAL_MEASURE AS "Elev",
        UTV80.TINLOC.VERT_ACCURACY_MSR AS "ElevAcc",
        UTV80.TINLOC.VER_COL_METH_CD AS "ElevMeth",
        UTV80.TINLOC.VERT_REF_DATUM_CD AS "ElevRef",
        MAX(UTV80.TINWLCAS.BOTTOM_DEPTH_MSR) AS "Depth",
        UTV80.TINWLCAS.BOTTOM_DP_MSR_UOM AS "DepthUnit"

        FROM UTV80.TINWSF
        JOIN UTV80.TINWSYS ON
            UTV80.TINWSF.TINWSYS_IS_NUMBER = UTV80.TINWSYS.TINWSYS_IS_NUMBER
        JOIN UTV80.TINLOC ON
            UTV80.TINWSF.TINWSF_IS_NUMBER = UTV80.TINLOC.TINWSF_IS_NUMBER
        LEFT JOIN UTV80.TINWLCAS ON
            UTV80.TINWSF.TINWSF_IS_NUMBER = UTV80.TINWLCAS.TINWSF_IS_NUMBER

        WHERE (UTV80.TINWSF.TYPE_CODE = 'SP' OR
            UTV80.TINWSF.TYPE_CODE = 'WL') AND
            UTV80.TINLOC.LATITUDE_MEASURE != 0
        GROUP BY UTV80.TINWSF.TINWSF_IS_NUMBER,
                UTV80.TINWSF.NAME,
                UTV80.TINWSF.TYPE_CODE,
                UTV80.TINWSYS.TINWSYS_IS_NUMBER,
                UTV80.TINWSYS.NAME,
                UTV80.TINLOC.LATITUDE_MEASURE,
                UTV80.TINLOC.LONGITUDE_MEASURE,
                UTV80.TINLOC.SRC_MAP_SCALE_NUM,
                UTV80.TINLOC.HORIZ_ACCURACY_MSR,
                UTV80.TINLOC.HZ_COLLECT_METH_CD,
                UTV80.TINLOC.HORIZ_REF_DATUM_CD,
                UTV80.TINLOC.VERTICAL_MEASURE,
                UTV80.TINLOC.VERT_ACCURACY_MSR,
                UTV80.TINLOC.VER_COL_METH_CD,
                UTV80.TINLOC.VERT_REF_DATUM_CD,
                UTV80.TINWLCAS.BOTTOM_DEPTH_MSR,
                UTV80.TINWLCAS.BOTTOM_DP_MSR_UOM"""

    def __init__(self, location, InsertCursor):
        super(Sdwis, self).__init__(location, InsertCursor)

        #: testing variable to reduce query times
        self.count = None

        config = ConfigParser.RawConfigParser()

        file = os.path.join(
            os.path.abspath(os.path.dirname(__file__)), 'secrets.cfg')

        config.read(file)

        user = config.get('sdwis', 'username')
        password = config.get('sdwis', 'password')
        server = config.get('sdwis', 'server')
        instance = config.get('sdwis', 'instance')

        self._connection_string = '{}/{}@{}/{}'.format(
            user, password, server, instance)

    def _query(self, query):
        print 'querying SDWIS database'

        conn = cx_Oracle.connect(self._connection_string)
        cursor = conn.cursor()

        results = cursor.execute(query)

        if self.count is not None:
            some = results.fetchmany(self.count)

            cursor.close()
            conn.close()

            return some

        return results

    def _insert_rows(self, data, feature_class):
        location = os.path.join(self.location, feature_class)
        print 'inserting into {} SDWIS type {}'.format(location, feature_class)

        if feature_class == 'Results':
            Type = resultmodel.SdwisResult
        elif feature_class == 'Stations':
            Type = stationmodel.SdwisStation

        fields = self._get_fields(Type.build_schema_map(feature_class))

        if feature_class == 'Stations':
            fields.append('SHAPE@XY')

        with self.InsertCursor(location, fields) as curser:
            for row in data:
                etl = Type(row, self.normalizer)
                insert_row = etl.row

                curser.insertRow(insert_row)

                if etl.balanceable and etl.sample_id is not None:
                    self.track_concentration(etl)

        if etl.balanceable:
            with self.InsertCursor(location, etl.balance_fields) as cursor:
                self.write_balance_rows(etl, location, cursor)

    def seed(self, model_types):
        query_string = None

        for model_type in model_types:
            if model_type == 'Stations':
                query_string = self._station_query
            elif model_type == 'Results':
                query_string = self._result_query

            records = self._query(query_string)
            self._insert_rows(records, model_type)


class Dogm(GdbProgram, Balanceable):
    #: location to dogm gdb
    gdb_name = 'DOGM\DOGM_AGRC.gdb'
    #: results table name
    results = 'DOGM_RESULT'
    #: stations feature class name
    stations = 'DOGM_STATION'

    def __init__(self, location, SearchCursor, InsertCursor):
        super(Dogm, self).__init__(location, InsertCursor)
        self.SearchCursor = SearchCursor

    def seed(self, folder, model_types):
        """
        seeds the geodata base with the Dogm data

        #: folder - the parent folder to the data directory
        #: model_types - [Stations, Results]
        """

        for model_type in model_types:
            if model_type == 'Stations':
                table = os.path.join(folder, self.gdb_name, self.stations)
                Type = stationmodel.OgmStation
            elif model_type == 'Results':
                table = os.path.join(folder, self.gdb_name, self.results)
                Type = resultmodel.OgmResult

            location = os.path.join(self.location, model_type)

            print 'inserting into {} DOGM type {}'.format(location, model_type)

            fields_to_insert = None

            for record in self._read_gdb(table, Type.fields):
                etl = Type(record, self.normalizer)
                if not fields_to_insert:
                    fields_to_insert = self._get_default_fields(etl.schema_map)

                    if model_type == 'Stations':
                        fields_to_insert.append('SHAPE@XY')

                self._insert_row(etl.row, fields_to_insert, location)

                if etl.balanceable and etl.sample_id is not None:
                    self.track_concentration(etl)

            if etl.balanceable:
                self.write_balance_rows(etl, location)


class Udwr(GdbProgram, Balanceable):
    #: location to dogm gdb
    gdb_name = 'UDWR\UDWR_AGRC.gdb'
    #: results table name
    results = 'UDWR_RESULTS'
    #: stations feature class name
    stations = 'UDWR_STATION'

    def __init__(self, location, SearchCursor, InsertCursor):
        super(Udwr, self).__init__(location, InsertCursor)
        self.SearchCursor = SearchCursor

    def seed(self, folder, model_types):
        #: folder - the parent folder to the data directory
        #: model_types - [Stations, Results]

        for model_type in model_types:
            if model_type == 'Stations':
                table = os.path.join(folder, self.gdb_name, self.stations)
                Type = stationmodel.DwrStation
            elif model_type == 'Results':
                table = os.path.join(folder, self.gdb_name, self.results)
                Type = resultmodel.DwrResult

            location = os.path.join(self.location, model_type)

            print 'inserting into {} UDWR type {}'.format(location, model_type)

            fields_to_insert = None

            for record in self._read_gdb(table, Type.fields):
                etl = Type(record, self.normalizer)
                if not fields_to_insert:
                    fields_to_insert = self._get_default_fields(etl.schema_map)

                    if model_type == 'Stations':
                        fields_to_insert.append('SHAPE@XY')

                self._insert_row(etl.row, fields_to_insert, location)

                if etl.balanceable and etl.sample_id is not None:
                    self.track_concentration(etl)

            self.write_balance_rows(etl, location)


class Ugs(GdbProgram, Balanceable):
    #: location to dogm gdb
    gdb_name = 'UGS\UGS_AGRC.gdb'
    #: results table name
    results = 'RESULTS'
    #: stations feature class name
    stations = 'STATIONS'

    def __init__(self, location, SearchCursor, InsertCursor):
        super(Ugs, self).__init__(location, InsertCursor)
        self.SearchCursor = SearchCursor

    def seed(self, folder, model_types):
        #: folder - the parent folder to the data directory
        #: types - [Stations, Results]

        for model_type in model_types:
            if model_type == 'Stations':
                table = os.path.join(folder, self.gdb_name, self.stations)
                Type = stationmodel.UgsStation
            elif model_type == 'Results':
                table = os.path.join(folder, self.gdb_name, self.results)
                Type = resultmodel.UgsResult

            location = os.path.join(self.location, model_type)

            print 'inserting into {} UGS type {}'.format(location, model_type)

            fields_to_insert = None

            for record in self._read_gdb(table, Type.fields):
                etl = Type(record, self.normalizer)
                if not fields_to_insert:
                    fields_to_insert = self._get_default_fields(etl.schema_map)

                    if model_type == 'Stations':
                        fields_to_insert.append('SHAPE@XY')

                self._insert_row(etl.row, fields_to_insert, location)

                if etl.balanceable and etl.sample_id is not None:
                    self.track_concentration(etl)

            self.write_balance_rows(etl, location)
