#!usr/bin/env python
# -*- coding: utf-8 -*-

'''
test_programs.py
----------------------------------
test the programs module
'''

import unittest
from dbseeder.programs import WqpProgram, DogmProgram
from dbseeder import sql, arcpy_mock
from collections import OrderedDict
from csv import reader as csvreader
from mock import Mock
from nose.tools import raises
from os.path import join, basename


class TestWqpProgram(unittest.TestCase):
    def setUp(self):
        self.test_get_files_folder = join('tests', 'data', 'WQP', 'get_files')
        self.patient = WqpProgram(db='bad db connection',
                                  update=False,
                                  sql_statements=sql.sql_statements,
                                  source=self.test_get_files_folder)

    def test_get_files_finds_files(self):
        expected_results = [
            'sample_chemistry.csv',
            'sample_chemistry2.csv'
        ]

        expected_stations = [
            'sample_stations.csv'
        ]

        results_folder = map(basename, self.patient._get_files(self.patient.results_folder))
        stations_folder = map(basename, self.patient._get_files(self.patient.stations_folder))

        self.assertItemsEqual(results_folder, expected_results)
        self.assertItemsEqual(stations_folder, expected_stations)

    @raises(Exception)
    def test_get_files_with_empty_location_throws(self):
        self.patient._get_files(None)

    @raises(Exception)
    def test_get_files_with_no_csv_found_throws(self):
        self.patient._get_files(join('tests', 'data', 'WQP', 'empty_folders', 'Stations'))

    @raises(Exception)
    def test_folder_without_required_folders_throws(self):
        self.patient = WqpProgram(db=None, file_location=join('tests', 'data', 'WQP', 'incorrect_structure'))

    @raises(Exception)
    def test_folder_without_required_child_folders_throws(self):
        self.patient = WqpProgram(db=None, file_location=join('tests', 'data', 'WQP', 'incorrect_child_structure'))

    def test_get_samples_for_id_returns_correct_list(self):
        self.patient.sample_id_field = 'id'
        config = OrderedDict([('a', 'eh'), ('b', 'bee'), ('c', 'sea')])
        sample_id_set = (1,)
        file_path = join('tests', 'data', 'WQP', 'get_sample_ids.csv')

        rows = self.patient._get_samples_for_id(sample_id_set, file_path, config=config)

        self.assertEqual(len(rows), 2)
        self.assertItemsEqual([
                              {'eh': 'a1', 'bee': 'b1', 'sea': 'c1', 'ActivityIdentifier': '1'},
                              {'eh': 'a2', 'bee': 'b2', 'sea': 'c2', 'ActivityIdentifier': '1'}
                              ], rows)

    def test_get_distict_samples(self):
        self.patient.sample_id_field = 'id'
        rows = self.patient._get_distinct_sample_ids_from(join('tests', 'data', 'WQP', 'distinct_sampleids.csv'))

        self.assertEqual(len(rows), 2)
        self.assertItemsEqual([('1',), ('2',)], rows)

    def test_get_wqx_duplicate_ids(self):
        ids = self.patient._get_wqx_duplicate_ids(join('tests', 'data', 'WQP', 'wqxids.csv'))

        self.assertItemsEqual(set([u'UTAHDWQ-4904410', u'UTAHDWQ-4904640', u'UTAHDWQ-4904610']), ids)

    def test_wqx_duplicates_for_update(self):
        rows = [{'StationId': 'UTAHDWQ_WQX-4904410'},
                {'StationId': 'UTAHDWQ_WQX-4904610'},
                {'StationId': 'UTAHDWQ-111'}]

        actual = self.patient._get_wqx_duplicate_ids(rows)

        self.assertEqual(set(['UTAHDWQ-4904610', 'UTAHDWQ-4904410']), actual)

    def test_insert_args(self):
        self.maxDiff = None
        insert_mock = Mock()
        cursor_mock = Mock()
        db = {
            'connection_string': ''
        }
        self.patient = WqpProgram(db=db,
                                  source=join('tests', 'data', 'WQP', 'insert'),
                                  update=False,
                                  insert_rows=insert_mock,
                                  update_row=sql.update_row,
                                  cursor_factory=cursor_mock)

        #: prevent sql error for missing table
        self.patient._add_sample_index = lambda x: None

        self.patient.seed()

        self.assertEqual(insert_mock.call_count, 2)

        station_call = insert_mock.call_args_list[0]
        result_call = insert_mock.call_args_list[1]

        station_row = station_call[0][0][0]

        self.assertEqual(station_row, [
            "'orgid'",
            "'orgname'",
            "'stationid'",
            "'stationname'",
            "'stationtype'",
            "'stationcomment'",
            "'huc8'",
            "-114.0",  #: Longitude
            "42.0",  #: latitude
            "0.0",  #: HorAcc
            "'hunit'",  #: HorAccUnit
            "'horcollmeth'",
            "'horref'",
            "1.0",  #: Elev
            "'elevunit'",
            "2.0",
            "'euni'",
            "'elevmeth'",
            "'elevref'",
            "3",  #: StateCode
            "4",  #: CountyCode
            "'aquifer'",
            "'fmtype'",
            "'aquifertype'",
            "Cast('2011-01-01' as date)",
            "5.0",  #: depth
            "'dunit'",
            "6.0",  #: HoleDepth
            "'hdunit'",
            'Null',  #: demELEVm
            "'WQP'",  #: DataSource
            'Null',  #: WIN
            "geometry::STGeomFromText('POINT (251535.079282 4654130.89121)', 26912)"
        ])

        result_rows = result_call[0][0][0]
        self.assertEqual(result_rows, [
            "Cast('2011-01-01' as date)",  #: analysis date
            "'analythmeth'",
            "'analythmethid'",
            'Null',  #: AutoQual
            'Null',  #: CAS_Reg
            'Null',  #: Chrg
            "'WQP'",  #: DataSource
            "'detectcondition'",
            'Null',  #: IdNum
            "'labcomments'",
            "'labname'",
            'Null',  #: Lat
            "'limittype'",
            'Null',  #: Long
            '0.0',  #: MDL
            "'mdlunit'",
            "'methoddescript'",
            "'origid'",
            "'orgname'",
            "'param'",
            'Null',  #: ParamGroup
            "'projectid'",
            "'qualcode'",
            "'resultcomment'",
            "'resultstatus'",
            '1.0',  #: ResultValue
            "'sampcomment'",
            '2.0',  #: SampDepth
            "'sampdepthref'",
            "'sampdepthu'",
            "'sampequip'",
            "'sampfrac'",
            "Cast('2011-01-02' as date)",  #: activity date
            "'12:00:00'",  #: activity Time
            "'sampleid'",
            "'sampmedia'",
            "'sampmeth'",
            "'sampmethname'",
            "'samptype'",
            "'stationid'",
            "'unit'",
            "'usgspcode'"
        ])

    @raises(Exception)
    def test_seed_with_no_file_location(self):
        self.patient = WqpProgram('bad db connection')
        self.patient.seed()

    def test_format_url(self):
        template = 'type={}&lastupdated={}&today={}'
        self.assertEqual(self.patient._format_url(template, 'Result', '01/01/1999', today='01/01/2000'),
                         'type=Result&lastupdated=01-01-1999&today=01-01-2000')

    def test_group_sample_ids(self):
        sample_response = join('tests', 'data', 'WQP', 'webservice.csv.as.txt')
        with open(sample_response, 'rb') as f:
            wqp_service_csv = csvreader(f)

            unique_sample_ids = self.patient._group_rows_by_id(wqp_service_csv)

            self.assertEqual(len(unique_sample_ids.keys()), 1)
            self.assertEqual(len(unique_sample_ids['nwisaz.01.00000154']), 3)
            self.assertTrue('nwisaz.01.00000154' in unique_sample_ids)

    def test_find_new_station_ids(self):
        mock = Mock()
        mock.side_effect = lambda x: [[i] for i in x]

        self.patient._get_unique_station_ids = mock

        rows = {
            'sampleid1': [{
                'StationId': 1
            }, {
                'StationId': 2
            }, {
                'StationId': 1
            }]
        }

        expected = self.patient._find_new_station_ids(rows)

        self.assertEqual(expected, [1, 2])

    def test_find_new_station_ids_with_empty(self):
        mock = Mock()
        mock.side_effect = lambda x: [[i] for i in x]

        self.patient._get_unique_station_ids = mock

        rows = []

        expected = self.patient._find_new_station_ids(rows)

        self.assertEqual(expected, [])

    def test_extract_stations_by_id(self):
        station_config = {
            'a': 'StationId',
            'b': 'someValue'
        }

        cursor = (x for x in [[1, 'insert1'], [2, 'missed'], [3, 'insert3']])

        self.patient.station_config = station_config

        actual = self.patient._extract_stations_by_id(cursor, [1, 3], ['a', 'b'])

        self.assertEqual(actual, [{'StationId': 1, 'someValue': 'insert1'}, {'StationId': 3, 'someValue': 'insert3'}])

    def test_new_stations_sql_format(self):
        station_ids = ['(1)', '(2)', '(3)']
        statement = self.patient.new_stations_query.format(','.join(station_ids))

        self.assertEqual(('SELECT * FROM (VALUES(1),(2),(3)) AS t(StationId) WHERE NOT EXISTS(' +
                          'SELECT 1 FROM [UGSWaterChemistry].[ugswaterchemistry].[Stations] WHERE [StationId] = t.StationId)'),
                         statement)

    def test_etl_column_names_with_dict(self):
        row = {'StationId': 1}

        actual = self.patient._etl_column_names(row, None)

        self.assertEqual(row, actual)

    def test_remove_existing_wqx_station_ids_that_exist_in_database(self):
        new_station_ids = ['123_WQX-ABC', '1234']

        mock = Mock()
        mock.side_effect = lambda x: [[i] for i in ['123-ABC']]

        self.patient._get_unique_station_ids = mock

        ids = self.patient._remove_existing_wqx_station_ids(new_station_ids)

        self.assertEqual(ids, ['123_WQX-ABC'])

    def test_remove_existing_wqx_station_ids_returns_ids_when_no_wqx(self):
        new_station_ids = ['123', '1234']

        ids = self.patient._remove_existing_wqx_station_ids(new_station_ids)

        self.assertEqual(ids, ['123', '1234'])

    def test_remove_existing_wqx_station_ids_where_there_are_duplicate_input(self):
        new_station_ids = ['123_WQX-ABC', '123-ABC']

        mock = Mock()
        mock.side_effect = lambda x: [[i] for i in ['123-ABC', '123-ABC']]

        self.patient._get_unique_station_ids = mock

        ids = self.patient._remove_existing_wqx_station_ids(new_station_ids)

        self.assertEqual(ids, ['123_WQX-ABC', '123-ABC'])

    def test_get_unique_sample_ids(self):
        results = {'sampleid1': [], 'sampleid2': [], 'existingsampleid': []}

        mock = Mock()
        mock.side_effect = lambda x: [[i] for i in ['sampleid1', 'sampleid2']]

        self.patient._get_unique_sample_ids = mock

        new_results = self.patient._remove_existing_results(results)

        self.assertItemsEqual(new_results.keys(), ['sampleid1', 'sampleid2'])

    def test_get_samples_for_id_returns_correct_list_when_quoted(self):
        self.patient.sample_id_field = 'id'
        sample_id_set = ('nwisnv.01.00901373',)
        file_path = join('tests', 'data', 'WQP', 'quotes_in_csv.csv')

        rows = self.patient._get_samples_for_id(sample_id_set, file_path, config=self.patient.result_config)

        self.assertEqual(len(rows), 1)

        result = rows[0]

        self.assertEqual(result['OrgName'], 'USGS Nevada Water Science Center')
        self.assertEqual(result['StationId'], 'USGS-362735114154501')
        self.assertEqual(result['SampComment'], '"A-2400004 Filter lot Q625 L-2400004 Received August 27, 2009..  verified FA btl not received,paa,9/2/09"')
        self.assertEqual(result['SampMeth'], 'USGS')
        self.assertEqual(result['SampMethName'], 'USGS')
        self.assertEqual(result['SampEquip'], 'Unknown')
        self.assertEqual(result['Param'], 'Specific conductance')


class TestDogmProgram(unittest.TestCase):
    def setUp(self):
        self.test_get_files_folder = join('tests', 'data')
        self.patient = DogmProgram(db='bad db connection',
                                   update=False,
                                   source=self.test_get_files_folder,
                                   arcpy=arcpy_mock)

    def test_get_field_intersection(self):
        ugs_schema = OrderedDict([
            ('AnalysisDate', {
                'alias': 'Analysis Start Date',
                'type': 'Date'
            }),
            ('AnalytMeth', {
                'alias': 'Analytical Method Name',
                'type': 'String',
                'length': 150
            }),
            ('AnalytMethId', {
                'alias': 'Analytical Method Id',
                'type': 'String',
                'length': 50
            }),
            ('AutoQual', {
                'alias': 'Auto Quality Check',
                'type': 'String'
            })
        ])

        table_fields = [Field('AnalysisDate'), Field('AnalytMeth'), Field('SAMPLE_TYPE'), Field('AutoQual')]

        actual = self.patient._get_field_instersection(ugs_schema, table_fields)

        self.assertItemsEqual(actual, ['AnalysisDate', 'AnalytMeth', 'AutoQual'])


class Field(object):
    '''Mock of an arcpy.Field class'''
    def __init__(self, name):
        self.name = name
