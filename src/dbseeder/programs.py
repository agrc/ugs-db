#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Programs.py
----------------------------------
the different source programs
'''

import csv
import pyodbc
import re
import schema
from collections import OrderedDict
from glob import glob
from os.path import join, isdir, basename, splitext
from querycsv import query_csv
from functools import partial
from services import Caster, Reproject, Normalizer, ChargeBalancer
from timeit import default_timer


class WqpProgram(object):
    '''class for handling wqp csv files'''

    datasouce = 'WQP'

    fields = {
        'sample_id': 'ActivityIdentifier',
        'monitoring_location_id': 'MonitoringLocationIdentifier'
    }
    sql = {
        'distinct_sample_id': 'select distinct({}) from {}',
        'sample_id': 'select * from {} where {} = \'{}\'',
        'wqxids': 'select {0} from {1} where {0} LIKE \'%_WQX%\'',
        'station_insert': ('insert into Stations (OrgId, OrgName, StationId, StationName, StationType, StationComment,'
                           + ' HUC8, Lon_X, Lat_Y, HorAcc, HorAccUnit, HorCollMeth, HorRef, Elev, ElevUnit, ElevAcc,'
                           + ' ElevAccUnit, ElevMeth, ElevRef, StateCode, CountyCode, Aquifer, FmType, AquiferType,'
                           + ' ConstDate, Depth, DepthUnit, HoleDepth, HoleDUnit, demELEVm, DataSource, WIN, Shape)'
                           + ' values ({})'),
        'result_insert': ('insert into Results (AnalysisDate, AnalytMeth, AnalytMethId, AutoQual, CAS_Reg, Chrg,'
                          + ' DataSource, DetectCond, IdNum, LabComments, LabName, Lat_Y, LimitType, Lon_X, MDL,'
                          + ' MDLUnit, MethodDescript, OrgId, OrgName, Param, ParamGroup, ProjectId, QualCode,'
                          + ' ResultComment, ResultStatus, ResultValue, SampComment, SampDepth, SampDepthRef,'
                          + ' SampDepthU, SampEquip, SampFrac, SampleDate, SampleTime, SampleId, SampMedia, SampMeth,'
                          + ' SampMethName, SampType, StationId, Unit, USGSPCode) values ({})')
    }
    wqx_re = re.compile('(_WQX)-')
    station = False
    result = True

    station_config = OrderedDict([
        ('OrganizationIdentifier', 'OrgId'),
        ('OrganizationFormalName', 'OrgName'),
        ('MonitoringLocationIdentifier', 'StationId'),
        ('MonitoringLocationName', 'StationName'),
        ('MonitoringLocationTypeName', 'StationType'),
        ('MonitoringLocationDescriptionText', 'StationComment'),
        ('HUCEightDigitCode', 'HUC8'),
        ('LongitudeMeasure', 'Lon_X'),
        ('LatitudeMeasure', 'Lat_Y'),
        ('HorizontalAccuracyMeasure/MeasureValue', 'HorAcc'),
        ('HorizontalAccuracyMeasure/MeasureUnitCode', 'HorAccUnit'),
        ('HorizontalCollectionMethodName', 'HorCollMeth'),
        ('HorizontalCoordinateReferenceSystemDatumName', 'HorRef'),
        ('VerticalMeasure/MeasureValue', 'Elev'),
        ('VerticalMeasure/MeasureUnitCode', 'ElevUnit'),
        ('VerticalAccuracyMeasure/MeasureValue', 'ElevAcc'),
        ('VerticalAccuracyMeasure/MeasureUnitCode', 'ElevAccUnit'),
        ('VerticalCollectionMethodName', 'ElevMeth'),
        ('VerticalCoordinateReferenceSystemDatumName', 'ElevRef'),
        ('StateCode', 'StateCode'),
        ('CountyCode', 'CountyCode'),
        ('AquiferName', 'Aquifer'),
        ('FormationTypeText', 'FmType'),
        ('AquiferTypeName', 'AquiferType'),
        ('ConstructionDateText', 'ConstDate'),
        ('WellDepthMeasure/MeasureValue', 'Depth'),
        ('WellDepthMeasure/MeasureUnitCode', 'DepthUnit'),
        ('WellHoleDepthMeasure/MeasureValue', 'HoleDepth'),
        ('WellHoleDepthMeasure/MeasureUnitCode', 'HoleDUnit'),
        ('demELEVm', '!demELEVm'),
        ('DataSource', '!DataSource'),
        ('WIN', '!WIN'),
        ('Shape', '!Shape')
    ])

    result_config = OrderedDict([
        ('AnalysisStartDate', 'AnalysisDate'),
        ('ResultAnalyticalMethod/MethodName', 'AnalytMeth'),
        ('ResultAnalyticalMethod/MethodIdentifier', 'AnalytMethId'),
        ('AutoQual', '!AutoQual'),
        ('CAS_Reg', '!CAS_Reg'),
        ('Chrg', '!Chrg'),
        ('DataSource', '!DataSource'),
        ('ResultDetectionConditionText', 'DetectCond'),
        ('IdNum', '!IdNum'),
        ('ResultLaboratoryCommentText', 'LabComments'),
        ('LaboratoryName', 'LabName'),
        ('Lat_Y', '!Lat_Y'),
        ('DetectionQuantitationLimitTypeName', 'LimitType'),
        ('Lon_X', '!Lon_X'),
        ('DetectionQuantitationLimitMeasure/MeasureValue', 'MDL'),
        ('DetectionQuantitationLimitMeasure/MeasureUnitCode', 'MDLUnit'),
        ('MethodDescriptionText', 'MethodDescript'),
        ('OrganizationIdentifier', 'OrgId'),
        ('OrganizationFormalName', 'OrgName'),
        ('CharacteristicName', 'Param'),
        ('ParamGroup', '*ParamGroup'),
        ('ProjectIdentifier', 'ProjectId'),
        ('MeasureQualifierCode', 'QualCode'),
        ('ResultCommentText', 'ResultComment'),
        ('ResultStatusIdentifier', 'ResultStatus'),
        ('ResultMeasureValue', 'ResultValue'),
        ('ActivityCommentText', 'SampComment'),
        ('ActivityDepthHeightMeasure/MeasureValue', 'SampDepth'),
        ('ActivityDepthAltitudeReferencePointText', 'SampDepthRef'),
        ('ActivityDepthHeightMeasure/MeasureUnitCode', 'SampDepthU'),
        ('SampleCollectionEquipmentName', 'SampEquip'),
        ('ResultSampleFractionText', 'SampFrac'),
        ('ActivityStartDate', 'SampleDate'),
        ('ActivityStartTime/Time', 'SampleTime'),
        ('ActivityIdentifier', 'SampleId'),
        ('ActivityMediaSubdivisionName', 'SampMedia'),
        ('SampleCollectionMethod/MethodIdentifier', 'SampMeth'),
        ('SampleCollectionMethod/MethodName', 'SampMethName'),
        ('ActivityTypeCode', 'SampType'),
        ('MonitoringLocationIdentifier', 'StationId'),
        ('ResultMeasure/MeasureUnitCode', 'Unit'),
        ('USGSPCode', 'USGSPCode')
    ])

    def __init__(self, file_location, db):
        super(WqpProgram, self).__init__()

        #: check that file_location exists wqp/results and wqp/stations
        parent_folder = join(file_location, self.datasouce)

        if not isdir(parent_folder):
            raise Exception('Pass in a location to the parent folder that contains WQP. {}'.format(parent_folder))

        results_folder = join(parent_folder, 'Results')
        stations_folder = join(parent_folder, 'Stations')

        if not isdir(results_folder or stations_folder):
            raise Exception('WQP folder is missing Results or Stations child folders containing csv files.')

        #: all is well set the folders for seeding later on
        self.results_folder = results_folder
        self.stations_folder = stations_folder

        self.db = db

    def seed(self):
        print('processing stations')
        for csv_file in self._get_files(self.stations_folder):
            #: create csv reader
            with open(csv_file, 'r') as f:
                print('processing {}'.format(basename(csv_file)))

                stations = []
                wxps = self._get_wxps_duplicate_ids(csv_file)
                reader = csv.reader(f)

                header = reader.next()
                for row in reader:
                    row = self._etl_column_names(row, self.station_config, header=header)

                    #: check for duplicate stationid and skip if found
                    station_id = row['StationId']
                    if not self.wqx_re.search(station_id) and station_id in wxps:
                        continue

                    #: cast (plus strip _WXP)
                    row = Caster.cast(row, schema.station)

                    #: reproject and update shape
                    row = self._update_shape(row)

                    #: reorder and filter out any fields not in the schema
                    #: TODO: this is not necessary if we dynamically build the sql statement
                    row = Normalizer.reorder_filter(row, schema.station)

                    row = Caster.cast_for_sql(row)

                    stations.append(row.values())

                #: insert stations
                self._insert_rows(stations, self.sql['station_insert'])

                print('processing {}: done'.format(basename(csv_file)))

        print('processing results')
        #: this could be more functional
        for csv_file in self._get_files(self.results_folder):
            print('processing {}'.format(basename(csv_file)))

            unique_sample_ids = self._get_distinct_sample_ids_from(csv_file)

            for sample_id in unique_sample_ids:
                samples = self._get_samples_for_id(sample_id, csv_file)

                samples = map(partial(Caster.cast, schema=schema.result), samples)

                samples = map(Normalizer.normalize_sample, samples)

                charge_balances = ChargeBalancer.get_charge_balance(samples)

                samples.extend(charge_balances)

                samples = map(partial(Normalizer.reorder_filter, schema=schema.result), samples)

                samples = map(Caster.cast_for_sql, samples)

                rows = map(lambda sample: sample.values(), samples)

                self._insert_rows(rows, self.sql['result_insert'])

            print('processing {}: done'.format(basename(csv_file)))

    def _get_files(self, location):
        '''Takes the file location and returns the csv's within it.'''

        if not location:
            raise Exception('Pass in a location containing csv files to import.')

        files = glob(join(location, '*.csv'))

        if len(files) < 1:
            raise Exception(location, 'No csv files found.')

        return files

    def _get_distinct_sample_ids_from(self, file_path):
        '''Given a file_path, this returns a set of unique sample ids.'''

        file_name = self._get_file_name_without_extension(file_path)

        unique_sample_ids = query_csv(self.sql['distinct_sample_id'].format(self.fields['sample_id'], file_name), [file_path])
        if len(unique_sample_ids) > 0:
            #: remove header cell
            unique_sample_ids.pop(0)

        return unique_sample_ids

    def _get_wxps_duplicate_ids(self, file_path):
        '''Given the file_path, return the list of stripped station ids that have the _WQX suffix'''

        file_name = self._get_file_name_without_extension(file_path)

        rows = query_csv(self.sql['wqxids'].format(self.fields['monitoring_location_id'], file_name), [file_path])
        if len(rows) > 0:
            rows.pop(0)

        return set([re.sub(self.wqx_re, '-', row[0]) for row in rows])

    def _get_samples_for_id(self, sample_id_set, file_path, config=None):
        '''Given a `(id,)` styled set of sample ids, this will return the sample
        rows for the given csv file_path. The format will be an array of dictionaries
        with the key being the destination field name and the value being the source csv value.

        This is only invoked for result files.
        '''

        file_name = self._get_file_name_without_extension(file_path)
        samples_for_id = query_csv(self.sql['sample_id'].format(file_name, self.fields['sample_id'], sample_id_set[0]), [file_path])

        return self._etl_column_names(samples_for_id, config or self.result_config)

    def _etl_column_names(self, rows, config, header=None):
        if len(rows) == 0:
            return None

        if not header:
            #: get header cell from rows and remove
            header = rows.pop(0)

        def return_value_if_not_in_config(key):
            if key in config:
                return config[key]

            return key

        header = map(lambda x: return_value_if_not_in_config(x), header)

        #: if we are passing a single item, not an array of sets, don't map over it.
        #: this is when we have a station and not a set of results
        if not isinstance(rows[0], tuple):
            return dict(zip(header, rows))

        return map(lambda x: dict(zip(header, x)), rows)

    def _get_file_name_without_extension(self, file_path):
        '''Given a filename with an extension, the file name is returned without the extension.'''

        return splitext(basename(file_path))[0]

    def _update_shape(self, row):
        template = 'geometry::STGeomFromText(\'POINT ({} {})\', 26912)'

        x = row['Lon_X']
        y = row['Lat_Y']

        if not (x and y):
            return row

        shape = Reproject.to_utm(x, y)

        row['Shape'] = template.format(shape[0], shape[1])

        return row

    def _insert_rows(self, rows, insert_statement):
        if not hasattr(self, 'cursor') or not self.cursor:
            c = pyodbc.connect(self.db['connection_string'])
            self.cursor = c.cursor()

        i = 1
        start = default_timer()
        for row in rows:
            statement = insert_statement.format(','.join(row))

            try:
                self.cursor.execute(statement)
                i += 1
            except Exception, e:
                del self.cursor
                raise e

            if i % 5000 == 0:
                self.cursor.commit()
                print('station total: {} in {}'.format(i, default_timer() - start))
                start = default_timer()

            self.cursor.commit()
