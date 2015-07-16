#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Programs.py
----------------------------------
the different source programs
'''

from querycsv import query_csv
from os.path import join, isdir, basename, splitext
from glob import glob
from collections import OrderedDict


#: this should probably get organized into a module
class WqpProgram(object):
    '''class for handling wqp csv files'''

    datasouce = 'WQP'
    sample_id_field = 'ActivityIdentifier'
    distinct_sample_id_query = 'select distinct({}) from {}'
    sample_id_query = 'select * from {} where {} = \'{}\''

    station_config = OrderedDict([
        ('OrgId', 'OrganizationIdentifier'),
        ('OrgName', 'OrganizationFormalName'),
        ('StationId', 'MonitoringLocationIdentifier'),
        ('StationName', 'MonitoringLocationName'),
        ('StationType', 'MonitoringLocationTypeName'),
        ('StationComment', 'MonitoringLocationDescriptionText'),
        ('HUC8', 'HUCEightDigitCode'),
        ('Lon_X', 'LongitudeMeasure'),
        ('Lat_Y', 'LatitudeMeasure'),
        ('HorAcc', 'HorizontalAccuracyMeasure/MeasureValue'),
        ('HorAccUnit', 'HorizontalAccuracyMeasure/MeasureUnitCode'),
        ('HorCollMeth', 'HorizontalCollectionMethodName'),
        ('HorRef', 'HorizontalCoordinateReferenceSystemDatumName'),
        ('Elev', 'VerticalMeasure/MeasureValue'),
        ('ElevUnit', 'VerticalMeasure/MeasureUnitCode'),
        ('ElevAcc', 'VerticalAccuracyMeasure/MeasureValue'),
        ('ElevAccUnit', 'VerticalAccuracyMeasure/MeasureUnitCode'),
        ('ElevMeth', 'VerticalCollectionMethodName'),
        ('ElevRef', 'VerticalCoordinateReferenceSystemDatumName'),
        ('StateCode', 'StateCode'),
        ('CountyCode', 'CountyCode'),
        ('Aquifer', 'AquiferName'),
        ('FmType', 'FormationTypeText'),
        ('AquiferType', 'AquiferTypeName'),
        ('ConstDate', 'ConstructionDateText'),
        ('Depth', 'WellDepthMeasure/MeasureValue'),
        ('DepthUnit', 'WellDepthMeasure/MeasureUnitCode'),
        ('HoleDepth', 'WellHoleDepthMeasure/MeasureValue'),
        ('HoleDUnit', 'WellHoleDepthMeasure/MeasureUnitCode'),
        ('demELEVm', None),
        ('DataSource', None),
        ('WIN', None)
    ])

    result_config = OrderedDict([
        ('AnalysisDate', 'AnalysisStartDate'),
        ('AnalytMeth', 'ResultAnalyticalMethod/MethodName'),
        ('AnalytMethId', 'ResultAnalyticalMethod/MethodIdentifier'),
        ('AutoQual', None),
        ('CAS_Reg', None),
        ('Chrg', None),
        ('DataSource', None),
        ('DetectCond', 'ResultDetectionConditionText'),
        ('IdNum', None),
        ('LabComments', 'ResultLaboratoryCommentText'),
        ('LabName', 'LaboratoryName'),
        ('Lat_Y', None),
        ('LimitType', 'DetectionQuantitationLimitTypeName'),
        ('Lon_X', None),
        ('MDL', 'DetectionQuantitationLimitMeasure/MeasureValue'),
        ('MDLUnit', 'DetectionQuantitationLimitMeasure/MeasureUnitCode'),
        ('MethodDescript', 'MethodDescriptionText'),
        ('OrgId', 'OrganizationIdentifier'),
        ('OrgName', 'OrganizationFormalName'),
        ('Param', 'CharacteristicName'),
        ('ParamGroup', None),
        ('ProjectId', 'ProjectIdentifier'),
        ('QualCode', 'MeasureQualifierCode'),
        ('ResultComment', 'ResultCommentText'),
        ('ResultStatus', 'ResultStatusIdentifier'),
        ('ResultValue', 'ResultMeasureValue'),
        ('SampComment', 'ActivityCommentText'),
        ('SampDepth', 'ActivityDepthHeightMeasure/MeasureValue'),
        ('SampDepthRef', 'ActivityDepthAltitudeReferencePointText'),
        ('SampDepthU', 'ActivityDepthHeightMeasure/MeasureUnitCode'),
        ('SampEquip', 'SampleCollectionEquipmentName'),
        ('SampFrac', 'ResultSampleFractionText'),
        ('SampleDate', 'ActivityStartDate'),
        ('SampleTime', 'ActivityStartTime/Time'),
        ('SampleId', 'ActivityIdentifier'),
        ('SampMedia', 'ActivityMediaSubdivisionName'),
        ('SampMeth', 'SampleCollectionMethod/MethodIdentifier'),
        ('SampMethName', 'SampleCollectionMethod/MethodName'),
        ('SampType', 'ActivityTypeCode'),
        ('StationId', 'MonitoringLocationIdentifier'),
        ('Unit', 'ResultMeasure/MeasureUnitCode'),
        ('USGSPCode', 'USGSPCode')
    ])

    def __init__(self, file_location):
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

    def seed(self):
        #: loop over each csv file
        #: load all stations first so you can validate results station id
        #: csv query for distinct sampleid's on results
        #: loop over query results and search for all sampleid's
        #: for each group of sampleid's
        #: etl the resulting rows
        #: normalize chemical units
        #: create charge balance
        #: return rows
        for csv_file in self._get_files(self.stations_folder):
            #: create csv reader
            #: cast to correct type
            #: normalize station id
            #: insert rows
            print(csv_file)

        #: this could be more functional
        for csv_file in self._get_files(self.results_folder):
            unique_sample_ids = self._get_distinct_sample_ids_from(csv_file)

            for sample_id in unique_sample_ids:
                samples = self._get_samples_for_id(sample_id, csv_file)
                #: etl samples
                #: cast to corrent type
                #: normalize chemical names amounts and units
                #: calculate charge balance
                #: insert rows

    def _get_files(self, location):
        if not location:
            raise Exception('Pass in a location containing csv files to import.')

        files = glob(join(location, '*.csv'))

        if len(files) < 1:
            raise Exception(location, 'No csv files found.')

        return files

    def _get_distinct_sample_ids_from(self, file_path):
        file_name = self._get_file_name_without_extension(file_path)

        unique_sample_ids = query_csv(self.distinct_sample_id_query.format(self.sample_id_field, file_name), [file_path])
        if len(unique_sample_ids) > 0:
            #: remove header cell
            unique_sample_ids.pop(0)

        return unique_sample_ids

    def _get_samples_for_id(self, sample_id_set, file_path):
        file_name = self._get_file_name_without_extension(file_path)
        sample_ids = query_csv(self.sample_id_query.format(file_name, self.sample_id_field, sample_id_set[0]), [file_path])

        return self._etl_column_names(sample_ids)

    def _etl_column_names(self, sample_ids):
        if len(sample_ids) > 0:
            #: remove header cell
            header = sample_ids.pop(0)

            def return_value_if_not_in_config(key):
                if key in self.config:
                    return self.config[key]

                return key

            header = map(lambda x: return_value_if_not_in_config(x), header)

        return map(lambda x: dict(zip(header, x)), sample_ids)

    def _get_file_name_without_extension(self, file_path):
        return splitext(basename(file_path))[0]
