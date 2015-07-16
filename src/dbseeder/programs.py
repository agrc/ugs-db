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


#: this should probably get organized into a module
class WqpProgram(object):
    '''class for handling wqp csv files'''

    datasouce = 'WQP'
    sample_id_field = 'ActivityIdentifier'
    distinct_sample_id_query = 'select distinct({}) from {}'
    sample_id_query = 'select * from {} where {} = \'{}\''

    config = {
        'destination field name': 'source field name'
    }

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
            print(csv_file)

        for csv_file in self._get_files(self.results_folder):
            unique_sample_ids = self._get_distinct_sample_ids_from(csv_file)

            for sample_id in unique_sample_ids:
                samples = self._get_samples_for_id(sample_id, csv_file)

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
        if len(sample_ids) > 0:
            #: remove header cell
            sample_ids.pop(0)

        return sample_ids

    def _get_file_name_without_extension(self, file_path):
        return splitext(basename(file_path))[0]
