'''
Created on 9. 10. 2018

@author: esner
'''

import csv
import os
import io


PERIOD_HEADER = ['period_type', 'begin_period', 'end_period']

KEY_PERIOD_BEGIN = 'begin'
KEY_PERIOD_END = 'end'
KEY_PERIOD_TYPE = 'period type'

ENDPOINT_DEMOGRAPHY = 'demography'
DEMOGRAPHY_TRAITS_HEADER = 'continuous\tid\tname'
DEMOGRAPHY_ASNWERS_HEADER = 'id\tname\ttrait_id'
DEMOGRAPHY_DFTS_HEADER = 'max\tmin\ttrait_id'

DEFAULT_DS_PKEY = ['id', 'name', 'country',
                   'begin_period', 'end_period', 'period_type']
STATS_PKEY = ['geo_id', 'node_id', 'platform_id', 'target_group',
              'begin_period', 'end_period', 'period_type']


class ExtractorService():

    def __init__(self, client):
        self.client = client

    def get_n_save_stats_in_available_periods(self, output_folder_path, file_uid, periods, **filter_params):
        '''
        Get stats data and save to specified folder
        :param output_folder_path: output folder to save result file
        :param file_uid: unique identifier to be added to the result file name 
                    so there are no conflicts when iterating multiple_times with different settings (content agnostic)
        :param country: Optional country.
        :param output_type: Optional 'json' or 'csv'.
        :param geo List of selected geolocation ids. Optional. When missing, statistics are listed for all possible values. .
        :param platform  List of selected platform. Optional. When missing, statistics are listed for all possible values. 
        :param node  List of selected node ids. Optional. When missing, statistics are listed for all possible values. 
        :param metric  List of selected selected metric. Optional. When missing, values for all metrics are shown. 
        :param target  List of  selected target group. Optional. When missing, statistics are listed for target group "Population". 
                        One target group is described by string with two parts separated by semicolon (';'):
                        [name] - name of target group. It will be shown in results. Optional. When missing, definition will be used.
                        definition - definition of target group in form of equalities: {trait}={value} joined by and (logical and) or or (logical or).

        '''

        res_files = []
        country_list = periods.keys()

        append_headers = ['country']

        for country in country_list:
            file_path = os.path.join(
                output_folder_path, 'stats' + '-' + str(file_uid) + '-' + country + '.csv')

            with open(file_path, 'w+', newline='') as out_file:
                writer = csv.writer(out_file, delimiter=',',
                                    quotechar='"', quoting=csv.QUOTE_MINIMAL)
                self._get_n_write_ds_in_period_in_country(
                    'stats', writer,  periods, country, append_headers, [country], **filter_params)
                
               
            
            #remove if empty
            if os.stat(out_file.name).st_size > 0:
                res_files += [{'full_path': file_path,
                               'type': 'stats',
                               'name': os.path.basename(out_file.name),
                               'pkey': STATS_PKEY}]
            else:
                os.remove(out_file)
        
        return res_files

    def get_periods_in_interval(self, begin=None, end=None, period_type='daily', country_list=None):
        return self.client.get_all_available_periods(
            begin, end, period_type, country_list)

    def get_n_save_dataset_in_available_periods(self, endpoint_name, output_folder_path, file_uid, periods):
        '''
        Gets and saves specified dataset to output folder

        endpoint_name -- name of endpoint (dataset)
        output_folder_path -- 
        periods -- dictionary with periods (containing country spec) as returned by @self.get_periods_in_interval
        file_uid -- unique identifier to be added to the result file name 
                    so there are no conflicts when iterating multiple_times with different settings (content agnostic)
        Returns list of result file paths

        '''
        # separate demography (different structure
        if endpoint_name == ENDPOINT_DEMOGRAPHY:
            return self.get_n_save_demography(output_folder_path, file_uid, periods)

        res_files = []
        country_list = periods.keys()

        append_headers = ['country']

        for country in country_list:
            file_path = os.path.join(
                output_folder_path, endpoint_name + '-' + str(file_uid) + country + '.csv')

            with open(file_path, 'w+', newline='') as out_file:
                writer = csv.writer(out_file, delimiter=',',
                                    quotechar='"', quoting=csv.QUOTE_MINIMAL)
                self._get_n_write_ds_in_period_in_country(
                    endpoint_name, writer,  periods, country, append_headers, [country])

            #remove if empty
            if os.stat(out_file.name).st_size > 0:
                res_files += [{'full_path': file_path,
                               'type': endpoint_name,
                               'name': os.path.basename(out_file.name),
                               'pkey': DEFAULT_DS_PKEY}]
            else:
                os.remove(out_file)

        return res_files

    def get_n_save_demography(self, output_folder_path, file_uid, periods):
        '''
        get and save demography data for specified period into specified folder

        Returns -- list of result file paths
        '''

        country_list = periods.keys()
        res_files = []
        for country in country_list:
            res = self._get_n_write_demography_in_period_in_country(
                output_folder_path, str(file_uid), periods, country, [], [country])
            res_files.extend(res)
        return res_files

#============== PRIVATE METHODS

    def _get_n_write_ds_in_period_in_country(self, endpoint_name, writer, periods, country, append_headers, append_data, **additional_params):
        write_header = True
        for period in periods.get(country):
            res = self.client.get_dataset_generic(
                endpoint_name, period[KEY_PERIOD_BEGIN], period[KEY_PERIOD_END], country, output_type='csv', **additional_params)

            self._write_ds_resp_in_period(
                res.text, writer, period, append_headers, append_data, write_header)
            write_header = False
        return True

    def _get_n_write_demography_in_period_in_country(self, output_folder_path, file_uid, periods, country, append_headers, append_data):
        '''
        Method to get and process demography endpoint response. Creates three file types when available
        '''

        write_header = True
        res_files = []
        for period in periods.get(country):
            res = self.client.get_dataset_generic(
                ENDPOINT_DEMOGRAPHY, period[KEY_PERIOD_BEGIN], period[KEY_PERIOD_END], country, output_type='csv')

            traits = defaults = answers = False
            # res file paths
            traits_path = os.path.join(
                output_folder_path, ENDPOINT_DEMOGRAPHY + '-' + 'traits' + '-' + file_uid + '-' + country + '.csv')
            answers_path = os.path.join(
                output_folder_path, ENDPOINT_DEMOGRAPHY + '-' + 'answers' + '-' + file_uid + '-' + country + '.csv')
            defaults_path = os.path.join(
                output_folder_path, ENDPOINT_DEMOGRAPHY + '-' + 'defaults' + '-' + file_uid + '-' + country + '.csv')

            # res file writers
            traits_writer = csv.writer(open(traits_path, 'w+', newline=''), delimiter=',',
                                       quotechar='"', quoting=csv.QUOTE_MINIMAL)
            answers_writer = csv.writer(open(answers_path, 'w+', newline=''), delimiter=',',
                                        quotechar='"', quoting=csv.QUOTE_MINIMAL)
            defaults_writer = csv.writer(open(defaults_path, 'w+', newline=''), delimiter=',',
                                         quotechar='"', quoting=csv.QUOTE_MINIMAL)

            # split file by empty line
            dem_files = res.text.split(os.linesep + os.linesep)
            
            for line in dem_files:
                if DEMOGRAPHY_TRAITS_HEADER in line:
                    traits = True
                    # traits
                    self._write_ds_resp_in_period(
                        res.text, traits_writer, period, append_headers, append_data, write_header)

                elif DEMOGRAPHY_ASNWERS_HEADER in line:
                    answers = True
                    # answers
                    self._write_ds_resp_in_period(
                        res.text, answers_writer, period, append_headers, append_data, write_header)
                elif DEMOGRAPHY_DFTS_HEADER in line:
                    defaults = True
                    # defaults
                    self._write_ds_resp_in_period(
                        res.text, defaults_writer, period, append_headers, append_data, write_header)
            write_header = False

            del traits_writer, answers_writer, defaults_writer

            # cleanup empty files, add metadata
            if traits:
                res_files += [{'full_path': traits_path,
                               'type': ENDPOINT_DEMOGRAPHY+"_traits",
                               'name': os.path.basename(traits_path),
                               'pkey': DEFAULT_DS_PKEY}]
            else:
                os.remove(traits_path)
            if answers:
                res_files += [{'full_path': answers_path,
                               'type': ENDPOINT_DEMOGRAPHY+"_answers",
                               'name': os.path.basename(answers_path),
                               'pkey': DEFAULT_DS_PKEY}]
            else:
                os.remove(answers_path)
            if defaults:
                res_files += [{'full_path': defaults_path,
                               'type': ENDPOINT_DEMOGRAPHY+"_defaults",
                               'name': os.path.basename(defaults_path),
                               'pkey': DEFAULT_DS_PKEY}]
            else:
                os.remove(defaults_path)

        return res_files

    def _write_ds_resp_in_period(self, csv_data, writer, period, append_headers, append_data, write_header):
        '''
        Writes csv response to file. 
        '''

        reader = csv.reader(io.StringIO(csv_data),
                            delimiter='\t', quotechar='"')
        for row in reader:
            if write_header:
                writer.writerow(row + append_headers + PERIOD_HEADER)
                write_header = False
                continue

            writer.writerow(
                row + append_data + [period[KEY_PERIOD_BEGIN], period[KEY_PERIOD_END], period[KEY_PERIOD_TYPE]])
        return True
