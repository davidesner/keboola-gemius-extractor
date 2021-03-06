'''
Created on 9. 10. 2018

@author: esner
'''

import csv
import os

import io
import pandas as pd

PERIOD_HEADER = ['begin_period', 'end_period', 'period_type']

KEY_PERIOD_BEGIN = 'begin'
KEY_PERIOD_END = 'end'
KEY_PERIOD_TYPE = 'period type'

ENDPOINT_DEMOGRAPHY = 'demography'
DEMOGRAPHY_TRAITS_HEADER = 'continuous\tid\tname'
DEMOGRAPHY_ASNWERS_HEADER = 'id\tname\ttrait_id'
DEMOGRAPHY_DFTS_HEADER = 'max\tmin\ttrait_id'

STATS_BASE_HEADER = ['geo_id', 'node_id', 'platform_id', 'target_group']

DEFAULT_DS_PKEY = ['id', 'country',
                   'begin_period', 'end_period', 'period_type']
# endpoint specific pkeys
DS_PKEYS = {"nodes": DEFAULT_DS_PKEY + ['parent_id'],
            "geos": DEFAULT_DS_PKEY + ['parent_id'],
            "platforms": DEFAULT_DS_PKEY + ['parent_id']
            }
STATS_PKEY = ['geo_id', 'node_id', 'platform_id', 'target_group',
              'begin_period', 'end_period', 'period_type']


class ExtractorService():

    def __init__(self, client):
        self.client = client

    def _get_ds_pkey(self, ds_type):
        if DS_PKEYS.get(ds_type):
            return DS_PKEYS[ds_type]
        else:
            return DEFAULT_DS_PKEY

    def get_n_save_stats_in_available_periods(self, output_folder_path, file_uid, periods, metrics, **filter_params):
        '''
        Get stats data and save to specified folder
        :param output_folder_path: output folder to save result file
        :param metrics: list of metric columns to expect in form [name] - names must match exactly the names of columns in filter
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

        append_headers = ['country', 'filter']

        # clean metric names
        header_cleaned = [col['name'].replace('%', 'prc') for col in
                          metrics] + append_headers + STATS_BASE_HEADER + PERIOD_HEADER

        for country in country_list:
            # build additional data
            append_data = {'country': country,
                           'filter': str(filter_params)}

            file_path = os.path.join(
                output_folder_path, 'stats' + '-' + str(file_uid) + '-' + country + '.csv')

            with open(file_path, 'w+', newline='', encoding='utf-8') as out_file:
                writer = csv.DictWriter(out_file, delimiter=',',
                                        quotechar='"', quoting=csv.QUOTE_MINIMAL, fieldnames=header_cleaned,
                                        extrasaction='ignore')

                self._get_n_write_ds_in_periods_in_country(
                    'stats', writer, periods, country, append_headers, append_data, type_='Stats', **filter_params)

            # remove if empty
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

    def get_unique_available_metrics_in_periods(self, periods, metric_ids=None):
        country_list = periods.keys()

        resdf = None

        for country in country_list:
            for period in periods.get(country):
                res = self.client.get_standard_dataset('metrics', begin_period=period[KEY_PERIOD_BEGIN],
                                                       end_period=period[KEY_PERIOD_END], output_type='csv')
                if resdf is None:
                    resdf = pd.read_table(io.BytesIO(res.content))
                else:
                    resdf.append(pd.read_table(io.BytesIO(res.content)))

        resdf = resdf[['id', 'name']].drop_duplicates()

        if metric_ids:
            s = pd.Series(metric_ids, name='id')
            resdf = resdf[resdf['id'].isin(metric_ids)]
            if not s.isin(resdf['id']):
                raise ValueError('Some metric IDs are not valid! {}')

        return resdf.to_dict('records')

    def get_n_save_dataset_in_available_periods(self, endpoint_name, output_folder_path, file_uid, periods):
        '''
        Gets and saves specified dataset to output folder

        endpoint_name -- name of endpoint (dataset) supported :[nodes, demography, trees, geos, platforms, metrics]
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

            with open(file_path, 'w+', newline='', encoding='utf-8') as out_file:
                writer = csv.writer(out_file, delimiter=',',
                                    quotechar='"', quoting=csv.QUOTE_MINIMAL)
                self._get_n_write_ds_in_periods_in_country(
                    endpoint_name, writer, periods, country, append_headers, [country])

            # remove if empty
            if os.stat(out_file.name).st_size > 0:
                res_files += [{'full_path': file_path,
                               'type': endpoint_name,
                               'name': os.path.basename(out_file.name),
                               'pkey': self._get_ds_pkey(endpoint_name)}]
            else:
                os.remove(out_file)

        return res_files

    def get_n_save_demography(self, output_folder_path, file_uid, periods):
        '''
        get and save demography data for specified period into specified folder

        Returns -- list of result file paths
        '''
        append_headers = ['country']
        country_list = periods.keys()
        res_files = []
        for country in country_list:
            res = self._get_n_write_demography_in_period_in_country(
                output_folder_path, str(file_uid), periods, country, append_headers, [country])
            res_files.extend(res)
        return res_files

    # ============== PRIVATE METHODS

    def _get_n_write_ds_in_periods_in_country(self, endpoint_name, writer, periods, country, append_headers,
                                              append_data, type_='Standard', **additional_params):
        write_header = True
        for period in periods.get(country):
            res = self.client.get_dataset_generic(
                endpoint_name, period[KEY_PERIOD_BEGIN], period[KEY_PERIOD_END], country, output_type='csv',
                **additional_params)

            # use stats writer
            if type_ == 'Stats':
                self._write_stats_resp_in_period(res.text, writer, period, append_data, write_header)
            else:
                self._write_ds_resp_in_period(res.text, writer, period, append_headers, append_data, write_header)

            write_header = False
        return True

    def _get_n_write_demography_in_period_in_country(self, output_folder_path, file_uid, periods, country,
                                                     append_headers, append_data):
        '''
        Method to get and process demography endpoint response. Creates three file types when available
        '''

        write_header = True
        res_files = []
        # res file paths
        traits_path = os.path.join(
            output_folder_path, ENDPOINT_DEMOGRAPHY + '-' + 'traits' + '-' + file_uid + '-' + country + '.csv')
        answers_path = os.path.join(
            output_folder_path, ENDPOINT_DEMOGRAPHY + '-' + 'answers' + '-' + file_uid + '-' + country + '.csv')
        defaults_path = os.path.join(
            output_folder_path, ENDPOINT_DEMOGRAPHY + '-' + 'defaults' + '-' + file_uid + '-' + country + '.csv')

        traits = defaults = answers = False
        # res file writers
        with open(traits_path, 'w+', newline='', encoding='utf-8') as traits_f:
            with open(answers_path, 'w+', newline='', encoding='utf-8')as answers_f:
                with open(defaults_path, 'w+', newline='', encoding='utf-8') as defaults_f:
                    traits_writer = csv.writer(
                        traits_f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    answers_writer = csv.writer(
                        answers_f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    defaults_writer = csv.writer(
                        defaults_f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    # iterate periods
                    for period in periods.get(country):
                        res = self.client.get_dataset_generic(
                            ENDPOINT_DEMOGRAPHY, period[KEY_PERIOD_BEGIN], period[KEY_PERIOD_END], country,
                            output_type='csv')
                        # split file by empty line
                        dem_files = res.text.split('\r\n\r\n')

                        for line in dem_files:
                            if DEMOGRAPHY_TRAITS_HEADER in line:
                                traits = True
                                # traits
                                self._write_ds_resp_in_period(
                                    line, traits_writer, period, append_headers, append_data, write_header)

                            elif DEMOGRAPHY_ASNWERS_HEADER in line:
                                answers = True
                                # answers
                                self._write_ds_resp_in_period(
                                    line, answers_writer, period, append_headers, append_data, write_header)
                            elif DEMOGRAPHY_DFTS_HEADER in line:
                                defaults = True
                                # defaults
                                self._write_ds_resp_in_period(
                                    line, defaults_writer, period, append_headers, append_data, write_header)
                        write_header = False

            # cleanup empty files, add metadata
            if traits:
                res_files += [{'full_path': traits_path,
                               'type': ENDPOINT_DEMOGRAPHY + "_traits",
                               'name': os.path.basename(traits_path),
                               'pkey': DEFAULT_DS_PKEY}]
            else:
                os.remove(traits_path)
            if answers:
                res_files += [{'full_path': answers_path,
                               'type': ENDPOINT_DEMOGRAPHY + "_answers",
                               'name': os.path.basename(answers_path),
                               'pkey': DEFAULT_DS_PKEY}]
            else:
                os.remove(answers_path)
            if defaults:
                res_files += [{'full_path': defaults_path,
                               'type': ENDPOINT_DEMOGRAPHY + "_defaults",
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

        if not write_header:
            next(reader)
        for row in reader:
            if write_header:
                row = [col.replace('%', 'prc') for col in row]
                writer.writerow(row + append_headers + PERIOD_HEADER)
                write_header = False
            else:
                writer.writerow(
                    row + append_data + [period[KEY_PERIOD_BEGIN], period[KEY_PERIOD_END], period[KEY_PERIOD_TYPE]])

        return True

    def _write_stats_resp_in_period(self, csv_data, writer, period, append_data, write_header):
        # clean header
        split_resp = csv_data.splitlines()
        split_resp[0] = split_resp[0].replace('%', 'prc')

        reader = csv.DictReader(io.StringIO(os.linesep.join(split_resp)),
                                delimiter='\t', quotechar='"')
        if write_header:
            writer.writeheader()

        period_data = {'begin_period': period[KEY_PERIOD_BEGIN],
                       'end_period': period[KEY_PERIOD_END],
                       'period_type': period[KEY_PERIOD_TYPE]}

        for row in reader:
            row.update(period_data)
            row.update(append_data)
            writer.writerow(row)

        return True
