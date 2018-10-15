'''
Created on 5. 10. 2018

@author: esner
'''
import logging
import requests
import pandas as pd
from io import StringIO
from io import BytesIO

from kbc.client_base import HttpClientBase

DEAFULT_V1_BASE = 'https://api-audience.gemius.com/v1/'

ENDPOINT_STATS = 'stats'
ENDPOINT_OPEN_SESSION = 'open-session'
ENDPOINT_AVAILABLE_PERIODS = 'available-periods'
ENDPOINT_GEOS = 'geos'
ENDPOINT_PLATFORMS = 'platforms'
ENDPOINT_METRICS = 'metrics'
ENDPOINT_NODES = 'nodes'
ENDPOINT_DEMOGRAPHY = 'demography'
ENDPOINT_TREES = 'trees'
SUPPORTED_ENDPOINTS = [ENDPOINT_STATS, ENDPOINT_AVAILABLE_PERIODS, ENDPOINT_GEOS, ENDPOINT_PLATFORMS,
                       ENDPOINT_METRICS, ENDPOINT_NODES, ENDPOINT_DEMOGRAPHY, ENDPOINT_TREES]


class Client(HttpClientBase):

    def __init__(self, user, password, service_base_url=DEAFULT_V1_BASE):
        HttpClientBase.__init__(self, base_url=service_base_url)
        self.user = user
        self.password = password
        self.session = self.login()
        self.session_param = {'session': self.session}

    def login(self):
        params = {'login': self.user, 'password': self.password}
        url = DEAFULT_V1_BASE + ENDPOINT_OPEN_SESSION
        try:
            r = self.post(url, params=params)
        except requests.HTTPError as e:
            logging.error("Failed to perform login request!", exc_info=e)
            raise e

        return r.get('data').get('session')

    def get_available_periods(self, period_type=None, country=None, output='json'):

        url = DEAFULT_V1_BASE + ENDPOINT_AVAILABLE_PERIODS

        params = {'period_type': period_type,
                  'country': country,
                  'output': output}
        params.update(self.session_param)
        if output == 'json':
            return self.get(url, params=params)
        else:
            return self._get_raw(url, params)

    def get_all_available_periods(self, begin=None, end=None, period_type=None, country_list=None):
        available_periods_raw = self.get_available_periods(
            period_type, output='csv')

        # use pandas DataFrame for simolicity in this case
        date_periods_df = pd.read_table(BytesIO(
            available_periods_raw.content), infer_datetime_format=True, parse_dates=['begin', 'end'])

        if begin and end:
            date_periods_df = date_periods_df[(date_periods_df['begin'] >= begin) & (date_periods_df['begin'] < end)]                

        if country_list and country_list[0] is not None:
            date_periods_df = date_periods_df[date_periods_df['country']
                                              in country_list]

        countries = date_periods_df['country'].drop_duplicates()
        periods_result = {}
        for country in countries:
            # API filter not working, filter period type in code
            periods_result[country] = self._filter_period(date_periods_df, country, period_type)

        return periods_result

    def _filter_period(self, period_df, country, period_type = None):
        if not period_type:
            return period_df.loc[(period_df['country'] == country), ['begin', 'end', 'period type']].to_dict('records')
        else:
            return period_df.loc[(period_df['country'] == country) &
                                                          (period_df['period type'] == period_type), ['begin', 'end', 'period type']].to_dict('records')

    def get_stats_data(self, begin_period=None, end_period=None, country=None, output_type=None, **additional_params):
        '''
        Get stats data.

        :param begin_period: Optional of available begin period
        :param end_period: Optional end of end period.
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
        :return: :class:`Response <Response>` object or 'OrderedDictionary'

        :rtype: requests.Response if output_type == 'csv' else JSON dictionary

        '''
        url = DEAFULT_V1_BASE + ENDPOINT_STATS
        strict = False
        multi_params = {}
        multi_params.update({'geo': additional_params.pop('geo', {})})
        multi_params.update(
            {'platform': additional_params.pop('platform', {})})
        multi_params.update({'node': additional_params.pop('node', {})})
        multi_params.update({'metric': additional_params.pop('metric', {})})
        multi_params.update({'target': additional_params.pop('target', {})})

        single_params = {'begin': self._convert_date(begin_period),
                         'end': self._convert_date(end_period),
                         'output': output_type,
                         'country': country,
                         'strict': strict}

        single_params.update(self.session_param)

        params = self._build_params_with_duplicate_keys(
            single_params, multi_params)

        if output_type == 'JSON':
            return self.get(url, params=params)
        else:
            return self._get_raw(url, params)

    def _convert_date(self, date_obj):
        if isinstance(date_obj, str):
            return date_obj
        else:
            return date_obj.strftime("%Y-%m-%d")

    def _build_params_with_duplicate_keys(self, params_dict, params_lists_dict):
        
        # single params
        single_param_string = '&'.join([key + '=' + str(params_dict[key])
                                  for key in params_dict.keys() if params_dict[key] is not None])

        multi_param_string = '&'.join([self._build_multi_param_string(key, params_lists_dict[key])
                                  for key in params_lists_dict.keys() if params_lists_dict[key]
                                  ])
        param_string = '&'.join([single_param_string, multi_param_string])
        return param_string.encode('utf-8')

    def _build_multi_param_string(self, key, values):
        return '&'.join([key + '=' + str(value) for value in values])

    def get_standard_dataset(self, endpoint_name, begin_period=None, end_period=None, country=None, output_type=None):
        '''
        get specified standard dataset [geos, available-periods,platforms,metrics,trees,nodes,demography]

        output_type -- json,csv [default json]
        '''

        url = DEAFULT_V1_BASE + endpoint_name

        params = {'begin': begin_period,
                  'end': end_period,
                  'output': output_type,
                  'country': country}
        params.update(self.session_param)

        # special case for stats data

        if output_type == 'json':
            return self.get(url, params=params)
        else:
            return self._get_raw(url, params)

    def get_dataset_generic(self, endpoint_name, begin_period=None, end_period=None, country=None, output_type=None, **additional_params):
        '''
        generic get specified dataset

        output_type -- json,csv [default json]
        '''
        if endpoint_name == 'stats':
            return self.get_stats_data(begin_period, end_period, country, output_type, **additional_params)
        elif endpoint_name in SUPPORTED_ENDPOINTS:
            return self.get_standard_dataset(endpoint_name, begin_period, end_period, country, output_type)
        else:
            raise ValueError('Unsupported enpoint! [{}]'.format(endpoint_name))
