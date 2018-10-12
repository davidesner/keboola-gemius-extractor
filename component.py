'''
Created on 10. 10. 2018

@author: esner
'''
from kbc.env_handler import KBCEnvHandler
from gemius.extractor_service import ExtractorService
from gemius.client import Client
import logging

import ast
import csv

from datetime import datetime

KEY_USER = 'user'
KEY_PASS = '#pass'
KEY_PERIOD_FROM = 'period_from'
KEY_PERIOD_TO = 'period_to'
KEY_RELATIVE_PERIOD = 'relative_period'
KEY_DATASETS = 'datasets'

KEY_MAND_PERIOD_GROUP = [KEY_PERIOD_FROM, KEY_PERIOD_TO]
KEY_MAND_DATE_GROUP = [KEY_RELATIVE_PERIOD, KEY_MAND_PERIOD_GROUP]


MANDATORY_PARS = [KEY_USER, KEY_PASS, KEY_DATASETS, KEY_MAND_DATE_GROUP]

APP_VERSION = '0.1.1'
class Component(KBCEnvHandler):

    def __init__(self):
        KBCEnvHandler.__init__(self, MANDATORY_PARS)

    def run(self, debug=False):
        '''
        Main execution code
        '''
        self.set_default_logger('DEBUG' if debug else 'INFO')
        logging.info('Running version ' + APP_VERSION)
        logging.info('Loading configuration...')
        self.validateConfig()

        params = self.cfg_params
        # get periods
        if not params.get(KEY_RELATIVE_PERIOD):
            from_date = params.get(KEY_PERIOD_FROM)
            to_date = params.get(KEY_PERIOD_TO)
        else:
            from_date = super().get_past_date(params.get(KEY_RELATIVE_PERIOD))
            to_date = datetime.utcnow()

        gemius_srv = ExtractorService(
            Client(params.get(KEY_USER), params.get(KEY_PASS)))

        datasets = params.get(KEY_DATASETS)

        result_files = []
        index = 0
        for dataset in datasets:
            p_type = dataset.get('period_type')
            logging.info(
                'Downloading dataset %s in period %s - %s [%s]', dataset["dataset_type"], from_date, to_date, p_type)
            index += 1
            periods = gemius_srv.get_periods_in_interval(
                from_date, to_date, p_type)

            countries_no_period = [
                c for c in periods.keys() if len(periods[c]) == 0]

            if not periods or len(countries_no_period) == len(periods.values()):
                logging.warning(
                    'No periods [from:%s,to:%s] type:%s', from_date, to_date, p_type)
                continue
            elif len(countries_no_period) > 0:
                logging.warning(
                    'Some countries contain no specified periods [from:%s,to:%s] type:%s', from_date, to_date, p_type)

            res = self.retrieve_n_save_dataset(
                dataset, periods, index, gemius_srv)

            result_files.extend(res)

        logging.info('Building manifest files..')
        self._process_results(result_files, self.cfg_params.get('bucket'))

        logging.info('Extraction finished sucessfully!')

    def _process_results(self, res_files, output_bucket):
        for res in res_files:
            dest_bucket = 'in.c-esnerda-ex-gemius-' + str(self.kbc_config_id)
            if output_bucket:
                suffix = '-' + output_bucket
            else:
                suffix = ''

            # build manifest
            self.configuration.write_table_manifest(
                file_name=res['full_path'],
                destination=dest_bucket + suffix + '.' + res['type'],
                primary_key=res['pkey'],
                incremental=True)

    def retrieve_n_save_dataset(self, dataset, periods, index, gemius_srv):
        dataset_type = dataset.get('dataset_type')
        if dataset_type == 'stats':
            res = self.retrieve_n_save_stats(
                dataset, periods, index, gemius_srv)
        else:
            res = gemius_srv.get_n_save_dataset_in_available_periods(
                dataset_type, self.tables_out_path, index, periods)

        return res

    def retrieve_n_save_stats(self, dataset, periods, index, service):
        filters = dataset.get('filters')
        filter_dict = {}
        for f in filters:
            filter_dict.update(self._build_filter(f))

        return service.get_n_save_stats_in_available_periods(self.tables_out_path, index, periods, **filter_dict)

    def _build_filter(self, filter_):
        key = filter_.get('filter')
        src = filter_.get('source_table')

        if src.startswith('['):
            # is manually entered
            values = ast.literal_eval(src)
        else:
            # is from table
            values = self._get_values_from_table(src)

        return {key: values}

    def _get_values_from_table(self, input_table_name):
        table = super().get_input_table_by_name(input_table_name)

        with open(table['full_path'], 'r') as input_:
            reader = csv.reader(input_)
            next(reader)
            values = [row[0] for row in reader]

        return values


"""
        Main entrypoint
"""
if __name__ == "__main__":
    comp = Component()
    comp.run()
