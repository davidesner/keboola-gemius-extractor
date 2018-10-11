#==============================================================================
# KBC Env handler
#==============================================================================


#============================ Import libraries ================================
import logging
import json
import os
import csv
import pytz
from collections import Counter
from keboola import docker
import datetime
from dateutil.relativedelta import relativedelta

DEFAULT_DEL = ','
DEFAULT_ENCLOSURE = '"'


class KBCEnvHandler:
    def __init__(self, mandatory_params, data_path=None):
        # fetch data folder from ENV by default
        if not data_path:
            data_path = os.environ.get('KBC_DATADIR')

        self.kbc_config_id = os.environ.get('KBC_CONFIGID')
        
        self.data_path = data_path
        self.configuration = docker.Config(data_path)
        self.cfg_params = self.configuration.get_parameters()
        self.tables_out_path = os.path.join(data_path, 'out', 'tables')
        self.tables_in_path = os.path.join(data_path, 'in', 'tables')

        self._mandatory_params = mandatory_params

#==============================================================================

    def validateConfig(self):
        '''
        Validates config based on provided mandatory params.
        Parameters can be grouped as arrays [Par1,Par2] => at least one of the pars has to be present
        [par1,[par2,par3]] => either par1 OR both par2 and par3 needs to be present
        '''
        parameters = self.cfg_params
        missing_fields = []
        for field in self._mandatory_params:
            if isinstance(field, list):
                missing_fields.extend(self._validate_par_group(field))            
            elif not parameters.get(field):
                missing_fields.append(field)

        if missing_fields:
            raise ValueError(
                'Missing mandatory configuration fields: [{}] '.format(', '.join(missing_fields)))

    def _validate_par_group(self, par_group):
        missing_fields = []
        is_present = False
        for par in par_group:
            if isinstance(par,list):
                missing_subset = self._get_par_missing_fields(par)
                missing_fields.extend(missing_subset)
                if not missing_subset:
                    is_present = True
                    
            elif self.cfg_params.get(par):                
                is_present = True
            else:
                missing_fields.append(par)
        if not is_present:
            return missing_fields
        else:
            return []
    
    def _get_par_missing_fields(self, mand_params):
        parameters = self.cfg_params
        missing_fields = []
        for field in mand_params:
           if not parameters.get(field):
                missing_fields.append(field)
        return missing_fields
    
    
    
    
    def get_input_table_by_name(self, table_name):
        tables = self.configuration.get_input_tables()
        table = [t for t in tables if t.get('destination') == table_name]
        if not table:
            raise ValueError('Specified input mapping [{}] does not exist'.format(table_name))
        return table[0]

#================================= Logging ====================================

    def set_default_logger(self, log_level = 'INFO'):
        logging.basicConfig(
            level=log_level,
            format='%(levelname)s - %(message)s')

        logger = logging.getLogger()
        return logger

    def get_state_file(self):
        logging.getLogger().info('Loading state file..')
        state_file_path = os.path.join(self.data_path, 'in', 'state.json')
        if os.path.isfile(state_file_path):
            logging.getLogger().info('State file not found. First run?')
        try:
            with open(state_file_path, 'r') \
                    as state_file:
                return json.load(state_file)
        except (OSError, IOError):
            raise ValueError(
                "State file state.json unable to read "
            )

    def create_sliced_tables(self, folder_name, pkey=None, incremental=False, src_delimiter=DEFAULT_DEL, src_enclosure=DEFAULT_ENCLOSURE, dest_bucket=None):
        """
        Creates prepares sliced tables from all files in DATA_PATH/out/tables/{folder_name} - i.e. removes all headers
        and creates single manifest file based on provided parameters.

        folder_name -- folder name in DATA_PATH directory that contains files for slices, the same name will be used as table name
        src_enclosure -- enclosure of the source file ["]
        src_delimiter -- delimiter of the source file [,]
        dest_bucket -- name of the destination bucket (optional)


        """
        log = logging
        log.info('Creating sliced tables for [{}]..'.format(folder_name))

        folder_path = os.path.join(self.tables_out_path, folder_name)

        if not os.path.isdir(folder_path):
            raise ValueError("Specified folder ({}) does not exist in the data folder ({})".format(
                folder_name, self.data_path))

        # get files
        files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(
            os.path.join(folder_path, f))]

        header = self.get_and_remove_headers_in_all(
            files, src_delimiter, src_enclosure)
        if dest_bucket:
            destination = dest_bucket + '.' + folder_name

        log.info('Creating manifest file..')
        self.configuration.write_table_manifest(
            file_name=folder_path, destination=destination, primary_key=pkey, incremental=incremental, columns=header)

    def get_and_remove_headers_in_all(self, files, delimiter, enclosure):
        """
        Removes header from all specified files and return it as a list of strings

        Throws error if there is some file with different header.

        """
        first_run = True
        for file in files:
            curr_header = self._get_and_remove_headers(
                file, delimiter, enclosure)
            if first_run:
                header = curr_header
                first_file = file
                first_run = False
            # check whether header matches
            if Counter(header) != Counter(curr_header):
                raise Exception('Header in file {}:[{}] is different than header in file {}:[{}]'.format(
                    first_file, header, file, curr_header))
        return header

    def _get_and_remove_headers(self, file, delimiter, enclosure):
        """
        Removes header from specified file and return it as a list of strings.
        Creates new updated file 'upd_'+origFileName and deletes the original
        """
        head, tail = os.path.split(file)
        with open(file, "r") as input_file:
            with open(os.path.join(head, 'upd_' + tail), 'w+',  newline='') as updated:
                reader = csv.DictReader(
                    input_file, delimiter=delimiter, quotechar=enclosure)
                header = reader.fieldnames
                writer = csv.DictWriter(
                    updated, fieldnames=header, delimiter=DEFAULT_DEL, quotechar=DEFAULT_ENCLOSURE)
                for row in reader:
                    # write row
                    writer.writerow(row)
        os.remove(file)
        return header
#==============================================================================
#== UTIL functions

    def get_past_date(self, str_days_ago, tz = pytz.utc):
        '''
        Returns date in specified timezone relative to today.
        
        e.g.
        '5 hours ago',
        'yesterday',
        '3 days ago',
        '4 months ago',
        '2 years ago',
        'today'
        '''
        
        TODAY = datetime.datetime.now(tz)
        splitted = str_days_ago.split()
        if len(splitted) == 1 and splitted[0].lower() == 'today':
            return str(TODAY.isoformat())
        elif len(splitted) == 1 and splitted[0].lower() == 'yesterday':
            date = TODAY - relativedelta(days=1)
            return str(date.isoformat())
        elif splitted[1].lower() in ['hour', 'hours', 'hr', 'hrs', 'h']:
            date = datetime.datetime.now() - relativedelta(hours=int(splitted[0]))
            return str(date.date().isoformat())
        elif splitted[1].lower() in ['day', 'days', 'd']:
            date = TODAY - relativedelta(days=int(splitted[0]))
            return str(date.isoformat())
        elif splitted[1].lower() in ['wk', 'wks', 'week', 'weeks', 'w']:
            date = TODAY - relativedelta(weeks=int(splitted[0]))
            return str(date.isoformat())
        elif splitted[1].lower() in ['mon', 'mons', 'month', 'months', 'm']:
            date = TODAY - relativedelta(months=int(splitted[0]))
            return str(date.isoformat())
        elif splitted[1].lower() in ['yrs', 'yr', 'years', 'year', 'y']:
            date = TODAY - relativedelta(years=int(splitted[0]))
            return str(date.isoformat())
        else:
            return "Wrong Argument format"


