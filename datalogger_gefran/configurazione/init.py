import configparser
import os

starting_directory = os.getcwd()
percorso_file_configurazione = os.path.join(starting_directory, 'configurazione/configurazione.ini')

download_data_path = os.path.join(starting_directory, 'download_ftp')

config = configparser.ConfigParser()
config.read(percorso_file_configurazione)

#dati datalogger_gefran ftp
ftp_host = config['datalogger_gefran']['HOST']
ftp_port = int(config['datalogger_gefran']['PORT'])
ftp_user = config['datalogger_gefran']['USER']
ftp_password = config['datalogger_gefran']['PASSWD']

host_inverter = config['sql_database_datalogger_energia_vm']['host']
user_inverter = config['sql_database_datalogger_energia_vm']['user']
password_inverter = config['sql_database_datalogger_energia_vm']['password']
db_inverter = config['sql_database_datalogger_energia_vm']['database']
db_table_stringa = config['sql_database_datalogger_energia_vm']['stringa']
db_table_inverter = config['sql_database_datalogger_energia_vm']['inverter']

lista_colonne_da_eliminare = ['s', 'Typ', 'S_VERS', 'Thres_dev', 'Time_dev', 'Str_State', 'Box_State', 'Box_Reg', 'tres comp', 'v', 'DIG_INPUT', 'SPARE1','SPARE2', 'SPARE3', 'SPARE4', 'SPARE5', 'Inv_size', 'ALARM1', 'ALARM2', 'ALARM3',  'Unnamed: 26']



