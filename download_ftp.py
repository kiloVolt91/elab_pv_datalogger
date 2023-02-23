# INIZIALIZZAZIONE DATI

import numpy as np
from datetime import datetime
import ftplib
import os
from cfg_docu.init import *
from cfg_docu.dati import *

today=datetime.today().strftime('%y%m%d')

#data_inizio=str('230223') #INSERIRE DATA DI INIZIO formato AA+MM+DD
data_inizio= str(input('INSERIRE DATA DI INIZIO formato AA+MM+DD: '))

# INIZIO OPERAZIONI SCRIPT

os.chdir(download_data_path)
ftpc = ftplib.FTP()
ftpc.connect(HOST, PORT)
ftpc.login(USER, PASSWD)

for m in mesi:
    for d in range (1, month[m]+1):
        data = str(data_inizio[0:2]) + str(mesi.index(m)+1).zfill(2) + str(d).zfill(2)
        if data >= data_inizio and data <= today:
            print('Giorno :', data)
            file_originale_inverter = 'int_gefran_' + data + '.txt'
            file_originale_stringhe = 'int_gefran_sb_' + data + '.txt'
            file_scaricato_inverter = os.path.join(data, 'download_int_' + data +'.txt')
            file_scaricato_stringhe = os.path.join(data, 'download_sb_'+  data +'.txt')
            if data < today:
                file_originale_tag = 'tag_' + data + '.txt'
                file_scaricato_tag = os.path.join(data , 'download_tag_' + data + '.txt')
            print('Directory Iniziale: ', download_data_path)

            if os.path.isdir(data): 
                print ('cartella esistente') 
            else:
                    os.makedirs(data) 
                    print ('cartella creata')   
            
            ftp_path = ftpc.pwd()
            ftpc.cwd('DATA')
            ftpc.cwd(data)
            ftpc.retrlines('LIST')
            ftpc.encoding='utf-8'
            
            if data < today:
                files = [(file_originale_inverter, file_scaricato_inverter), (file_originale_stringhe, file_scaricato_stringhe), (file_originale_tag, file_scaricato_tag)]
            else:
                files = [(file_originale_inverter, file_scaricato_inverter), (file_originale_stringhe, file_scaricato_stringhe)]

            for file in files:
                with open(file[1], "wb") as f:
                    ftpc.retrbinary("RETR " + file[0], f.write)                    
            ftpc.cwd(ftp_path)
            os.chdir(download_data_path)
ftpc.quit()

print('Fine Operazioni')