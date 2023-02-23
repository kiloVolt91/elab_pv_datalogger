#INIZIALIZZAZIONE 
from cfg_docu.funzioni_dataframe import *
from cfg_docu.dati import *
from cfg_docu.init import *
from datetime import datetime

today=datetime.today().strftime('%y%m%d')

## DF
os.chdir(origin_path)
df_year=pd.DataFrame()
data_inizio=str('220101')
for m in mesi:
    for d in range (1, month[m]+1):
        data = str('22') + str(mesi.index(m)+1).zfill(2) + str(d).zfill(2)
        if data >= data_inizio:
            if data < today:
                print(data)
                file_sb = 'download_sb_' + data + '.txt'
                file_int = 'download_int_' + data + '.txt'
                os.chdir('download_ftp')
                os.chdir(data)
                df_sb = get_df_string_box(file_sb)
                df_int = get_df_inverter(file_int, data)
                df_day = get_df_day (df_int, df_sb)
                df_year = pd.concat([df_year, df_day])#, ignore_index=True)
                os.chdir(origin_path)
df_year.fillna(0, inplace=True)        
df_year.to_csv(export_path+'/df_year.csv', index=False)

#ESTRAZIONE DEI MASSIMI GIORNALIERI Potenza e Irraggiamento

header_t_card = ['1.1_TCARD', '1.2_TCARD', '2.1_TCARD', '2.2_TCARD','3.1_TCARD','3.2_TCARD','4.1_TCARD','4.2_TCARD','5.1_TCARD','6.1_TCARD','6.2_TCARD']
df_year['Medie_T_CARD'] = df_year[header_t_card].astype(float).mean(axis=1)

massimi_potenza=df_year.astype({'0_Anno': 'int32','0_Mese': 'int32','0_Giorno': 'int32','0_P_AC': 'float' }).groupby(['0_Anno', '0_Mese', '0_Giorno'])['0_P_AC'].max().to_frame()
massimi_irr=df_year.astype({'0_Anno': 'int32','0_Mese': 'int32','0_Giorno': 'int32','0_Irr': 'float' }).groupby(['0_Anno', '0_Mese', '0_Giorno'])['0_Irr'].max().to_frame()
massimi_temp=df_year.astype({'0_Anno': 'int32','0_Mese': 'int32','0_Giorno': 'int32','Medie_T_CARD': 'float' }).groupby(['0_Anno', '0_Mese', '0_Giorno'])['Medie_T_CARD'].max().to_frame()
massimi=massimi_potenza.merge(massimi_irr, right_index=True, left_index=True)
massimi=massimi.merge(massimi_temp, right_index=True, left_index=True).reset_index()
massimi.to_csv(export_path+'/max.csv', index=False)

##ESTRAZIONE MASSIMO GIORNALIERO E RELATIVO VALORE DI IRRAGGIAMENTO E TEMPERATURA 
#elaborazione grafica

picchi_potenza=pd.DataFrame()
input_df = pd.concat([df_year['0_Irr'].astype('int32'),df_year['0_P_AC'].astype(float)], axis =1 )
input_df = pd.concat([input_df, df_year['Medie_T_CARD'].astype(float)], axis =1)
input_df = pd.concat([input_df, df_year['0_Mese']], axis =1)
input_df = pd.concat([input_df, df_year['0_Giorno']], axis =1)
for i in range(0,massimi_potenza.shape[0]):
    
    iteration=input_df.loc[df_year['0_P_AC'].astype(float) == massimi_potenza.iloc[i,0],['0_P_AC','0_Irr', 'Medie_T_CARD','0_Mese',  '0_Giorno']]
    picchi_potenza = pd.concat([picchi_potenza, iteration])
print(picchi_potenza)

picchi_potenza.to_csv(export_path+'/test.csv', index=False)

m =np.linspace(1,picchi_potenza.shape[0],picchi_potenza.shape[0])
print(m)
fig1, (ax1, ax2, ax3) = plt.subplots(3)
ax1.plot(m,picchi_potenza ['0_P_AC'])
ax2.plot( m, picchi_potenza ['0_Irr'])
ax3.plot(m, picchi_potenza ['Medie_T_CARD'])


fig2, (ax1, ax2, ax3) = plt.subplots(3)
ax1.plot(picchi_potenza ['0_Irr'],picchi_potenza ['0_P_AC'], 'o')
ax2.plot( picchi_potenza ['0_Irr'], picchi_potenza['Medie_T_CARD'], 'o')
ax3.plot(picchi_potenza ['Medie_T_CARD'], picchi_potenza ['0_P_AC'], 'o')

