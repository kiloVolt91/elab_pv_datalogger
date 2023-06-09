#Selezione di una data di INIZIO per l'import dei dati
from configurazione.dati import *

def seleziona_data_inizio (lista_date):
    while True:
        ans = input("Selezionare una data di inizio per l'import dei dati? (y/n): ")
        if ans.lower() == 'y':
            anno_inizio = input("Inserire anno nel formato AAAA")

            if anno_inizio.isnumeric() == False:
                sys.exit('Il valore inserito non è un numero')
            if (len(str(anno_inizio)) != 4) or (str(anno_inizio)[0:2]!='20'):
                sys.exit('Il valore inserito è errato o non è presente') 
            print('Hai selezionato il seguente anno: ', anno_inizio)
            data_inizio = str(anno_inizio)[2:4]
            mese_inizio = input("inserire mese nel formato mm")
            if mese_inizio.isnumeric() == False:
                sys.exit('Il valore inserito non è un numero')
            if (len(str(mese_inizio)) != 2) or (int(mese_inizio)>12):
                sys.exit('Il valore inserito è errato o non è presente') 
            print('Hai selezionato il seguente mese: ', mese_inizio)
            data_inizio = data_inizio + mese_inizio

            if (mese_inizio == "02"):
                if (int(anno_inizio)%4 == 0 and int(anno_inizio)%100 !=0) or (int(anno_inizio)%400 == 0):
                    mese_inizio="2b"
            giorno_inizio = input("inserire giorno nel formato dd") 
            if giorno_inizio.isnumeric() == False:
                sys.exit('Il valore inserito non è un numero')
            if (len(str(giorno_inizio)) != 2) or (int(giorno_inizio)>(month[mese_inizio])):
                sys.exit('Il valore inserito è errato o non è presente') 
            print('Hai selezionato il seguente giorno: ', giorno_inizio)
            data_inizio = data_inizio + giorno_inizio

            if lista_date.count(data_inizio) == 0:
                sys.exit('La data scelta non è presente tra quelle disponibili') 
            break
        elif ans.lower() == 'n':
            data_inizio = lista_date[0]
            print('Hai selezionato il seguente giorno: ', data_inizio)
            break
        else:
            print('Il carattere inserito è errato')
    return (data_inizio)


#Selezione di una data di FINE per l'import dei dati
def seleziona_data_fine (lista_date, data_inizio):
    while True:
        ans = input("Selezionare una data di fine per l'import dei dati? (y/n): ")
        if ans.lower() == 'y':
            anno_fine = input("Inserire anno nel formato AAAA")

            if anno_fine.isnumeric() == False:
                sys.exit('Il valore inserito non è un numero')
            if (len(str(anno_fine)) != 4) or (str(anno_fine)[0:2]!='20'):
                sys.exit('Il valore inserito è errato o non è presente') 
            print('Hai selezionato il seguente anno: ', anno_fine)
            data_fine = str(anno_fine)[2:4]
            mese_fine = input("inserire mese nel formato mm")
            if mese_fine.isnumeric() == False:
                sys.exit('Il valore inserito non è un numero')
            if (len(str(mese_fine)) != 2) or (int(mese_fine)>12):
                sys.exit('Il valore inserito è errato o non è presente') 
            print('Hai selezionato il seguente mese: ', mese_fine)
            data_fine = data_fine + mese_fine

            if (mese_fine == "02"):
                if (int(anno_fine)%4 == 0 and int(anno_fine)%100 !=0) or (int(anno_fine)%400 == 0):
                    mese_fine="2b"
            giorno_fine = input("inserire giorno nel formato dd") 
            if giorno_fine.isnumeric() == False:
                sys.exit('Il valore inserito non è un numero')
            if (len(str(giorno_fine)) != 2) or (int(giorno_fine)>(month[mese_fine])):
                sys.exit('Il valore inserito è errato o non è presente') 
            print('Hai selezionato il seguente giorno: ', giorno_fine)
            data_fine = data_fine + giorno_fine

            if lista_date.count(data_fine) == 0:
                sys.exit('La data scelta non è presente tra quelle disponibili')
            if lista_date.index(data_inizio) > lista_date.index(data_fine):
                sys.exit('La data scelta è antecedente alla data di inizio')
            break
        elif ans.lower() == 'n':
            data_fine = lista_date[len(lista_date)-1]
            print('Hai selezionato il seguente giorno: ', data_fine)
            break
        else:
            print('Il carattere inserito è errato')
    return (data_fine)

def sql_export_df(df, sql_tabella):
    try:
        cnx = mysql.connector.connect(user=user, password=password, host=host, database=database)  
        print('connesso')

    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
      else:
        print(err) 

    columns = df.columns.tolist()
    placeholders = '%s'
    str_nomi = '('+columns[0]+','
    str_vals = '(%s,'
    for i in range(1, len(columns)):
        if i == len(columns)-1:
            str_nomi = str_nomi +'`'+ columns[i] +'`' +')'
            str_vals = str_vals + placeholders + ')'
        else: 
            str_nomi = str_nomi +'`'+ columns[i] +'`'+', '
            str_vals = str_vals + placeholders + ', '
    mysql_str = "INSERT INTO "+ sql_tabella+ " {col_name} VALUES {values}".format(col_name = str_nomi, values = str_vals)
    cursor = cnx.cursor()

    for i in range (0, df.shape[0]):
        if i%100==True:
            print('.')
        cursor.execute (mysql_str, df.iloc[i].tolist())
    cnx.commit()
    cursor.close()
    cnx.close()
    return