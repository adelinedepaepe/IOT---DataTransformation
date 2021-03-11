import pandas as pd
from os import walk
import os


##Configuration##

#Renseigner le dossier dans lequel se trouvent les fichiers .xlsx
input_filepath = "D:\\Entroview_Data\\Data_modif"

#Renseigner le dossier dans lequel on souhaite stocker le fichier .csv final
output_filepath = "D:\\Entroview_Data\\donnees_aggregees.csv"
#Le . représente le dossier dans lequel est enregistré le script File_cleansing.py

#Lecture du fichier files_list.csv
#Le fichier files_list.csv liste les fichiers déjà lus
with open('D:\\Entroview_Data\\files_list.csv', 'r+') as csv_files_list:
    my_list = csv_files_list.readlines()
    #print(f'mylist: {my_list}')


for (dirpath, dirnames, filenames) in walk(input_filepath):
    for filename in filenames:
        # Si le fichier n'a pas déjà été lu (son nom n'est pas présent dans le fichier files_list.csv)
        if not f'{filename}\n' in my_list and filename[-4:] == 'xlsx':

            # Chargement du fichier
            print(f'ouverture du fichier {filename}')
            df = pd.read_excel(f'{input_filepath}\\{filename}', sheet_name = 'Detail_97_1_1')
            #print(df)
            # # Mise en forme pour debugage
            # pd.set_option('display.max_rows', None)
            # pd.set_option('display.max_columns', None)
            # pd.set_option('display.width', None)
            # pd.set_option('display.max_colwidth', None)

            #Arrondi du temps relatif à la seconde
            #df['Real Time(h:min:s.ms)'] = df['Real Time(h:min:s.ms)'].str[:10]
            df['Relative Time(h:min:s.ms)']= df['Relative Time(h:min:s.ms)'].str.split(".").str[0]

            df.rename(columns={"Real Time(h:min:s.ms)": "Real Time(yyyy-mm-dd)", "Relative Time(h:min:s.ms)": "Relative Time(h:min:s)"}, inplace= True)

            #Aggregation par seconde
            print(f'Aggregation des données du fichier {filename}')
            agg_func = {
                'Record number':['first'],
                'status':['first'],
                'Cycle':['first'],
                'Current(A)':['first'],
                'Voltage(V)':['first'],
                'Capacity(Ah)':['first'],
                'Energy(Wh)':['first'],
                'Auxiliary channel TU1 U(V)':['first'],
                'Auxiliary channel TU2 U(V)':['first'],
                'Auxiliary channel TU1 T(°C)':['mean'],
                'Auxiliary channel TU2 T(°C)':['mean'],
                'Auxiliary pressure difference(V)':['first'],
                'Auxiliary temperature difference(°C)':['mean']
                }
            print(f'group by sur le fichier {filename}')
            #df2 = df.groupby(['Real Time(yyyy-mm-dd)','Steps','Relative Time(h:min:s)']).agg(agg_func)
            df2 = df.groupby(['Steps', 'Relative Time(h:min:s)']).agg(agg_func)

            #Suppression des différents niveaux de colonnes
            print('Suppression niveaux de colonnes')
            df2.columns = [
                'Record number',
                'status',
                'Cycle',
                'Current(A)',
                'Voltage(V)',
                'Capacity(Ah)',
                'Energy(Wh)',
                'Auxiliary channel TU1 U(V)',
                'Auxiliary channel TU2 U(V)',
                'Auxiliary channel TU1 T(°C)',
                'Auxiliary channel TU2 T(°C)',
                'Auxiliary pressure difference(V)',
                'Auxiliary temperature difference(°C)'
                ]
            df3 = df2.reset_index(level=[0,1])
            df4 = df3.sort_values(by=['Record number'])
            #print(df4)

            #Creation du fichier csv
            print(f'Ajout des données du fichier {filename} au fichier csv final')
            #Si le fichier existe déjà, on ajoute les lignes
            if os.path.exists(f'{output_filepath}'):
                df4.to_csv(f'{output_filepath}', mode='a', sep=';', header=None)
            #Si le fichier n'existe pas, on le crée et on ajoute les lignes
            else:
                df4.to_csv(f'{output_filepath}', sep=';')

            #Ecriture du nom du fichier dans files_list
            with open ('files_list.csv', 'a') as csv_files_list:
               csv_files_list.write(f'{filename}\n')

    #Quand tous les fichiers .xlsx du dossier ont été lus
    print("Il n'y a plus de nouveau fichier à traiter. Reportez vous au fichier files_list.csv pour connaître la liste des fichiers déjà traités")

