import os
import csv
import pandas as pd

def isdigit(input):
    #checa se a entrada é digito
    try:
        val = int(input)
    except ValueError:
        return False
    return True

def write_file(bus_points, bus_days): 
    #criar dataframe
    #se o timestamp do item for de acordo com o timestamp da pasta, ele é adicionado ao dataframe
    for timestamp in bus_days:
        df = pd.DataFrame()   
        for item in bus_points:
            foldername=((item[2])[-1])[0:10]

            if foldername == timestamp:
                #define o local de salvamento do arquivo
                path = rf"path\{timestamp}"

                if not os.path.exists(path):
                    os.makedirs(path)
            
                df = pd.concat([df, pd.DataFrame([item])], ignore_index=True)
        print(df)
    
        #define o nome do arquivo parquet
        filename = os.path.join(path, f'{item[0]}.parquet')
    
        #salva o df em parquet
        df.to_parquet(filename)

def convert_to_trajectory(path, line_code):
    #define a pasta dos arquivos 
    files = os.listdir(path)
    csv_files = [file for file in files if file.endswith('.csv')]

    #variáveis para verificar e agrupar os pontos pelo id
    bus_days = []
    bus_points = []

    size = 1

    for item in csv_files:
        if size<=100:
            #transforma cada csv em df
            file_path = os.path.join(path, item)
            file = pd.read_csv(file_path)

            #itera cada coluna do df para extrair as info necessárias
            for index, row in file.iterrows():

                #checa se o onibus faz parte da linha
                if ((row['line_code'])==line_code or (row['line_code'])==str(line_code)):

                    #add o id do onibus pra agrupar os pontos dele em arquivos separados
                    if (row['updated_at'])[:10] not in bus_days:
                        bus_days.append((row['updated_at'])[:10])

                    Semantica = []

                    loc = [row['latitude'], row['longitude']]
                    #x = row['latitude']
                    #y = row['longitude']

                    #verifica se tem a info da velocidade
                    if isdigit(row['bus_speed']):
                        Semantica.append("speed: "+ str(int(row['bus_speed'])))

                    #verifica se tem a info da direcao
                    if isdigit(row['bus_direction']):
                        Semantica.append("direction: "+ str(int(row['bus_direction'])))
                    
                    Semantica.append(str(row['updated_at']))

                    #junta todos os atributos no formato P = (id, loc, semantica), o id do onibus é só para separar o arquivo depois
                    point = (row['bus_id'], loc, Semantica)

                    #junta os pontos no formato T = (p1, p2, ..., pi)
                    bus_points.append(point)
                    
            if size % 500 == 0:
                print(size)

        size+=1
    #escreve os pontos em arquivos
    write_file(bus_points, bus_days)

convert_to_trajectory(r"path", 528)