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

def write_csv(path, bus_ids, bus_points):
    #header de cada csv
    header = ['id', 'loc', 'Semantica']
    
    for id in bus_ids:

        #define o local de salvamento do arquivo
        filename = os.path.join(path, f'{id}.csv') 
        
        with open(filename, 'w', newline="") as file:
            #escreve info no csv
            csvwriter = csv.writer(file)
            csvwriter.writerow(header)
            for item in bus_points:
                if item[0]==id:
                    csvwriter.writerow(item[1:])

def convert_to_trajectory(path, line_code):
    #define a pasta dos arquivos 
    files = os.listdir(path)
    csv_files = [file for file in files if file.endswith('.csv')]

    #variáveis para verificar e agrupar os pontos pelo id
    bus_ids = []
    bus_points = []

    id = 1
    size = 1

    for item in csv_files:
        if size<100:
            #transforma cada csv em df
            file_path = os.path.join(path, item)
            file = pd.read_csv(file_path)

            #itera cada coluna do df para extrair as info necessárias
            for index, row in file.iterrows():

                #checa se o onibus faz parte da linha
                if ((row['line_code'])==line_code or (row['line_code'])==str(line_code)):

                    #add o id do onibus pra agrupar os pontos dele em arquivos separados
                    if row['bus_id'] not in bus_ids:   
                        bus_ids.append(row['bus_id'])

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
                    
                    Semantica.append("timestamp: "+ str(row['updated_at']))

                    #junta todos os atributos no formato P = (id, loc, semantica), o id do onibus é só para separar o arquivo depois
                    point = (row['bus_id'], id, loc, Semantica)

                    #junta os pontos no formato T = (p1, p2, ..., pi)
                    bus_points.append(point)

                    id+=1
        size+=1

    #escreve os pontos em arquivos csv
    write_csv(r"path",bus_ids, bus_points)

convert_to_trajectory(r"path", 528)