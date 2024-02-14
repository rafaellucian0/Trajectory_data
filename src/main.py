import os
import pandas as pd

def isdigit(input):
    #checa se a entrada é digito
    try:
        val = int(input)
    except ValueError:
        return False
    return True

def write_file(bus_points): 
    #PARQUET

    for key in bus_points:
        path = rf"path"
        if not os.path.exists(path):
            os.makedirs(path)
        filename = os.path.join(path, f'{key}.parquet')

        df = pd.DataFrame(bus_points[key])

        df.to_parquet(filename)


def convert_to_trajectory(path):
    #define a pasta dos arquivos 
    files = os.listdir(path)
    csv_files = [file for file in files if file.endswith('.csv')]

    size = 1
    bus_points = {}
    for item in csv_files:
        if size <= 500:
            #transforma cada csv em df
            file_path = os.path.join(path, item)
            file = pd.read_csv(file_path)
            for index, row in file.iterrows():
                Semantica = []

                #loc = [row['latitude'], row['longitude']]
                x = row['latitude']
                y = row['longitude']



                #verifica se tem a info da velocidade
                if isdigit(row['bus_speed']):
                    Semantica.append("speed: "+ str(int(row['bus_speed'])))

                #verifica se tem a info da direcao
                if isdigit(row['bus_direction']):
                    Semantica.append("direction: "+ str(int(row['bus_direction'])))
                
                time = str(row['updated_at'])

                #junta todos os atributos no formato P = (id, loc, semantica), o id do onibus é só para separar o arquivo depois

                point = (x,y, time, Semantica)

                #timestamp em parquet (olhar github)
                key = row['bus_id']

                if key in bus_points:
                    bus_points[key].append(point)
                else:
                    bus_points[key] = [point]
                    bus_points[key].append(point)
                    
            print(size)
            size+=1
    #print(bus_points)
    write_file(bus_points)

convert_to_trajectory(r"path")