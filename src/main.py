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

def write_file(bus_points, day): 
    #criar dataframe
    #se o timestamp do item for de acordo com o timestamp da pasta, ele é adicionado ao dataframe
    path = rf"C:\Users\Rafael\Desktop\Rafael\UFAL\PIBIC\BRBus\Trajectory_data\trajectory_528\{day[:10]}"
    tam = 0
    if not os.path.exists(path):
        os.makedirs(path)
    get_time = day[11:13]+day[-1]

    #if not os.path.exists(f"{path}\\cur_{get_time}"):
        # Define o nome do arquivo Parquet
    filename = os.path.join(path, f'cur_{get_time}.parquet')
    
    # Cria um DataFrame com os dados de 'i'
    df = pd.DataFrame(bus_points)
    
    # Salva o DataFrame em um arquivo Parquet
    df.to_parquet(filename)
    
    #else:
        #existente = pd.read_parquet(f"{path}\\cur_{get_time}")
        # Concatena o DataFrame existente com o DataFrame acumulado
        #df_new = pd.DataFrame(bus_points)
    
        # Concatenar o DataFrame existente com o novo DataFrame
        #concatenado = pd.concat([existente, df_new], ignore_index=True)
        
        # Salvar o DataFrame resultante de volta no arquivo Parquet original
        #concatenado.to_parquet(f"{path}\\cur_{get_time}")
    tam+=1


def convert_to_trajectory(path):
    #define a pasta dos arquivos 
    files = os.listdir(path)
    csv_files = [file for file in files if file.endswith('.csv')]

    bus_days = []
    size = 1
    print("mapping timestamps")
    for item in csv_files:
        if size<1000:
            #transforma cada csv em df
            file_path = os.path.join(path, item)
            file = pd.read_csv(file_path)

            for index, row in file.iterrows():
                if (row['updated_at'])[:15] not in bus_days:
                    bus_days.append((row['updated_at'])[:15])
            size+=1
    size = 1
    print(bus_days)
    for day in bus_days:
        changed = 0
        print(day)
        size = 1
        bus_points = []
        for item in csv_files:
            if changed == 1:
                changed = 0
                break
            
            if size<1000:
                #transforma cada csv em df
                file_path = os.path.join(path, item)
                file = pd.read_csv(file_path)
                for index, row in file.iterrows():

                    if (row['updated_at'])[:15] == day:
            
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
            
                size+=1
            else:
                break
        print(len(bus_points))
        write_file(bus_points, day)

convert_to_trajectory(r"C:\Users\Rafael\Downloads\curitiba")