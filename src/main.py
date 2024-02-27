import os
import pandas as pd
import csv

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
        path = rf"C:\Users\Rafael\Desktop\Rafael\UFAL\PIBIC\BRBus\Trajectory_data\trajectories_bus\curitiba"
        if not os.path.exists(path):
            os.makedirs(path)
        filename = os.path.join(path, f'{key}.parquet')

        df = pd.DataFrame(bus_points[key])

        df.to_parquet(filename)

    #CSV
    #header = ['id', 'x', 'y', 'Semantica']
    #path = rf"C:\Users\Rafael\Desktop\Rafael\UFAL\PIBIC\BRBus\Trajectory_data\csv_file"

    #for key in bus_points:
        #id = 1

        #define o local de salvamento do arquivo
        #filename = os.path.join(path, f'{key}.csv') 

        #with open(filename, 'w', newline="") as file:
            #escreve info no csv
            #csvwriter = csv.writer(file)
            #csvwriter.writerow(header)
            #id = 1
            #x_last = ""
            #y_last = ""
            #for point in bus_points[key]:
                #print(point)
                #return 0
                #if point[0]!=x_last or y!=y_last:
                    #for i in point[1]:
                        #if i[:4]=='2023':
                            #time = i[8:10]+'/'+i[5:7]+'/'+i[:4]+' '+i[11:19]
                    #new_point = [id, float(x), float(y), time]
                    #csvwriter.writerow(new_point)
                    #id += 1 
                    #x_last=x
                    #y_last=y


def convert_to_trajectory(path):
    #define a pasta dos arquivos 
    files = os.listdir(path)
    csv_files = [file for file in files if file.endswith('.csv')]

    size = 1
    bus_points = {}
    for item in csv_files:
        if size <= 10000:
            #transforma cada csv em df
            file_path = os.path.join(path, item)
            file = pd.read_csv(file_path)
            for index, row in file.iterrows():
                Semantica = []

                loc = [row['latitude'], row['longitude']]
                x = row['longitude']
                y = row['latitude']



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

convert_to_trajectory(r"C:\Users\Rafael\Downloads\curitiba")