import os
import pandas as pd

def isdigit(input):
    #checa se a entrada é digito
    try:
        val = int(input)
    except ValueError:
        return False
    return True

<<<<<<< Updated upstream
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
=======
def write_file(bus_points): 
    for key in bus_points:
        path = rf"C:\Users\Rafael\Desktop\Rafael\UFAL\PIBIC\BRBus\Trajectory_data\trajectory_528\{key[:10]}"
        if not os.path.exists(path):
            os.makedirs(path)
        filename = os.path.join(path, f'cur_{key[11:13]+key[-1]}.parquet')

        df = pd.DataFrame(bus_points[key])

        df.to_parquet(filename)
>>>>>>> Stashed changes

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
<<<<<<< Updated upstream
        if size<100:
=======
        if size<5000:
>>>>>>> Stashed changes
            #transforma cada csv em df
            file_path = os.path.join(path, item)
            file = pd.read_csv(file_path)

            #itera cada coluna do df para extrair as info necessárias
            for index, row in file.iterrows():
<<<<<<< Updated upstream

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
=======
                if (row['updated_at'])[:15] not in bus_days:
                    bus_days.append((row['updated_at'])[:15])
            size+=1
            print(size)
    bus_points = {}

    size = 1
    for item in csv_files:
        if size<5000:
            #transforma cada csv em df
            file_path = os.path.join(path, item)
            file = pd.read_csv(file_path)
            for index, row in file.iterrows():
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

                key = (row['updated_at'])[:15]

                if key in bus_points:
                    bus_points[key].append(point)
                else:
                    bus_points[key] = [point]
            print(size)
            size+=1
        else:
            break
    write_file(bus_points)
>>>>>>> Stashed changes

                    #junta todos os atributos no formato P = (id, loc, semantica), o id do onibus é só para separar o arquivo depois
                    point = (row['bus_id'], id, loc, Semantica)

                    #junta os pontos no formato T = (p1, p2, ..., pi)
                    bus_points.append(point)

                    id+=1
        size+=1

    #escreve os pontos em arquivos csv
    write_csv(r"path",bus_ids, bus_points)

convert_to_trajectory(r"path", 528)