import os
import csv
from collections import defaultdict
import time
import dask
from dask import delayed, compute
from dask.distributed import Client, as_completed, LocalCluster  

# Função para registrar o log (não modificada)
def registrar_log(mensagem):
    log_file = 'log_CAGEDERRORS.txt'
    with open(log_file, 'a') as f:
        f.write(mensagem + '\n')

# Função para formatar a string do nome do arquivo e obter a data formatada
def format_string(input_str):
    if len(input_str) < 6:
        return "A string deve ter pelo menos 6 caracteres."
   
    last_six = input_str[-10:]
    year = last_six[:4]
    month = last_six[4:-4]
    formatted_date = f"{year}-{month}-01"
   
    return formatted_date

# Função para calcular a média de uma lista de valores
def calcular_media(valores):
    if valores:
        return sum(valores) / len(valores)
    else:
        return 0.0

# Função para determinar a faixa etária
def determinar_faixa_etaria(idade):
    faixas_etarias = {
        '18-29': (18, 29),
        '30-39': (30, 39),
        '40-49': (40, 49),
        '50-59': (50, 59),
        '60+': (60, 200)
    }
    for faixa, (min_idade, max_idade) in faixas_etarias.items():
        if min_idade <= idade <= max_idade:
            return faixa
    return None

# Função para processar cada arquivo CSV
@delayed
def processar_arquivo(csv_file_path, formatted_date):
    subclass_salaries = defaultdict(list)
    subclass_idades = defaultdict(list)
    cbo_salaries = defaultdict(list)
    cbo_idades = defaultdict(list)

    subclass_faixa_salaries = defaultdict(lambda: defaultdict(list))
    cbo_faixa_salaries = defaultdict(lambda: defaultdict(list))

    with open(csv_file_path, mode='r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')

        if 'subclasse' not in reader.fieldnames or 'cbo2002ocupação' not in reader.fieldnames or 'salário' not in reader.fieldnames or 'idade' not in reader.fieldnames:
            registrar_log(f"Erro: As colunas 'subclasse', 'cbo2002ocupação', 'salário' e/ou 'idade' não foram encontradas no arquivo CSV: {csv_file_path}")
        else:
            for row in reader:
                if 'subclasse' in row and 'cbo2002ocupação' in row and 'salário' in row and 'idade' in row:
                    if row['saldomovimentação'] == '1':
                        subclass = row['subclasse'].strip()
                        cbo = row['cbo2002ocupação'].strip()
                        if row['salário'] != '':
                            salario_float = float(row['salário'].replace(',', '.'))
                            

                            # Filtrando salários conforme as unidades salariais
                            if row['unidadesaláriocódigo'] in ['99', '6', '7']:
                                continue
                            elif row['unidadesaláriocódigo'] == '5':
                                if salario_float < 1000 or salario_float > 25000:
                                    continue
                                else:
                                    salario = salario_float
                            elif row['unidadesaláriocódigo'] == '1':  # Hora
                                if row['horascontratuais'] == '':
                                    continue
                                elif int(float(row['horascontratuais'].replace(',', '.'))) < 20:
                                    continue
                                else:  
                                    horas = int(float(row['horascontratuais'].replace(',', '.')))
                                    salario_hora = salario_float * (horas * 4.33)
                                    if salario_hora < 1000 or salario_hora > 25000:
                                        continue
                                    else:
                                        salario = salario_hora
                            elif row['unidadesaláriocódigo'] == '3':  # Semana
                                salario_semanal = salario_float * 4.33
                                if salario_semanal < 1000 or salario_semanal > 25000:
                                    continue
                                else:
                                    salario = salario_semanal
                            elif row['unidadesaláriocódigo'] == '4':  # Quinzena
                                salario_quinzenal = salario_float * 2
                                if salario_quinzenal < 1000 or salario_quinzenal > 25000:
                                    continue
                                else:
                                    salario = salario_quinzenal

                        idade_str = row['idade'].strip()
                    
                        if idade_str != "":
                            idade = int(idade_str)
                            faixa_etaria = determinar_faixa_etaria(idade)
                        
                            subclass_salaries[subclass].append(salario)
                            subclass_idades[subclass].append(idade)
                            cbo_salaries[cbo].append(salario)
                            cbo_idades[cbo].append(idade)
                        
                            subclass_faixa_salaries[subclass][faixa_etaria].append(salario)
                            cbo_faixa_salaries[cbo][faixa_etaria].append(salario)

    return subclass_salaries, subclass_idades, cbo_salaries, cbo_idades, subclass_faixa_salaries, cbo_faixa_salaries, formatted_date

# Função para escrever no arquivo CSV
def escrever_csv(subclass_writer, cbo_writer, subclass_salaries, subclass_idades, cbo_salaries, cbo_idades, subclass_faixa_salaries, cbo_faixa_salaries, formatted_date):
    # Calcular médias salariais e de idade para cada subclasse
    subclass_avg_salary = {subclass: calcular_media(salaries) for subclass, salaries in subclass_salaries.items()}
    subclass_avg_idade = {subclass: calcular_media(idades) for subclass, idades in subclass_idades.items()}

    # Calcular médias salariais para cada subclasse e faixa etária
    for subclass, faixas in subclass_faixa_salaries.items():
        row = {
            'id': subclass,
            'cnae': subclass,
            'media_salarial_geral': f'{subclass_avg_salary[subclass]:.2f}',
            'media_idade_geral': f'{subclass_avg_idade[subclass]:.2f}',
            'date': formatted_date
        }
        for faixa in ['18-29', '30-39', '40-49', '50-59', '60+']:
            salaries = subclass_faixa_salaries[subclass].get(faixa, [])
            avg_salary = calcular_media(salaries)
            row[faixa] = f'{avg_salary:.2f}'
        subclass_writer.writerow(row)

    # Calcular médias salariais e de idade para cada ocupação (cbo2002ocupacao)
    cbo_avg_salary = {cbo: calcular_media(salaries) for cbo, salaries in cbo_salaries.items()}
    cbo_avg_idade = {cbo: calcular_media(idades) for cbo, idades in cbo_idades.items()}

    # Calcular médias salariais para cada ocupação (cbo2002ocupacao) e faixa etária
    for cbo, faixas in cbo_faixa_salaries.items():
        row = {
            'id': cbo,
            'ocupacao': cbo,
            'media_salarial_geral': f'{cbo_avg_salary[cbo]:.2f}',
            'media_idade_geral': f'{cbo_avg_idade[cbo]:.2f}',
            'date': formatted_date
        }
        for faixa in ['18-29', '30-39', '40-49', '50-59', '60+']:
            salaries = cbo_faixa_salaries[cbo].get(faixa, [])
            avg_salary = calcular_media(salaries)
            row[faixa] = f'{avg_salary:.2f}'
        cbo_writer.writerow(row)

# Função principal para processar arquivos em paralelo
def processar_arquivos_em_paralelo():
    print("Iniciando processamento paralelo...")
    output_directory = 'output_caged'
    folder_path = './CAGEDMOV_downloads'
    os.makedirs(output_directory, exist_ok=True)

    output_subclass_csv = f'./{output_directory}/subclasse_output.csv'
    output_cbo_csv = f'./{output_directory}/ocupacoes_output.csv'

    # Inicializar os arquivos CSV de saída
    with open(output_subclass_csv, mode='w', newline='', encoding='utf-8') as subclass_file, \
         open(output_cbo_csv, mode='w', newline='', encoding='utf-8') as cbo_file:

        subclass_fieldnames = ['id', 'cnae', 'media_salarial_geral'] + ['18-29', '30-39', '40-49', '50-59', '60+'] + ['media_idade_geral', 'date']
        cbo_fieldnames = ['id', 'ocupacao', 'media_salarial_geral'] + ['18-29', '30-39', '40-49', '50-59', '60+'] + ['media_idade_geral', 'date']

        subclass_writer = csv.DictWriter(subclass_file, fieldnames=subclass_fieldnames, delimiter=';')
        cbo_writer = csv.DictWriter(cbo_file, fieldnames=cbo_fieldnames, delimiter=';')

        subclass_writer.writeheader()
        cbo_writer.writeheader()

        arquivos = [os.path.join(folder_path, file) for file in os.listdir(folder_path)]
        tasks = []

        for arquivo in arquivos:
            formatted_date = format_string(os.path.basename(arquivo))
            tasks.append(processar_arquivo(arquivo, formatted_date))

        # Coletar os resultados das tarefas paralelizadas
        resultados = dask.compute(*tasks)

        # Consolidar e escrever os resultados no CSV
        for (subclass_salaries, subclass_idades, cbo_salaries, cbo_idades, subclass_faixa_salaries, cbo_faixa_salaries, formatted_date), arquivo in zip(resultados, arquivos):
            escrever_csv(subclass_writer, cbo_writer, subclass_salaries, subclass_idades, cbo_salaries, cbo_idades, subclass_faixa_salaries, cbo_faixa_salaries, formatted_date)

# Função principal do programa
if __name__ == '__main__':
    # Inicializar o cliente Dask
    cluster = LocalCluster(n_workers=6, threads_per_worker=12)
    client = Client()

    # Medir o tempo de execução do processo
    inicio = time.time()
    processar_arquivos_em_paralelo()
    fim = time.time()

    print(f"Tempo de execução: {fim - inicio:.2f} segundos")
