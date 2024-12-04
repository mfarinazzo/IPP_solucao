from mpi4py import MPI
import csv
from collections import defaultdict, Counter
import os
import time

# Inicialização do MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

log_file = 'log_CAGEDERRORS.txt'

def registrar_log(mensagem):
    # Apenas o processo mestre escreve no log
    if rank == 0:
        with open(log_file, 'a') as f:
            f.write(mensagem + '\n')

output_directory = 'output_caged'
folder_path = './CAGEDMOV_downloads'

if rank == 0:
    os.makedirs(output_directory, exist_ok=True)

# Sincronizar todos os processos após criar o diretório
comm.Barrier()

# Inicializar contadores para subclasses e ocupações
# Cada processo terá seus próprios contadores parciais
subclass_count = Counter()
cbo_count = Counter()

# Faixas etárias
faixas_etarias = {
    '18-29': (18, 29),
    '30-39': (30, 39),
    '40-49': (40, 49),
    '50-59': (50, 59),
    '60+': (60, 200)  # 200 é um valor arbitrário para representar 60+
}

# Função para formatar a string do nome do arquivo e obter a data formatada
def format_string(input_str):
    if len(input_str) < 6:
        return "A string deve ter pelo menos 6 caracteres."
   
    last_six = input_str[-10:]
    year = last_six[:4]
    month = last_six[4:-4]
    formatted_date = f"{year}-{month}-01"
   
    return formatted_date

# Função para calcular média
def calcular_media(valores):
    if valores:
        return sum(valores) / len(valores)
    else:
        return 0.0

# Função para determinar a faixa etária
def determinar_faixa_etaria(idade):
    for faixa, (min_idade, max_idade) in faixas_etarias.items():
        if min_idade <= idade <= max_idade:
            return faixa
    return None

# Função para processar um arquivo individualmente
def processar_arquivo(csv_file_path, formatted_date):
    local_subclass_count = Counter()
    local_cbo_count = Counter()
    
    subclass_salaries = defaultdict(list)
    subclass_idades = defaultdict(list)
    cbo_salaries = defaultdict(list)
    cbo_idades = defaultdict(list)
    
    subclass_faixa_salaries = defaultdict(lambda: defaultdict(list))
    cbo_faixa_salaries = defaultdict(lambda: defaultdict(list))
    
    try:
        with open(csv_file_path, mode='r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
        
            required_fields = ['subclasse', 'cbo2002ocupação', 'salário', 'idade', 'saldomovimentação', 'unidadesaláriocódigo', 'horascontratuais']
            if not all(field in reader.fieldnames for field in required_fields):
                registrar_log(f"Erro: As colunas necessárias não foram encontradas no arquivo CSV: {os.path.basename(csv_file_path)}")
                return None  # Retorna None para indicar falha no processamento
        
            for row in reader:
                try:
                    if row['saldomovimentação'] != '1':
                        continue
                    
                    subclass = row['subclasse'].strip()
                    cbo = row['cbo2002ocupação'].strip()
                    
                    if row['salário'] == '':
                        continue
                    
                    salario_float = float(row['salário'].replace(',', '.'))
                    
                    # Filtragem baseada no 'unidadesaláriocódigo'
                    unidade_codigo = row['unidadesaláriocódigo']
                    if unidade_codigo in ['99', '6', '7']:
                        continue
                    elif unidade_codigo == '5':
                        if salario_float < 1000 or salario_float > 25000:
                            continue
                        else:
                            salario = salario_float
                    elif unidade_codigo == '1':  # Hora
                        horas_str = row.get('horascontratuais', '').replace(',', '.')
                        if horas_str == '':
                            continue
                        horas = int(float(horas_str))
                        if horas < 20:
                            continue
                        salario_hora = salario_float * (horas * 4.33)
                        if salario_hora < 1000 or salario_hora > 25000:
                            continue
                        salario = salario_hora
                    elif unidade_codigo == '3':  # Semana
                        salario_semanal = salario_float * 4.33
                        if salario_semanal < 1000 or salario_semanal > 25000:
                            continue
                        salario = salario_semanal
                    elif unidade_codigo == '4':  # Quinzena
                        salario_quinzenal = salario_float * 2
                        if salario_quinzenal < 1000 or salario_quinzenal > 25000:
                            continue
                        salario = salario_quinzenal
                    else:
                        continue  # Ignora códigos desconhecidos
                    
                    idade_str = row['idade'].strip()
                    if idade_str == "":
                        continue
                    idade = int(idade_str)
                    faixa_etaria = determinar_faixa_etaria(idade)
                    if faixa_etaria is None:
                        continue  # Idade fora das faixas definidas
                    
                    # Atualizar contadores
                    local_subclass_count[subclass] += 1
                    local_cbo_count[cbo] += 1
                    
                    # Acumular salários e idades
                    subclass_salaries[subclass].append(salario)
                    subclass_idades[subclass].append(idade)
                    cbo_salaries[cbo].append(salario)
                    cbo_idades[cbo].append(idade)
                    
                    # Acumular salários por faixa etária
                    subclass_faixa_salaries[subclass][faixa_etaria].append(salario)
                    cbo_faixa_salaries[cbo][faixa_etaria].append(salario)
                    
                except Exception as e:
                    registrar_log(f"Erro ao processar linha no arquivo {os.path.basename(csv_file_path)}: {e}")
                    continue  # Continua com a próxima linha
        
        # Retornar os dados processados
        return {
            'subclass_count': local_subclass_count,
            'cbo_count': local_cbo_count,
            'subclass_salaries': subclass_salaries,
            'subclass_idades': subclass_idades,
            'cbo_salaries': cbo_salaries,
            'cbo_idades': cbo_idades,
            'subclass_faixa_salaries': subclass_faixa_salaries,
            'cbo_faixa_salaries': cbo_faixa_salaries,
            'formatted_date': formatted_date
        }
    
    except Exception as e:
        registrar_log(f"Erro ao abrir arquivo {os.path.basename(csv_file_path)}: {e}")
        return None

# Processo mestre coleta a lista de arquivos
if rank == 0:
    try:
        all_entries = [entry.name for entry in os.scandir(folder_path) if entry.is_file()]
    except Exception as e:
        registrar_log(f"Erro ao listar arquivos no diretório {folder_path}: {e}")
        all_entries = []
else:
    all_entries = None

# Distribuir a lista de arquivos para todos os processos
all_entries = comm.bcast(all_entries, root=0)

# Dividir os arquivos entre os processos
files_per_process = len(all_entries) // size
remainder = len(all_entries) % size

if rank < remainder:
    start = rank * (files_per_process + 1)
    end = start + files_per_process + 1
else:
    start = rank * files_per_process + remainder
    end = start + files_per_process

local_files = all_entries[start:end]

# Cada processo processa seus arquivos locais
local_results = []
for filename in local_files:
    csv_file_path = os.path.join(folder_path, filename)
    formatted_date = format_string(filename)
    resultado = processar_arquivo(csv_file_path, formatted_date)
    if resultado:
        local_results.append(resultado)

# Função para combinar resultados parciais
def combinar_counters(counter1, counter2):
    for key, value in counter2.items():
        counter1[key] += value

def combinar_dict_of_dicts(dict1, dict2):
    for key, subdict in dict2.items():
        for subkey, values in subdict.items():
            dict1[key][subkey].extend(values)

# Agregação dos resultados locais
# Inicializar estruturas de dados para agregação
agg_subclass_count = Counter()
agg_cbo_count = Counter()
agg_subclass_salaries = defaultdict(list)
agg_subclass_idades = defaultdict(list)
agg_cbo_salaries = defaultdict(list)
agg_cbo_idades = defaultdict(list)
agg_subclass_faixa_salaries = defaultdict(lambda: defaultdict(list))
agg_cbo_faixa_salaries = defaultdict(lambda: defaultdict(list))

# Agregar os resultados locais
for res in local_results:
    combinar_counters(agg_subclass_count, res['subclass_count'])
    combinar_counters(agg_cbo_count, res['cbo_count'])
    
    for subclass, salaries in res['subclass_salaries'].items():
        agg_subclass_salaries[subclass].extend(salaries)
    for subclass, idades in res['subclass_idades'].items():
        agg_subclass_idades[subclass].extend(idades)
    for cbo, salaries in res['cbo_salaries'].items():
        agg_cbo_salaries[cbo].extend(salaries)
    for cbo, idades in res['cbo_idades'].items():
        agg_cbo_idades[cbo].extend(idades)
    
    combinar_dict_of_dicts(agg_subclass_faixa_salaries, res['subclass_faixa_salaries'])
    combinar_dict_of_dicts(agg_cbo_faixa_salaries, res['cbo_faixa_salaries'])

import pickle

# Serializar os dados
serialized_data = pickle.dumps({
    'subclass_count': agg_subclass_count,
    'cbo_count': agg_cbo_count,
    'subclass_salaries': dict(agg_subclass_salaries),
    'subclass_idades': dict(agg_subclass_idades),
    'cbo_salaries': dict(agg_cbo_salaries),
    'cbo_idades': dict(agg_cbo_idades),
    'subclass_faixa_salaries': {k: dict(v) for k, v in agg_subclass_faixa_salaries.items()},
    'cbo_faixa_salaries': {k: dict(v) for k, v in agg_cbo_faixa_salaries.items()},
})

# Recolher os dados serializados no processo mestre
all_serialized_data = comm.gather(serialized_data, root=0)

if rank == 0:
    import collections
    
    # Inicializar estruturas de dados finais
    final_subclass_count = Counter()
    final_cbo_count = Counter()
    final_subclass_salaries = defaultdict(list)
    final_subclass_idades = defaultdict(list)
    final_cbo_salaries = defaultdict(list)
    final_cbo_idades = defaultdict(list)
    final_subclass_faixa_salaries = defaultdict(lambda: defaultdict(list))
    final_cbo_faixa_salaries = defaultdict(lambda: defaultdict(list))
    
    # Agregar todos os dados recebidos
    for serialized in all_serialized_data:
        data = pickle.loads(serialized)
        combinar_counters(final_subclass_count, data['subclass_count'])
        combinar_counters(final_cbo_count, data['cbo_count'])
        
        for subclass, salaries in data['subclass_salaries'].items():
            final_subclass_salaries[subclass].extend(salaries)
        for subclass, idades in data['subclass_idades'].items():
            final_subclass_idades[subclass].extend(idades)
        for cbo, salaries in data['cbo_salaries'].items():
            final_cbo_salaries[cbo].extend(salaries)
        for cbo, idades in data['cbo_idades'].items():
            final_cbo_idades[cbo].extend(idades)
        
        for subclass, faixas in data['subclass_faixa_salaries'].items():
            for faixa, salaries in faixas.items():
                final_subclass_faixa_salaries[subclass][faixa].extend(salaries)
        for cbo, faixas in data['cbo_faixa_salaries'].items():
            for faixa, salaries in faixas.items():
                final_cbo_faixa_salaries[cbo][faixa].extend(salaries)

    # Gerar arquivo CSV para ocupações (CBO2002) e Cnaes
    output_subclass_csv = f'./{output_directory}/subclasse_output.csv'
    output_cbo_csv = f'./{output_directory}/ocupacoes_output.csv'
    
    def calcular_media(valores):
        if valores:
            return sum(valores) / len(valores)
        else:
            return 0.0

    with open(output_subclass_csv, mode='w', newline='', encoding='utf-8') as subclass_file, \
         open(output_cbo_csv, mode='w', newline='', encoding='utf-8') as cbo_file:
         
        subclass_fieldnames = ['id', 'cnae', 'media_salarial_geral'] + list(faixas_etarias.keys()) + ['media_idade_geral', 'date']
        cbo_fieldnames = ['id', 'ocupacao', 'media_salarial_geral'] + list(faixas_etarias.keys()) + ['media_idade_geral', 'date']
       
        subclass_writer = csv.DictWriter(subclass_file, fieldnames=subclass_fieldnames, delimiter=';')
        cbo_writer = csv.DictWriter(cbo_file, fieldnames=cbo_fieldnames, delimiter=';')
    
        subclass_writer.writeheader()
        cbo_writer.writeheader()
    
        subclass_id = 1
        cbo_id = 1
    
        # Calcular médias salariais e de idade para cada subclasse
        subclass_avg_salary = {subclass: calcular_media(salaries) for subclass, salaries in final_subclass_salaries.items()}
        subclass_avg_idade = {subclass: calcular_media(idades) for subclass, idades in final_subclass_idades.items()}
    
        # Calcular médias salariais para cada subclasse e faixa etária
        for subclass, faixas in final_subclass_faixa_salaries.items():
            row = {
                'id': subclass_id,
                'cnae': subclass,
                'media_salarial_geral': f'{subclass_avg_salary[subclass]:.2f}',
                'media_idade_geral': f'{subclass_avg_idade[subclass]:.2f}',
            }
            subclass_id += 1
            for faixa in faixas_etarias.keys():
                salaries = faixas.get(faixa, [])
                avg_salary = calcular_media(salaries)
                row[faixa] = f'{avg_salary:.2f}'
            subclass_writer.writerow(row)

    with open(output_cbo_csv, mode='w', newline='', encoding='utf-8') as cbo_file:
        cbo_fieldnames = ['id', 'ocupacao', 'media_salarial_geral'] + list(faixas_etarias.keys()) + ['media_idade_geral', 'date']
        cbo_writer = csv.DictWriter(cbo_file, fieldnames=cbo_fieldnames, delimiter=';')
        cbo_writer.writeheader()
    
        cbo_id = 1
    
        # Calcular médias salariais e de idade para cada ocupação (cbo2002ocupacao)
        cbo_avg_salary = {cbo: calcular_media(salaries) for cbo, salaries in final_cbo_salaries.items()}
        cbo_avg_idade = {cbo: calcular_media(idades) for cbo, idades in final_cbo_idades.items()}
    
        # Calcular médias salariais para cada ocupação (cbo2002ocupacao) e faixa etária
        for cbo, faixas in final_cbo_faixa_salaries.items():
            row = {
                'id': cbo_id,
                'ocupacao': cbo,
                'media_salarial_geral': f'{cbo_avg_salary[cbo]:.2f}',
                'media_idade_geral': f'{cbo_avg_idade[cbo]:.2f}',
            }
            cbo_id += 1
            for faixa in faixas_etarias.keys():
                salaries = faixas.get(faixa, [])
                avg_salary = calcular_media(salaries)
                row[faixa] = f'{avg_salary:.2f}'
            cbo_writer.writerow(row)

    print(f'Arquivo CSV de subclasse gerado com sucesso: {output_subclass_csv}')
    print(f'Arquivo CSV de ocupações gerado com sucesso: {output_cbo_csv}')
    
    total_subclasses = len(final_subclass_count)
    total_ocupacoes = len(final_cbo_count)
    print(f"\nTotal de Subclasses: {total_subclasses}")
    print(f"Total de Ocupações (CBO2002): {total_ocupacoes}")

if rank == 0:
    tempo_inicio = time.time()

comm.Barrier()  # Sincronizar antes de começar a medir
if rank == 0:
    tempo_inicio = time.time()

comm.Barrier()  # Todos os processos chegaram aqui
if rank == 0:
    tempo_fim = time.time()
    tempo_execucao = tempo_fim - tempo_inicio
    print(f"Tempo de execução: {tempo_execucao:.2f} segundos")
