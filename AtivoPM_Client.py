import requests
import subprocess
import time

# Função para obter o status do serviço do Windows
def get_service_status(service_name):
    try:
        output = subprocess.check_output(['sc', 'query', service_name], shell=True)
        status_lines = [line.strip() for line in output.decode('utf-8', errors='ignore').split('\n') if 'ESTADO' in line]
        if status_lines:
            status_info = status_lines[0].split(':')[1].strip().split()[0]
            status_dict = {"1": "PARADO", "2": "INICIALIZANDO", "3": "PARANDO", "4": "EM EXECUÇÃO"}
            return status_dict.get(status_info, "DESCONHECIDO")
        else:
            return "DESCONHECIDO"
    except Exception as e:
        print(f"Erro ao obter status do serviço {service_name}: {e}")
        return "DESCONHECIDO"

# Função para inserir dados na tabela do MySQL
def insert_mysql_data(url, username, password, db_name, table_name, servico, status, hora, empresa):
    try:
        query = f"INSERT INTO `{db_name}`.`{table_name}` (`Servico`, `Status`, `Hora`, `Empresa`) VALUES ('{servico}', '{status}', '{hora}', '{empresa}')"
        response = requests.post(url, data={'sql_query': query}, auth=(username, password))
        if response.status_code == 200:
            print("Dados inseridos com sucesso!")
        else:
            print(f"Falha ao inserir dados na tabela: {response.status_code}")
    except Exception as e:
        print(f"Falha ao inserir dados na tabela: {e}")

# Função para excluir dados antigos para uma empresa específica
def delete_old_data(url, username, password, db_name, table_name, empresa):
    try:
        query = f"DELETE FROM `{db_name}`.`{table_name}` WHERE `Empresa` = '{empresa}'"
        response = requests.post(url, data={'sql_query': query}, auth=(username, password))
        if response.status_code == 200:
            print("Dados antigos excluídos com sucesso!")
        else:
            print(f"Falha ao excluir dados antigos: {response.status_code}")
    except Exception as e:
        print(f"Falha ao excluir dados antigos: {e}")

# URL do servidor MySQL e credenciais
mysql_url = "https://ativobi.loca.lt/phpmyadmin/index.php?route=/sql&db=ativopm&table=status"
mysql_username = "root"
mysql_password = ""

# Nome do serviço do Windows
service_name = "TimeBrokerSvc"

# Empresa (você pode alterar para o valor desejado)
empresa = "EmpresaXYZ"

# Loop infinito para verificar periodicamente o status do serviço e inserir os dados na tabela
while True:
    # Obter status do serviço
    service_status = get_service_status(service_name)

    # Obter hora atual
    current_time = time.strftime("%H:%M:%S")

    # Excluir dados antigos para a empresa específica
    delete_old_data(mysql_url, mysql_username, mysql_password, "ativopm", "status", empresa)

    # Inserir novos dados na tabela do MySQL
    insert_mysql_data(mysql_url, mysql_username, mysql_password, "ativopm", "status", service_name, service_status, current_time, empresa)

    # Aguardar 10 segundos antes de verificar novamente
    time.sleep(10)