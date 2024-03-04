import subprocess

# Função para obter o status do serviço
def get_service_status(service_name):
    try:
        output = subprocess.check_output(['sc', 'query', service_name]).decode('utf-8', errors='ignore')
        status_line = [line.strip() for line in output.split('\n') if 'ESTADO' in line]
        if status_line:
            status = status_line[0].split(':')[1].strip()
            return status
        else:
            return "Desconhecido"
    except Exception as e:
        print(f"Erro ao obter status do serviço {service_name}: {e}")
        return "Erro"

# Teste da função
service_name = "TimeBrokerSvc"
status = get_service_status(service_name)
print(f"Status do serviço {service_name}: {status}")