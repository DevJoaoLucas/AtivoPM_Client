import threading
import requests
import subprocess
import time
import win32evtlog
from datetime import datetime
import re

class Application:
    def _init_(self):
        self.mysql_url = "xxxxxxxxxx"
        self.mysql_password = "joao desenvolvimento!"
        self.monitor_thread = None
        self.monitoring = False
        self.read_config()
        self.start_monitoring()

    def read_config(self):
        try:
            with open(r"C:\Program Files (x86)\xxxxxxxxxxxxxxxxx", "r") as f:
                for line in f:
                    if line.startswith("xxxxx="):
                        self.empresa = line.split("=")[1].strip()
                    elif line.startswith("xxxx="):
                        self.mysql_username = line.split("=")[1].strip()
        except Exception as e:
            print(f"Erro ao ler arquivo de configuração: {e}")

    def start_monitoring(self):
        if not hasattr(self, 'empresa') or not hasattr(self, 'mysql_username'):
            print("Dados inválidos! Empresa e usuário MySQL devem ser especificados.")
            return

        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self.monitor_service)
        self.monitor_thread.start()

    def monitor_service(self):
        while self.monitoring:
            log_name = "NomeDoLog..."
            ultimo_evento, hora_evento = self.get_last_log_event(log_name)
            service_status_is = self.get_service_status("yyyyyyyyyy")
            service_status_dg = self.get_service_status("ttttttttt")
            horario_atual = datetime.now()
            horario_formatado = horario_atual.strftime("%d/%m/%Y - %H:%M")
            current_time = horario_formatado

            error_logs, hora_error = self.get_last_5_error_log_events(log_name)
            if error_logs:
                self.insert_statuserros_data("statuserros", hora_error[0], current_time, self.empresa, error_logs[0])
                self.update_statuserros_data("statuserros", hora_error[0], current_time, self.empresa, error_logs[0])

            self.insert_mysql_data("status", service_status_is, service_status_dg, ultimo_evento, hora_evento,
                                   current_time)
            self.update_mysql_data("status", service_status_is, service_status_dg, ultimo_evento, hora_evento,
                                   current_time)

            time.sleep(120)

    def get_service_status(self, service_name):
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

    def get_last_log_event(self, log_name):
        handle = win32evtlog.OpenEventLog(None, log_name)
        flags = win32evtlog.EVENTLOG_BACKWARDS_READ | win32evtlog.EVENTLOG_SEQUENTIAL_READ
        events = []

        try:
            while True:
                raw_events = win32evtlog.ReadEventLog(handle, flags, 0)
                if not raw_events:
                    break
                events.extend(raw_events)
        except Exception as e:
            print(f"Erro ao ler o log: {e}")
        finally:
            win32evtlog.CloseEventLog(handle)

        if events:
            events.sort(key=lambda event: event.TimeGenerated, reverse=True)
            ultimo_evento = events[0].StringInserts[0]
            hora_evento = events[0].TimeGenerated.strftime("%d/%m/%Y - %H:%M")
            return ultimo_evento, hora_evento
        else:
            print("Erro: Nenhum evento encontrado no log.")
            return None, None

    def get_last_5_error_log_events(self, log_name):
        handle = win32evtlog.OpenEventLog(None, log_name)
        flags = win32evtlog.EVENTLOG_BACKWARDS_READ | win32evtlog.EVENTLOG_SEQUENTIAL_READ
        events = []

        try:
            while True:
                raw_events = win32evtlog.ReadEventLog(handle, flags, 0)
                if not raw_events:
                    break
                events.extend(raw_events)
        except Exception as e:
            print(f"Erro ao ler o log: {e}")
        finally:
            win32evtlog.CloseEventLog(handle)

        error_events = []
        for event in events:
            if event.EventType == 1:  # 1 é o código para eventos de erro
                error_events.append(event)

        if error_events:
            error_events.sort(key=lambda event: event.TimeGenerated, reverse=True)
            # Pegar até os últimos 3 eventos de erro
            error_events = error_events[:1]
            event_vars = []
            for error_event in error_events:
                event_vars.append(error_event.StringInserts[0])
            hora_error = [error_event.TimeGenerated for error_event in error_events]
            return tuple(event_vars), hora_error
        else:
            print("Nenhum erro encontrado no Log do Ativo .IS")
            return None, None

    def delete_logs_of_current_company(self, table_name, empresa):
        try:
            mysql_url = "xxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            current_time_str = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            query = f"UPDATE ativopm.{table_name} SET HoraAtualizacao = '{current_time_str}', Empresa = '{empresa}'"
            response = requests.post(self.mysql_url, data={'sql_query': query},
                                     auth=(self.mysql_username, self.mysql_password))
            if response.status_code == 200:
                print(f"Logs da empresa {empresa} atualizados com sucesso.")
            else:
                print(f"Falha ao atualizar logs da empresa {empresa}. Código de status: {response.status_code}")
        except Exception as e:
            print(f"Falha ao atualizar logs da empresa {empresa}: {e}")

    def insert_statuserros_data(self, table_name, hora_error, hora_atualizacao, empresa, status_erro_dw):
        try:
            if isinstance(hora_error, list):
                if isinstance(hora_error[i], str):
                    hora_evento_formatada = datetime.strptime(hora_error[i], "%Y-%m-%d %H:%M:%S").strftime(
                        "%d/%m/%Y - %H:%M")
                else:
                    hora_evento_formatada = hora_error[i].strftime("%d/%m/%Y - %H:%M")
            else:  # Se for apenas uma string, usa diretamente
                if isinstance(hora_error, str):
                    hora_evento_formatada = datetime.strptime(hora_error, "%Y-%m-%d %H:%M:%S").strftime(
                        "%d/%m/%Y - %H:%M")
                else:
                    hora_evento_formatada = hora_error.strftime("%d/%m/%Y - %H:%M")

            status_erro_dw = status_erro_dw.replace("'", "")

            query = f"INSERT INTO ativopm.{table_name} (HoraEvento, HoraAtualizacao, Empresa, StatusErroDW) VALUES ('{hora_evento_formatada}', '{hora_atualizacao}', '{empresa}', '{status_erro_dw}')"
            response = requests.post(self.mysql_url, data={'sql_query': query}, auth=(self.mysql_username, self.mysql_password))
            if response.status_code == 200:
                print("Dados de erros inseridos com sucesso!")
            else:
                print(f"Falha ao inserir dados de erros na tabela: {response.status_code}")
        except Exception as e:
            print(f"Falha ao inserir dados de erros na tabela: {e}")

    def update_statuserros_data(self, table_name, hora_error, hora_atualizacao, empresa, status_erro_dw):
        try:
            if isinstance(hora_error, list):
                if isinstance(hora_error[i], str):
                    hora_evento_formatada = datetime.strptime(hora_error[i], "%Y-%m-%d %H:%M:%S").strftime(
                        "%d/%m/%Y - %H:%M")
                else:
                    hora_evento_formatada = hora_error[i].strftime("%d/%m/%Y - %H:%M")
            else:  # Se for apenas uma string, usa diretamente
                if isinstance(hora_error, str):
                    hora_evento_formatada = datetime.strptime(hora_error, "%Y-%m-%d %H:%M:%S").strftime(
                        "%d/%m/%Y - %H:%M")
                else:
                    hora_evento_formatada = hora_error.strftime("%d/%m/%Y - %H:%M")

            query = f"UPDATE ativopm.{table_name} SET HoraAtualizacao = '{hora_atualizacao}', StatusErroDW = '{status_erro_dw}', HoraEvento = '{hora_evento_formatada}' WHERE Empresa = '{self.empresa}'"
            response = requests.post(self.mysql_url, data={'sql_query': query}, auth=(self.mysql_username, self.mysql_password))
            if response.status_code == 200:
                print("Dados de erros atualizados com sucesso!")
            else:
                print(f"Falha ao atualizar dados na tabela: {response.status_code}")
        except Exception as e:
            print(f"Falha ao atualizar dados na tabela: {e}")

    def insert_mysql_data(self, table_name, status_is, status_dg, ultimo_evento, hora_evento, hora_atualizacao):
        try:
            query = f"INSERT INTO ativopm.{table_name} (StatusIS, StatusDG, StatusDW, HoraEvento, HoraAtualizacao, Empresa) VALUES ('{status_is}', '{status_dg}', '{ultimo_evento}', '{hora_evento}', '{hora_atualizacao}', '{self.empresa}')"
            response = requests.post(self.mysql_url, data={'sql_query': query}, auth=(self.mysql_username, self.mysql_password))
            if response.status_code == 200:
                print("Dados inseridos com sucesso!")
            else:
                print(f"Falha ao inserir dados na tabela: {response.status_code}")
        except Exception as e:
            print(f"Falha ao inserir dados na tabela: {e}")

    def update_mysql_data(self, table_name, status_is, status_dg, ultimo_evento, hora_evento, hora_atualizacao):
        try:
            query = f"UPDATE ativopm.{table_name} SET StatusIS = '{status_is}', StatusDG = '{status_dg}', StatusDW = '{ultimo_evento}', HoraEvento = '{hora_evento}', HoraAtualizacao = '{hora_atualizacao}' WHERE Empresa = '{self.empresa}'"
            response = requests.post(self.mysql_url, data={'sql_query': query}, auth=(self.mysql_username, self.mysql_password))
            if response.status_code == 200:
                print("Dados atualizados com sucesso!")
            else:
                print(f"Falha ao atualizar dados na tabela: {response.status_code}")
        except Exception as e:
            print(f"Falha ao atualizar dados na tabela: {e}")

def main():
    app = Application()

if _name_ == "_main_":
    main()
