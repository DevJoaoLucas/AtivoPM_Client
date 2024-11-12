import threading
import subprocess
import time
import win32evtlog
from datetime import datetime
from datetime import timedelta
import re
import os
import sys
import schedule
from multiprocessing import Process
from openpyxl import load_workbook
import msal
import requests

class Application:
    def __init__(self):
        self.mysql_password = "xxxxxxxxxxxx"
        self.monitor_thread = None
        self.monitoring = False
        self.read_config()
        self.start_monitoring()

    def read_config(self):
        try:
            with open(r"C:\xxxxxxxxxxxxxx\yyyyyyyyyyyyy.txt", "r") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("Empresa="):
                        self.empresa = line.split("=", 1)[1].strip()
                    elif line.startswith("Usuario="):
                        self.mysql_username = line.split("=", 1)[1].strip()
                    elif line.startswith("Ambiente="):
                        self.ambiente = line.split("=", 1)[1].strip()
                    elif line.startswith("Endereco="):
                        self.mysql_url = line.split("=", 1)[1].strip()
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
            log_name = "LogLog"
            ultimo_evento, hora_evento = self.get_last_log_event(log_name)
            service_status_is = self.get_service_status("xxxxxService")
            service_status_dg = self.get_service_status("YyyyyyyyService")
            horario_atual = datetime.now()
            horario_formatado = horario_atual.strftime("%d/%m/%Y - %H:%M")
            current_time = horario_formatado

            if self.ambiente == "BI":
                error_logs, hora_error = self.get_last_5_error_log_events(log_name)
                if error_logs:
                    self.insert_statuserros_data("statuserros", hora_error[0], current_time, self.empresa,
                                                 error_logs[0])
                    self.update_statuserros_data("statuserros", hora_error[0], current_time, self.empresa,
                                                 error_logs[0])

                self.insert_mysql_data("status", service_status_is, service_status_dg, ultimo_evento, hora_evento,
                                       current_time)
                self.update_mysql_data("status", service_status_is, service_status_dg, ultimo_evento, hora_evento,
                                       current_time)

                statusultimaatualizacaogateway, HoraTermino_formatado, gatewayStatusAtual = self.get_gateway_status()
                if statusultimaatualizacaogateway:
                    self.insert_gateway_data("statusgateway", statusultimaatualizacaogateway, HoraTermino_formatado,
                                             current_time, gatewayStatusAtual)
                    self.update_gateway_data("statusgateway", statusultimaatualizacaogateway, HoraTermino_formatado,
                                             current_time, gatewayStatusAtual)

            elif self.ambiente == "ERP10":
                statusultimaatualizacaogateway, HoraTermino_formatado, gatewayStatusAtual = self.get_gateway_status()
                if statusultimaatualizacaogateway:
                    self.insert_gateway_data("statusgateway", statusultimaatualizacaogateway, HoraTermino_formatado,
                                             current_time, gatewayStatusAtual)
                    self.update_gateway_data("statusgateway", statusultimaatualizacaogateway, HoraTermino_formatado,
                                             current_time, gatewayStatusAtual)

            elif self.ambiente == "REPORTING":
                error_logs, hora_error = self.get_last_5_error_log_events(log_name)
                if error_logs:
                    self.insert_statuserros_data("statuserros", hora_error[0], current_time, self.empresa,
                                                 error_logs[0])
                    self.update_statuserros_data("statuserros", hora_error[0], current_time, self.empresa,
                                                 error_logs[0])
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
            if event.EventType == 1:
                error_events.append(event)

        if error_events:
            error_events.sort(key=lambda event: event.TimeGenerated, reverse=True)

            error_events = error_events[:1]
            event_vars = []
            for error_event in error_events:
                event_vars.append(error_event.StringInserts[0])
            hora_error = [error_event.TimeGenerated for error_event in error_events]
            return tuple(event_vars), hora_error
        else:
            print("Nenhum erro encontrado no Log do Serviço XXXXXXXXXXX")
            return None, None

    def delete_logs_of_current_company(self, table_name, empresa):
        try:
            with open(r"C:\xxxxxxxxxxxxxxxxxx\yyyyyyyyyyyyyyyyyyy.txt", "r") as erro:
                for line in erro:
                    line = line.strip()
                    if line.startswith("EnderecoErro="):
                        self.mysql_url = line.split("=", 1)[1].strip()
            current_time_str = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            query = f"UPDATE `ativopm`.`{table_name}` SET `HoraAtualizacao` = '{current_time_str}', `Empresa` = '{empresa}'"
            response = requests.post(self.mysql_url, data={'sql_query': query},
                                     auth=(self.mysql_username, self.mysql_password))
            if response.status_code == 200:
                print(f"Logs da empresa {empresa} atualizados com sucesso.")
            else:
                print(f"Falha ao atualizar logs da empresa {empresa}. Código de status: {response.status_code}")
        except Exception as e:
            print(f"Falha ao atualizar logs da empresa {empresa}: {e}")

    def insert_statuserros_data(self, table_name, hora_error, hora_atualizacao, empresa, status_erro_dw):
        while True:
            try:
                if isinstance(hora_error, list):
                    if isinstance(hora_error[i], str):
                        hora_evento_formatada = datetime.strptime(hora_error[i], "%Y-%m-%d %H:%M:%S").strftime(
                            "%d/%m/%Y - %H:%M")
                    else:
                        hora_evento_formatada = hora_error[i].strftime("%d/%m/%Y - %H:%M")
                else:
                    if isinstance(hora_error, str):
                        hora_evento_formatada = datetime.strptime(hora_error, "%Y-%m-%d %H:%M:%S").strftime(
                            "%d/%m/%Y - %H:%M")
                    else:
                        hora_evento_formatada = hora_error.strftime("%d/%m/%Y - %H:%M")

                status_erro_dw = status_erro_dw.replace("'", "")

                query = f"INSERT INTO `ativopm`.`{table_name}` (`HoraEvento`, `HoraAtualizacao`, `Empresa`, `StatusErroDW`) VALUES ('{hora_evento_formatada}', '{hora_atualizacao}', '{empresa}', '{status_erro_dw}')"
                response = requests.post(self.mysql_url, data={'sql_query': query}, auth=(self.mysql_username, self.mysql_password))
                if response.status_code == 200:
                    print("Dados de erros inseridos com sucesso!")
                    break
                else:
                    print(f"Falha ao inserir dados de erros na tabela: {response.status_code}")
            except Exception as e:
                print(f"Falha ao inserir dados de erros na tabela: {e}")
                print("Tentando novamente em 10 segundos...")
                time.sleep(10)

    def update_statuserros_data(self, table_name, hora_error, hora_atualizacao, empresa, status_erro_dw):
        while True:
            try:
                if isinstance(hora_error, list):
                    if isinstance(hora_error[i], str):
                        hora_evento_formatada = datetime.strptime(hora_error[i], "%Y-%m-%d %H:%M:%S").strftime(
                            "%d/%m/%Y - %H:%M")
                    else:
                        hora_evento_formatada = hora_error[i].strftime("%d/%m/%Y - %H:%M")
                else:
                    if isinstance(hora_error, str):
                        hora_evento_formatada = datetime.strptime(hora_error, "%Y-%m-%d %H:%M:%S").strftime(
                            "%d/%m/%Y - %H:%M")
                    else:
                        hora_evento_formatada = hora_error.strftime("%d/%m/%Y - %H:%M")

                query = f"UPDATE `ativopm`.`{table_name}` SET `HoraAtualizacao` = '{hora_atualizacao}', `StatusErroDW` = '{status_erro_dw}', `HoraEvento` = '{hora_evento_formatada}' WHERE `Empresa` = '{self.empresa}'"
                response = requests.post(self.mysql_url, data={'sql_query': query}, auth=(self.mysql_username, self.mysql_password))
                if response.status_code == 200:
                    print("Dados de erros atualizados com sucesso!")
                    break
                else:
                    print(f"Falha ao atualizar dados na tabela: {response.status_code}")
            except Exception as e:
                print(f"Falha ao atualizar dados na tabela: {e}")
                print("Tentando novamente em 10 segundos...")
                time.sleep(10)

    def insert_mysql_data(self, table_name, status_is, status_dg, ultimo_evento, hora_evento, hora_atualizacao):
        while True:
            try:

                if not hasattr(self, 'ambiente'):
                    self.read_config()

                if self.ambiente == "REPORTING":
                    status_dg_value = "REPORTING"
                    status_is_value = status_is
                elif self.ambiente == "ERP10":
                    status_is_value = "ERP10"
                    status_dg_value = status_dg
                    ultimo_evento = "ERP10"
                    hora_evento = "ERP10"
                else:
                    status_dg_value = status_dg
                    status_is_value = status_is

                query = f"INSERT INTO `ativopm`.`{table_name}` (`StatusIS`, `StatusDG`, `StatusDW`, `HoraEvento`, `HoraAtualizacao`, `Empresa`) VALUES ('{status_is_value}', '{status_dg_value}', '{ultimo_evento}', '{hora_evento}', '{hora_atualizacao}', '{self.empresa}')"
                response = requests.post(self.mysql_url, data={'sql_query': query},
                                         auth=(self.mysql_username, self.mysql_password))
                if response.status_code == 200:
                    print("Dados inseridos com sucesso!")
                    break
                else:
                    print(f"Falha ao inserir dados. Código de status: {response.status_code}")
                    time.sleep(10)
            except requests.exceptions.RequestException as e:
                print(f"Falha ao inserir dados: {e}")
                print("Tentando novamente em 10 segundos...")
                time.sleep(10)

    def update_mysql_data(self, table_name, status_is, status_dg, ultimo_evento, hora_evento, hora_atualizacao):
        while True:
            try:

                if not hasattr(self, 'ambiente'):
                    self.read_config()

                if self.ambiente == "REPORTING":
                    status_dg_value = "REPORTING"
                    status_is_value = status_is
                elif self.ambiente == "ERP10":
                    status_is_value = "ERP10"
                    status_dg_value = status_dg
                    ultimo_evento = "ERP10"
                    hora_evento = "ERP10"
                else:
                    status_dg_value = status_dg
                    status_is_value = status_is

                query = f"UPDATE `ativopm`.`{table_name}` SET `StatusIS` = '{status_is_value}', `StatusDG` = '{status_dg_value}', `StatusDW` = '{ultimo_evento}', `HoraEvento` = '{hora_evento}', `HoraAtualizacao` = '{hora_atualizacao}' WHERE `Empresa` = '{self.empresa}'"
                response = requests.post(self.mysql_url, data={'sql_query': query},
                                         auth=(self.mysql_username, self.mysql_password))
                if response.status_code == 200:
                    print("Dados atualizados com sucesso!")
                    break
                else:
                    print(f"Falha ao inserir dados. Código de status: {response.status_code}")
                    time.sleep(10)
            except requests.exceptions.RequestException as e:
                print(f"Falha ao inserir dados: {e}")
                print("Tentando novamente em 10 segundos...")
                time.sleep(10)

    def get_gateway_status(self):
        try:
            def get_credentials(file_path):
                username = password = idGroup = idDataSet = idGateway = app_id = tenant_id = None

                if os.path.exists(file_path):
                    with open(file_path, 'r') as file:
                        for line in file:
                            if line.startswith('EmailBI='):
                                username = line.split('=')[1].strip()
                            elif line.startswith('SenhaBI='):
                                password = line.split('=')[1].strip()
                            elif line.startswith('idGroup='):
                                idGroup = line.split('=')[1].strip()
                            elif line.startswith('idDataSet='):
                                idDataSet = line.split('=')[1].strip()
                            elif line.startswith('idGateway='):
                                idGateway = line.split('=')[1].strip()
                            elif line.startswith('app_id='):
                                app_id = line.split('=')[1].strip()
                            elif line.startswith('tenant_id='):
                                tenant_id = line.split('=')[1].strip()

                return username, password, idGroup, idDataSet, idGateway, app_id, tenant_id

            config_path = r"C:\xxxxxxxxxxxxxxxxxxxxxxxxxx\yyyyyyyyyyyyyyyyyy.txt"
            username, password, idGroup, idDataSet, idGateway, app_id, tenant_id = get_credentials(config_path)

            if not username or not password:
                raise ValueError("Não foi possível obter as credenciais do arquivo de configuração.")

            authority_url = 'https://login.microsoftonline.com/' + tenant_id
            scopes = ['https://analysis.windows.net/powerbi/api/.default']

            client = msal.PublicClientApplication(app_id, authority=authority_url)
            response = client.acquire_token_by_username_password(username=username, password=password, scopes=scopes)

            if 'access_token' not in response:
                raise Exception(
                    "Falha ao obter o token de acesso: " + response.get('error_description', 'Sem descrição do erro.'))

            access_token = response['access_token']

            endpoint = f'https://api.powerbi.com/v1.0/myorg/groups/{idGroup}/datasets/{idDataSet}/refreshes?$top=1'
            headers = {'Authorization': f'Bearer {access_token}'}
            response_requests = requests.get(endpoint, headers=headers)

            if response_requests.status_code == 200:
                data = response_requests.json()
                refresh = data['value'][0]
                HoraTermino = refresh.get('endTime', '')
                statusultimaatualizacaogateway = refresh.get('status', 'Unknown')
            else:
                raise Exception(f"Erro na requisição: {response_requests.status_code} - {response_requests.text}")
            if HoraTermino:
                end_time = datetime.strptime(HoraTermino, "%Y-%m-%dT%H:%M:%S.%fZ")
                end_time_adjusted = end_time - timedelta(hours=4)
                HoraTermino_formatado = end_time_adjusted.strftime("%d/%m/%Y - %H:%M")
            else:
                HoraTermino_formatado = ''

            endpoint = f'https://api.powerbi.com/v1.0/myorg/gateways/{idGateway}'
            response_requests = requests.get(endpoint, headers=headers)

            if response_requests.status_code == 200:
                data = response_requests.json()
                gatewayStatusAtual = data['gatewayStatus']
            else:
                raise Exception(f"Erro na requisição: {response_requests.status_code} - {response_requests.text}")

            return statusultimaatualizacaogateway, HoraTermino_formatado, gatewayStatusAtual
        except Exception as e:
            print(f"Erro ao obter status do gateway: {e}")
            return None, None, None

    def insert_gateway_data(self, table_name, statusultimaatualizacaogateway, HoraTermino_formatado, hora_atualizacao, gatewayStatusAtual):
        while True:
            try:
                print("Valores a serem inseridos:")
                print(f"Empresa: {self.empresa}")
                print(f"StatusGateway: {gatewayStatusAtual}")
                print(f"HoraUltimaAtGateway: {HoraTermino_formatado}")
                print(f"Status última Atualizacao: {statusultimaatualizacaogateway}")
                print(f"HoraAtual: {hora_atualizacao}")

                query = f"INSERT INTO `ativopm`.`{table_name}` (`Empresa`, `StatusGateway`, `HoraUltimaAtGateway`, `HoraAtual`, `StatusUltimaAtualizacaoGateway` ) VALUES ('{self.empresa}', '{gatewayStatusAtual}', '{HoraTermino_formatado}', '{hora_atualizacao}', '{statusultimaatualizacaogateway}')"
                response = requests.post(self.mysql_url, data={'sql_query': query},
                                         auth=(self.mysql_username, self.mysql_password))
                if response.status_code == 200:
                    print("Dados inseridos com sucesso!")
                    break
                else:
                    print(f"Falha ao inserir dados. Código de status: {response.status_code}")
                    time.sleep(10)
            except requests.exceptions.RequestException as e:
                print(f"Falha ao inserir dados: {e}")
                print("Tentando novamente em 10 segundos...")
                time.sleep(10)

    def update_gateway_data(self, table_name, statusultimaatualizacaogateway, HoraTermino_formatado, hora_atualizacao, gatewayStatusAtual):
        while True:
            try:
                print("Valores a serem atualizados:")
                print(f"Empresa: {self.empresa}")
                print(f"StatusGateway: {gatewayStatusAtual}")
                print(f"HoraUltimaAtGateway: {HoraTermino_formatado}")
                print(f"Status última Atualizacao: {statusultimaatualizacaogateway}")
                print(f"HoraAtual: {hora_atualizacao}")

                query = f"UPDATE `ativopm`.`{table_name}` SET `StatusGateway` = '{gatewayStatusAtual}', `HoraUltimaAtGateway` = '{HoraTermino_formatado}', `StatusUltimaAtualizacaoGateway` = '{statusultimaatualizacaogateway}', `HoraAtual` = '{hora_atualizacao}' WHERE `Empresa` = '{self.empresa}'"
                response = requests.post(self.mysql_url, data={'sql_query': query},
                                         auth=(self.mysql_username, self.mysql_password))

                if response.status_code == 200:
                    print("Dados atualizados com sucesso!")
                    break
                else:
                    print(f"Falha ao atualizar dados. Código de status: {response.status_code}")
                    time.sleep(10)
            except requests.exceptions.RequestException as e:
                print(f"Falha ao atualizar dados: {e}")
                print("Tentando novamente em 10 segundos...")
                time.sleep(10)


def main():
    app = Application()

if __name__ == "__main__":
    main()
