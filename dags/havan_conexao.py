# -*- coding: utf-8 -*-
import psycopg2
import psycopg2.extras
import pymssql
import requests
import urllib
import sys
import ftplib
from botocore.client import Config
import boto3
import redis
import json

from adal import AuthenticationContext
from airflow.hooks.base_hook import BaseHook
import olap.xmla.xmla as xmla
from pymongo import MongoClient
from math import ceil

from airflow.models import Variable
import http.client


def on_success(context):
    # Nos argumentos padrão da DAG colocar o seguinte parametro: 'on_success_callback': hu.on_success

    dag_id = context['dag_run'].dag_id
    execution_date = context['task_instance'].execution_date
    context['task_instance'].xcom_push(key=dag_id, value=True)

    conn = http.client.HTTPSConnection("havan.webhook.office.com")
    payload = {
      "@type": "MessageCard",
      "@context": "http://schema.org/extensions",
      "themeColor": "007500",
      "summary": dag_id,
      "sections": [
        {
          "activityTitle": dag_id,
          "activitySubtitle": "Registro automatico de execucao de DAG",
          "activityImage": "https://airflow.apache.org/docs/apache-airflow/2.0.1/_images/pin_large.png",
          "facts": [
            {
              "name": "Data de execucao",
              "value": "{0}/{1}/{2} {3}:{4}".format(
                  execution_date.strftime('%d'),
                  execution_date.strftime('%m'),
                  execution_date.strftime('%Y'),
                  execution_date.strftime('%H'),
                  execution_date.strftime('%M')
              )
            },
            {
              "name": "Status",
              "value": "A DAG " + dag_id + " foi executada com sucesso!"
            }
          ],
          "markdown": "true"
        }
      ]
    }

    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache",
    }

    print(
        conn.request(
            "POST",
            "/webhookb2/50d90d58-0c0d-4310-833b-b681af251eb1@5499809c-eec0-491d-8cea-46dc7e1ffcf8/IncomingWebhook/236518c818634a1685986f07270de64b/5f415712-b146-46fc-b252-dc321a5053cc",
            str(payload),
            headers
        )
    )



def on_failure(context, owner):
    # Nos argumentos padrão da DAG colocar o seguinte parametro: 'on_failure_callback': on_failure
    # Na dag deverá ser criada uma função que vai chamar esta do havan utils:
    #
    # def on_failure(context):
    #     hu.on_failure(context, owner)
    #
    # Desta forma é possivel enviar o owner da DAG para o teams exibir na mensagem.
    #

    dag_id = context['dag_run'].dag_id
    execution_date = context['task_instance'].execution_date
    task_id = context['task_instance'].task_id
    context['task_instance'].xcom_push(key=dag_id, value=True)
    logs_url = "https://airflowds.havan.com.br/taskinstance/list/?_flt_3_dag_id={0}&_flt_3_state=failed".format(dag_id)

    conn = http.client.HTTPSConnection("havan.webhook.office.com")
    payload = {
      "@type": "MessageCard",
      "@context": "http://schema.org/extensions",
      "themeColor": "e63946",
      "summary": dag_id,
      "sections": [
        {
          "activityTitle": dag_id,
          "activitySubtitle": "Registro automatico de ocorrencia de erro",
          "activityImage": "https://airflow.apache.org/docs/apache-airflow/2.0.1/_images/pin_large.png",
          "facts": [
            {
              "name": "Responsavel ",
              "value": owner
            },
            {
              "name": "Data de execucao",
              "value": "{0}/{1}/{2} {3}:{4}".format(
                  execution_date.strftime('%d'),
                  execution_date.strftime('%m'),
                  execution_date.strftime('%Y'),
                  execution_date.strftime('%H'),
                  execution_date.strftime('%M')
              )
            },
            {
              "name": "Status",
              "value": "Falha em " + task_id
            }
          ],
          "markdown": "true"
        }
      ],
      "potentialAction": [
        {
          "@type": "OpenUri",
          "name": "Abrir Airflow",
          "targets": [
            {
              "os": "default",
              "uri": logs_url
            }
          ]
        }
      ]
    }

    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache",
    }

    print(
        conn.request(
            "POST",
            "/webhookb2/50d90d58-0c0d-4310-833b-b681af251eb1@5499809c-eec0-491d-8cea-46dc7e1ffcf8/IncomingWebhook/236518c818634a1685986f07270de64b/5f415712-b146-46fc-b252-dc321a5053cc",
            str(payload),
            headers
        ) #.getresponse().read().decode("utf-8")
    )


def __airflow_buscar_config_da_database(database_id):
    return BaseHook.get_connection(database_id)


def obter_conn_uri(database_id, app_name='Airflow_Default'):
    con = BaseHook.get_connection(database_id)
    return {
        'port': con.port,
        'host': con.host,
        'schema': con.schema,
        'user': con.login,
        'password': con.password
    }


def obter_mongo_uri(database_id, app_name='Airflow_Default'):
    config_db = BaseHook.get_connection(database_id)

    if config_db.password != "" and config_db.password != None:
        if config_db.port != "" and config_db.port is not None:
            config_db.port = ":" + str(config_db.port)
        if config_db.port is None:
            config_db.port = ""

        print('Conectou com autenticação.')
        return 'mongodb://{}:{}@{}{}/{}?{}appName={}'.format(config_db.login, urllib.parse.quote(config_db.password), config_db.host, config_db.port, config_db.schema, config_db.extra, app_name)
    else:
        print('Conectou sem autenticação.')
        return 'mongodb://{}:{}/?appName={}'.format(config_db.host, config_db.port, app_name)


def airflow_buscar_colecao_mongo(database_id, nome_colecao, app_name='Airflow_Default'):
    """
    Retorna coleção do mongo usando as configurações gravadas na metabase do airflow.

    Parâmetros
    - database_id (str) : Id da database gravada no airflow.
    - nome_colecao (str) : Nome da coleção a ser buscada na database mongo.

    Retorno
    - Coleção do mongo.
    """

    config_db = __airflow_buscar_config_da_database(database_id)
    client = MongoClient(obter_mongo_uri(database_id))
    db = client[config_db.schema]
    return db[nome_colecao]


def airflow_buscar_colecao_mongo_com_base_autenticacao(database_id, nome_colecao, app_name='Airflow_Default'):
    """
    Retorna coleção do mongo usando as configurações gravadas na metabase do airflow.

    Parâmetros
    - database_id (str) : Id da database gravada no airflow.
    - nome_colecao (str) : Nome da coleção a ser buscada na database mongo.

    Retorno
    - Coleção do mongo.
    """

    config_db = __airflow_buscar_config_da_database(database_id)

    if config_db.password != "" and config_db.password != None:
        if config_db.port != "" and config_db.port is not None:
            config_db.port = ":" + str(config_db.port)
        if config_db.port is None:
            config_db.port = ""
        uri = 'mongodb://{}:{}@{}{}/?{}appName={}'.format(config_db.login, urllib.parse.quote(
            config_db.password), config_db.host, config_db.port, config_db.extra, app_name)
        client = MongoClient(uri)
        print('Conectou autenticando na base {}.'.format(config_db.extra))
    else:
        client = MongoClient(config_db.host, config_db.port, appname=app_name)
        print('Conectou sem autenticação.')

    db = client[config_db.schema]
    return db[nome_colecao]

# def airflow_buscar_conexao_presto(database_id):
#     """
#             Retorna conexão com o PrestoDB usando as configurações gravadas na metabase do airflow.
#
#             Parâmetros
#             - database_id (str) : Id da database gravada no airflow.
#
#             Retorno
#             - Conexao com o PrestoDB.
#     """
#     config_db = __airflow_buscar_config_da_database(database_id)
#     conn = prestodb.dbapi.connect(
#         host=config_db.host,
#         port=config_db.port,
#         user=config_db.login,
#         catalog=config_db.extra,
#         schema=config_db.schema,
#         isolation_level=1
#     )
#     return conn

def airflow_buscar_conexao_postgres(database_id):
    """
            Retorna conexão com o postgres usando as configurações gravadas na metabase do airflow.

            Parâmetros
            - database_id (str) : Id da database gravada no airflow.

            Retorno
            - Conexao com o postgres.
            """
    config_db = __airflow_buscar_config_da_database(database_id)
    conn = psycopg2.connect(host=config_db.host,
                            port=config_db.port if config_db.port else 5432,
                            database=config_db.schema,
                            user=config_db.login,
                            password=config_db.password)
    return conn

def read_pgsql(database_id: str, query: str):
    """
        Obtém e resultado de uma consulta no postgresql

        Parâmetros
        :param query: Query a ser executada no banco
        :param database_id: Id da database gravada no Airflow
    """
    with airflow_buscar_conexao_postgres(database_id) as pgsql_conn:
        with pgsql_conn as conexao:
            with conexao.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(query, None)
                return cursor.fetchall()

def read_mssql(database_id: str, query: str):
    """
        Obtém e resultado de uma consulta no SQL Server

        Parâmetros
        :param query: Query a ser executada no SQLServer
        :param database_id: Id da database gravada no Airflow
    """
    with airflow_buscar_conexao_mssql(database_id) as sqlserver_conn:
        with sqlserver_conn as conexao:
            with conexao.cursor(as_dict=True) as cursor:
                cursor.execute(query, None)
                return cursor.fetchall()


# def emails_insightteam(pg_database_id: str = 'postgres_datalake_readonly',
#                        sqlserver_database_id: str = 'mssql_172.16.3.252_itlsys') -> str:
#     '''
#     Retorna os emails da equipe do Data Science separados por ponto e virgula cf. cadastro no ItlSys
#     :param pg_database_id: Id da database do Postgresql gravada no airflow
#     :param sqlserver_database_id: Id da database do SqlServer gravada no airflow
#     '''
#
#     query_rh = '''select matricula, nome_funcionario from rh.dim_funcionario where codigo_empresa = 1
#                     and tabela_organograma = 1 and sequencia_organograma = 6357 and data_demissao is null '''
#     rs_rh = read_pgsql(pg_database_id, query_rh)
#     query_adm030 = '''select UsrEmail as email from adm030 where usrcod in ({})'''\
#         .format(','.join(["'" + str(i[0]) + "'" for i in rs_rh]))
#     rs_adm030 = read_mssql(sqlserver_database_id, query_adm030)
#     emails = ";".join([e['email'] for e in rs_adm030 if e['email'] != ''])
#     print(f'Emails da equipe DataScience obtidos: {emails}')
#     return emails

def airflow_buscar_conexao_mssql(database_id):
    """
    Retorna conexão com o mssql usando as configurações gravadas na metabase do airflow.

    Parâmetros
    - database_id (str) : Id da database gravada no airflow.

    Retorno
    - Conexao com o mssql.
    """

    config_db = __airflow_buscar_config_da_database(database_id)
    conn = pymssql.connect(server=config_db.host,
                           user=config_db.login,
                           port=config_db.port if config_db.port else 1433,
                           password=config_db.password,
                           database=config_db.schema)

    return conn


def airflow_buscar_conexao_analysis_service(database_id: str) -> xmla.XMLASource:
    """
            Retorna conexão com o analysis services usando as configurações gravadas na metabase do airflow.

            Parâmetros
            - database_id (str) : Id da database gravada no airflow.

            Retorno
            - Conexao com o analysis services.
    """
    config_db = __airflow_buscar_config_da_database(database_id)
    provider = xmla.XMLAProvider()

    connect = provider.connect(location=config_db.host,
                               username=config_db.login, password=config_db.password)
    return connect

# def airflow_buscar_dados_prestodb(database_id, query):
#     """
#             Realiza consulta no PrestoDB.
#
#             Parâmetros
#             - database_id (str) : Id da database gravada no airflow.
#             - query (str) : Query a ser executada no presto
#
#             Retorno
#             - 2 listas separados por ",".
#             - 1º Lista com os nomes de todas as colunas do dataset
#             - 2º Lista com todos os valores do dataset
#
#             *Exemplo de uso: cols, datas = airflow_buscar_dados_prestodb()...
#     """
#     try:
#         conexao_presto = airflow_buscar_conexao_presto(database_id)
#         cur = conexao_presto.cursor()
#         cur.execute(query)
#         dados = cur.fetchall()
#         colunas = [part[0] for part in cur.description]
#         cur.cancel()
#         conexao_presto.close()
#         return colunas, dados
#     except Exception as ex:
#         conexao_presto.close()
#         print('Exception '+ex)
#         raise

def airflow_buscar_quantidade_linhas_tabela_mssql(database_id, tabela):
    """
    Retorna quantidade de linhas de uma determinada tabela.

    Parâmetros
    - database_id (str) : Id da database gravada no airflow.
    - tabela (str) : Tabela que se quer saber a quantidade de linhas.

    Retorno
    - Int.
    """

    conn = airflow_buscar_conexao_mssql(database_id)
    cursor = conn.cursor()
    cursor.execute('EXECUTE sys.sp_spaceused \'{}\''.format(tabela))
    result = int(cursor.fetchone()[1])
    conn.close()
    return result


def airflow_buscar_quantidade_linhas_query_mssql(database_id, query):
    """
    Retorna quantidade de linhas de uma determinada tabela.

    Parâmetros
    - database_id (str) : Id da database gravada no airflow.
    - query (str) : Query que se quer saber a quantidade de linhas.

    Retorno
    - Int.
    """

    conn = airflow_buscar_conexao_mssql(database_id)
    cursor = conn.cursor()
    cursor.execute(query)
    result = int(cursor.fetchone()[0])
    conn.close()
    return result


def airflow_buscar_pedacos_tabela_mssql(database_id, tabela, quantidade_por_pedaco):
    """
    Retorna pedaços de uma determinada tabela limitado pela quantidade passada por parametro.

    Parâmetros
    - database_id (str) : Id da database gravada no airflow.
    - tabela (str) : Tabela que se quer obter os pedaços.
    - quantidade_por_pedaco (int) : Quantidade que se quer em cada pedaço.

    Retorno
    - Array de arrays.
    """

    posicao = 0
    limite = 0
    fatias = ceil(airflow_buscar_quantidade_linhas_tabela_mssql(
        database_id, tabela) / quantidade_por_pedaco)
    pedacos = []

    for i in range(1, fatias + 1):
        limite = i * quantidade_por_pedaco
        pedacos.append([posicao, quantidade_por_pedaco])
        posicao = limite + 1

    return pedacos


def airflow_buscar_pedacos_tabela_mssql_pela_query(database_id, query, quantidade_por_pedaco):
    """
    Retorna pedaços de uma determinada tabela limitado pela quantidade passada por parametro.

    Parâmetros
    - database_id (str) : Id da database gravada no airflow.
    - query (str) : Query que se quer obter os pedaços.
    - quantidade_por_pedaco (int) : Quantidade que se quer em cada pedaço.

    Retorno
    - Array de arrays.
    """

    posicao = 0
    limite = 0
    fatias = ceil(airflow_buscar_quantidade_linhas_query_mssql(
        database_id, query) / quantidade_por_pedaco)
    pedacos = []

    for i in range(1, fatias + 1):
        limite = i * quantidade_por_pedaco
        pedacos.append([posicao, quantidade_por_pedaco])
        posicao = limite + 1

    return pedacos


def atualizar_dataset_powerbi(group_id, dataset_id):
    """
    Autoriza usuário na aplicação do Azure Active Directory, obtem o token, e chama REST API que atualiza dataset passado por parâmetro.

    Parâmetros
    - group_id (str) : Id do grupo que o dataset pertence no powerbi.
    - dataset_id (str) : Id do dataset que se quer atualizar.

    Retorno
    - String.
    """

    # (https://dev.powerbi.com/Apps) para criar um app.
    auth_context = AuthenticationContext(
        "https://login.microsoftonline.com/common")
    # token_response = auth_context.acquire_token_with_username_password('https://analysis.windows.net/powerbi/api', 'eduardo.kohler@havan.com.br', 'hv1212', '2f3144a1-850a-45fa-9f42-207b091c601c')
    token_response = auth_context.acquire_token_with_username_password(
        'https://analysis.windows.net/powerbi/api', 'powerbi@havan.com.br', '83NQwJ', 'bd38df6a-c8e0-42ce-83f8-cf3a47b59ebe')

    api_endpoint = 'https://api.powerbi.com/v1.0/myorg/groups/{}/datasets/{}/refreshes'.format(
        group_id, dataset_id)
    SESSION = requests.Session()
    SESSION.headers.update(
        {'Authorization': "Bearer " + token_response['accessToken']})
    retorno = SESSION.post(api_endpoint)
    status_code = str(retorno)
    content = str(retorno.content)

    print('Endpoint: {0}.\nContent de retorno: {1}'.format(
        api_endpoint, content))

    if status_code == '<Response [202]>':
        return status_code + ' Accepted'
    else:
        raise ValueError(status_code)


def verifica_atualizacao_dataset_powerbi(group_id, dataset_id):
    """
    Autoriza usuário na aplicação do Azure Active Directory, obtem o token, e chama REST API que verifica se o dataset passado por parâmetro foi atualizado com sucesso.

    Parâmetros
    - group_id (str) : Id do grupo que o dataset pertence no powerbi.
    - dataset_id (str) : Id do dataset que se quer verificar a atualização.

    Retorno
    - Boolean.
    """

    auth_context = AuthenticationContext(
        "https://login.microsoftonline.com/common")
    # token_response = auth_context.acquire_token_with_username_password('https://analysis.windows.net/powerbi/api', 'eduardo.kohler@havan.com.br', 'hv1212', '2f3144a1-850a-45fa-9f42-207b091c601c')
    token_response = auth_context.acquire_token_with_username_password(
        'https://analysis.windows.net/powerbi/api', 'powerbi@havan.com.br', '83NQwJ', 'bd38df6a-c8e0-42ce-83f8-cf3a47b59ebe')

    api_endpoint = 'https://api.powerbi.com/v1.0/myorg/groups/{}/datasets/{}/refreshes'.format(
        group_id, dataset_id)
    SESSION = requests.Session()
    SESSION.headers.update(
        {'Authorization': "Bearer " + token_response['accessToken']})
    retorno = (SESSION.get(api_endpoint).json())

    if retorno == '<Response [202]>':
        return retorno + ' Accepted'
    else:
        raise ValueError(retorno)


def airflow_buscar_s3(s3_id):
    """
    Retorna um objeto de serviço S3 do boto3 (AWS SDK for Python) como base nas informações contidas no Airflow

    Parâmetros
    - s3_id (str) : Id da connection do tipo S3 cadastrado no Airflow

    Retorno
    - Boto3 AWS SDK Service
    """

    conexao_s3 = __airflow_buscar_config_da_database(s3_id)
    s3_extra_data = json.loads(conexao_s3.extra)

    s3_access_key = s3_extra_data.get("aws_access_key_id")
    s3_secret_key = s3_extra_data.get("aws_secret_access_key")
    s3_hots = s3_extra_data.get("host")
    s3 = boto3.resource('s3',
                        endpoint_url=s3_hots,
                        aws_access_key_id=s3_access_key,
                        aws_secret_access_key=s3_secret_key,
                        config=Config(signature_version='s3v4'))

    return s3


def airflow_buscar_redis(redis_id):
    """
    Retorna uma conexão com o Redis

    Parâmetros
    - redis_id (str) : Id da connection do tipo Redis cadastrado no Airflow

    Retorno
    - Cliente de conexão com o redis
    """

    connection = BaseHook.get_connection(redis_id)
    return redis.Redis(host=connection.host, port=connection.port, password=connection.password, db=0)


def SubDagOperator_with_default_executor(subdag, *args, **kwargs):
    """
    Executa a SubDag com o executor padrão, respeitando o Pool definido no Airflow.

    Parametros
    - subdag (str) : Envia os valores para SubDagOperator
    """
    from airflow.operators.subdag_operator import SubDagOperator
    from airflow.executors import get_default_executor

    return SubDagOperator(subdag=subdag, executor=get_default_executor(), *args, **kwargs)


class MongoDB:
    def __init__(self, database_id: str, app_name='Airflow_Default'):
        self.config_db = BaseHook.get_connection(database_id)
        self.client = MongoClient(obter_mongo_uri(database_id))
        self.db = self.client[self.config_db.schema]

    def __del__(self):
        print('Fechou conexão base mongo')
        self.client.close()
# airflow_buscar_conexao_mssql()


class FTPS_TLS(ftplib.FTP_TLS):
    def ntransfercmd(self, cmd, rest=None):
        conn, size = ftplib.FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            conn = self.context.wrap_socket(conn,
                                            server_hostname=self.host,
                                            session=self.sock.session)
        return conn, size
