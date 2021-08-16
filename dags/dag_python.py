

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator


import datetime

args = {
    'owner': 'Havan',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='DAG_PYTHON',
    default_args=args,
    schedule_interval=None,
    tags=['exdample']
)



# [START howto_operator_python_kwargs]
# CONSULTA DE VENDAS NO SQL SERVER

def consulta_vendas():

    data_referencia = str(datetime.datetime.now() + datetime.timedelta(days=-1))[:10]

    print(data_referencia)
    # #---------------------CONEXAO COM O BANCO DE DADOS SQLSERVER---------------------------
    # server = 'ip,porta' 
    # database = 'name' 
    # username = 'root' 
    # password = 'senha'
    # driver = '{SQL Server}'
    # port = 3333

    # connectionstring = (f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};')
    # cnxn = pyodbc.connect(connectionstring)
    
    # sqlcursor = cnxn.cursor()
    # sqlcursor.execute("""SELECT TOP 999 CodFilial, NumOS, CodSequencia, Container, Nome  FROM tb_OSCarga JOIN tb_Motorista ON tb_OSCarga.codMotorista = tb_Motorista.codMotorista WHERE "Tara" NOT IN (0) AND "BaixaPiso" = 0 AND "Entrega" = 0 AND "CodFilial" = 4 ORDER BY "CodSequencia" DESC;""")
    # data = sqlcursor.fetchall()
    # print(data)
    pass


def insert_mongo():
    print('CONEXÂO DA API DO FACEBOOK')


def api_facebook():
    print('CONEXÂO DA API DO FACEBOOK')



def email_confirmacao():
    print('EMAIL ENVIADO')



# -------------------- INSTANCIA DE OPERADORES --------------------

consulta_vendas = PythonOperator(
    task_id='consulta_vendas',
    python_callable=consulta_vendas,
    dag=dag,
)

insert_mongo = PythonOperator(
    task_id='insert_mongo',
    python_callable=insert_mongo,
    dag=dag,
)

api_facebook = PythonOperator(
    task_id='api_facebook',
    python_callable=api_facebook,
    dag=dag,
)

email_confirmacao = PythonOperator(
    task_id='email_confirmacao',
    python_callable=email_confirmacao,
    dag=dag,
)

consulta_vendas >> insert_mongo >> api_facebook >>  email_confirmacao
# [END howto_operator_python_kwargs]