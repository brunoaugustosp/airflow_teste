import pyodbc
import datetime



#---------------------CONEXAO COM O BANCO DE DADOS SQLSERVER---------------------------

# server = 'ip' 
# database = 'TrddB' 
# username = 'trddty' 
# password = '1ddduality'
# driver = '{SQL Server}'
# port = 3333

# connectionstring = (f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};')
# cnxn = pyodbc.connect(connectionstring)




# sqlcursor = cnxn.cursor()
# sqlcursor.execute("""SELECT TOP 999 CodFilial, NumOS, CodSequencia, Container, Nome  FROM tb_OSCarga JOIN tb_Motorista ON tb_OSCarga.codMotorista = tb_Motorista.codMotorista WHERE "Tara" NOT IN (0) AND "BaixaPiso" = 0 AND "Entrega" = 0 AND "CodFilial" = 4 ORDER BY "CodSequencia" DESC;""")
# data = sqlcursor.fetchall()
# print(data)
# cnxn.close()



data_referencia = str(datetime.datetime.now() + datetime.timedelta(days=-1))[:10]

print(data_referencia)


query_consulta_vendas = ("SELECT "
                        "  PDV001F.Id [IdVenda], "
                        "  ENT001.Id [IdCliente], "
                        "	ISNULL(EntEmail,'') [email], "
                        "	ISNULL(EntDDDFon,'') + ISNULL(EntFon,'') [phone], "
                        "   (SELECT SUBSTRING(EntNom, 1,1)) [fi], "
                        "	(SELECT SUBSTRING(EntNom, 1, CHARINDEX(' ', EntNom))) [fn], "
                        "	(SELECT SUBSTRING(EntNom, CHARINDEX(' ', EntNom), LEN(EntNom))) [ln], "
                        "	EntCpfCgc [cpf],"
                        "	ISNULL(EntCep,'') [zip], "
                        "	ISNULL(ADM014.CidNom,'') [ct],"
                        "	ISNULL(EstCod,'') [st], "
                        "	'BR' [country], "
                        "	YEAR(EntDatNas) [doby],"
                        "	MONTH(EntDatNas) [dobm],"
                        "	DAY(EntDatNas) [dobd],"
                        "	CASE EntSex WHEN 0 THEN 'M' ELSE 'F' END [gen], "
                        "	DATEDIFF(YEAR, EntDatNas, GETDATE()) [age], "
                        "	'Purchase' [event_name], "
                        "	CxaHorIni [event_time], "
                        "	CxaVlrLiq [value], "
                        "	IdFilial [IdFilial], "
                        "	'BRL' [currency] "
                        "FROM PDV001F(NOLOCK) "
                        "	LEFT JOIN ENT001 (NOLOCK) ON CxaEntCod = EntCod "
                        "	LEFT JOIN DBO.ADM015 (NOLOCK) ON CepCod = EntCep "
                        "	LEFT JOIN DBO.ADM014 (NOLOCK) ON ADM014.CidCod = ADM015.CidCod "
                        "WHERE EmpCod = '1' "
                        "	AND CxaTip = 1 "
                        "	AND CxaCan = 0 "
                        f"	AND CxaDat = {data_referencia} "
                        "	ORDER BY CxaHorIni")


print(query_consulta_vendas)