
import pyodbc

# Database configuration
server = 'localhost'
username = 'SA'
password = 'test1234*'
databaseAct = 'act'
databaseAdaptor = 'adaptor'


def get_act_connection():
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + databaseAct + ';UID=' + username + ';PWD=' + password)
    return conn

def get_adaptor_connection():
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + databaseAdaptor + ';UID=' + username + ';PWD=' + password)
    return conn

