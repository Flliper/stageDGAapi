import json
import os
import urllib.parse
from django.http import JsonResponse
import sqlite3
import pyodbc
import adodbapi
import win32com.client
import pythoncom
from django.conf import settings



# FONCTIONS POUR SQLITE


def getNameTablesSQLITE(request, bdd):
    con = sqlite3.connect(f'{bdd}.db')
    cur = con.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    resultats = cur.fetchall()
    con.close()

    return JsonResponse(resultats, safe=False)


def getAllInfoTableSQLITE(request, table_name, bdd):
    table_name = urllib.parse.unquote(table_name)
    con = sqlite3.connect(f'{bdd}.db')
    cur = con.cursor()
    cur.execute(f'SELECT * FROM "{table_name}"')
    resultats = cur.fetchall()
    con.close()

    return JsonResponse(resultats, safe=False)


def getNameColumnsSQLITE(request, bdd, table_name):
    table_name = urllib.parse.unquote(table_name)

    con = sqlite3.connect(f'{bdd}.db')
    cur = con.cursor()
    cur.execute(f'PRAGMA table_info("{table_name}")')
    resultats = [column[1] for column in cur.fetchall()]
    con.close()

    return JsonResponse(resultats, safe=False)


def getAllInfoColumnSQLITE(request, bdd, table_name, column_name):
    table_name = urllib.parse.unquote(table_name)
    column_name = urllib.parse.unquote(column_name)

    con = sqlite3.connect(f'{bdd}.db')
    cur = con.cursor()
    cur.execute(f'SELECT "{column_name}" FROM "{table_name}"')
    resultats = [row[0] for row in cur.fetchall()]
    con.close()

    return JsonResponse(resultats, safe=False)


def getCountSQLITE(request, bdd, table_name):
    table_name = urllib.parse.unquote(table_name)

    filter_json = request.GET.get('filter', '{}')
    filters = json.loads(filter_json)

    # Ajouter les nouveaux paramètres
    column_name = request.GET.get('columnName', None)
    column_value = request.GET.get('columnValue', None)

    con = sqlite3.connect(f'{bdd}.db')
    cur = con.cursor()

    base_query = f'SELECT COUNT(*) FROM "{table_name}"'
    params = []

    where_clauses = []
    if filters:
        where_clauses.extend(f'{key} LIKE ?' for key in filters.keys())
        params.extend('%' + value + '%' for value in filters.values())

    if column_name is not None and column_value is not None:
        where_clauses.append(f'{column_name} = ?')
        params.append(column_value)

    if where_clauses:
        query = base_query + ' WHERE ' + ' AND '.join(where_clauses)
        cur.execute(query, params)
    else:
        cur.execute(base_query)

    count = cur.fetchone()[0]
    con.close()

    return JsonResponse({'count': count}, safe=False)


def getInfoTableSQLITE(request, bdd, table_name):
    page = int(request.GET.get('page', 1))
    limit = int(request.GET.get('limit', 10))
    table_name = urllib.parse.unquote(table_name)

    filter_param = request.GET.get('filter', '{}')
    filter_dict = json.loads(filter_param)

    sort_param = request.GET.get('sort', '{}')
    sort_dict = json.loads(sort_param)

    con = sqlite3.connect(f'{bdd}.db')
    cur = con.cursor()
    offset = (page - 1) * limit

    sort_clause = ', '.join(f'"{k}" {v}' for k, v in sort_dict.items())
    where_clause = ' AND '.join(f'"{k}" LIKE "%{v}%"' for k, v in filter_dict.items())

    sql_query = f'SELECT * FROM "{table_name}"'
    if where_clause:
        sql_query += f' WHERE {where_clause}'
    if sort_clause:
        sql_query += f' ORDER BY {sort_clause}'
    sql_query += f' LIMIT {limit} OFFSET {offset}'

    cur.execute(sql_query)
    resultats = cur.fetchall()
    con.close()

    return JsonResponse(resultats, safe=False)


def getRowSQLITE(request, bdd, table_name, row_id):
    table_name = urllib.parse.unquote(table_name)

    con = sqlite3.connect(f'{bdd}.db')
    cur = con.cursor()

    cur.execute(f"PRAGMA table_info('{table_name}')")
    columns = [column[1] for column in cur.fetchall() if column[-1]]
    primary_key = columns[0] if columns else 'id'

    sql_query = f'SELECT * FROM "{table_name}" WHERE "{primary_key}" = ?'
    cur.execute(sql_query, (row_id,))
    resultat = cur.fetchone()
    con.close()

    return JsonResponse(resultat, safe=False)


def getPrimaryKeySQLITE(request, bdd, table_name):
    con = sqlite3.connect(f'{bdd}.db')
    cur = con.cursor()
    cur.execute(f'PRAGMA table_info("{table_name}")')
    resultats = cur.fetchall()

    pk_name = None
    for row in resultats:
        if row[5] == 1:
            pk_name = row[1]
            break

    con.close()

    return JsonResponse({'primaryKey': pk_name})


def getForeignKeysSQLITE(request, bdd, table_name):
    con = sqlite3.connect(f'{bdd}.db')
    cur = con.cursor()
    cur.execute(f'PRAGMA foreign_key_list("{table_name}")')
    resultats = cur.fetchall()
    con.close()

    foreign_keys = []
    for res in resultats:
        foreign_keys.append([res[2], res[3], res[4]])

    return JsonResponse(foreign_keys, safe=False)



def getForeignKeysForAllTablesSQLITE(request, bdd):
    con = sqlite3.connect(f'{bdd}.db')
    cur = con.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    table_names = [table[0] for table in cur.fetchall()]

    data = {}
    for table_name in table_names:
        cur.execute(f'PRAGMA foreign_key_list("{table_name}")')
        fk_data = cur.fetchall()
        foreign_keys = []
        for row in fk_data:
            foreign_keys.append([row[3], row[2], row[4]])
        data[table_name] = foreign_keys

    con.close()

    return JsonResponse(data)


def getDataByColumnValueSQLITE(request, bdd, table_name, column_name, column_value):
    table_name = urllib.parse.unquote(table_name)
    column_name = urllib.parse.unquote(column_name)
    column_value = urllib.parse.unquote(column_value)

    con = sqlite3.connect(f'{bdd}.db')
    cur = con.cursor()

    sql_query = f'SELECT * FROM "{table_name}" WHERE "{column_name}" = ?'
    cur.execute(sql_query, (column_value,))
    resultats = cur.fetchall()
    con.close()

    return JsonResponse(resultats, safe=False)


def getPrimaryKeysForAllTablesSQLITE(request, bdd):
    con = sqlite3.connect(f'{bdd}.db')
    cur = con.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    table_names = [table[0] for table in cur.fetchall()]

    data = {}
    for table_name in table_names:
        cur.execute(f'PRAGMA table_info("{table_name}")')
        table_info = cur.fetchall()
        primary_key = next((column[1] for column in table_info if column[5] == 1), None)
        if primary_key is not None:
            data[table_name] = primary_key

    con.close()

    return JsonResponse(data)


def getTableDataSQLITE(request, bdd, table_name):
    page = int(request.GET.get('page', 1))
    limit = int(request.GET.get('limit', 10))
    table_name = urllib.parse.unquote(table_name)

    columnName = request.GET.get('columnName', None)
    columnValue = request.GET.get('columnValue', None)

    filter_param = request.GET.get('filter', '{}')
    filter_dict = json.loads(filter_param)

    sort_param = request.GET.get('sort', '{}')
    sort_dict = json.loads(sort_param)

    con = sqlite3.connect(f'{bdd}.db')
    cur = con.cursor()
    offset = (page - 1) * limit

    sort_clause = ', '.join(f'"{k}" {v}' for k, v in sort_dict.items())
    where_clause = ' AND '.join(f'"{k}" LIKE "%{v}%"' for k, v in filter_dict.items())

    # Add column search criteria if column_name and column_value are provided
    if columnName and columnValue:
        column_name = urllib.parse.unquote(columnName)
        column_value = urllib.parse.unquote(columnValue)
        if where_clause:
            where_clause += f' AND "{columnName}" = ?'
        else:
            where_clause = f'"{columnName}" = ?'

    sql_query = f'SELECT * FROM "{table_name}"'
    if where_clause:
        sql_query += f' WHERE {where_clause}'
    if sort_clause:
        sql_query += f' ORDER BY {sort_clause}'
    sql_query += f' LIMIT {limit} OFFSET {offset}'

    if columnName and columnValue:
        cur.execute(sql_query, (columnValue,))
    else:
        cur.execute(sql_query)

    resultats = cur.fetchall()
    con.close()

    return JsonResponse(resultats, safe=False)















# FONCTIONS POUR MSACCESS


def check_or_grant_permissions(request, bdd):
    bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + bdd_path + ';'
    )
    con = pyodbc.connect(conn_str)
    cur = con.cursor()

    try:
        # Tente de lire les enregistrements de MSysObjects.
        cur.execute("SELECT * FROM MSysObjects WHERE Type = 1 AND Flags = 0")
        cur.fetchall()
    except pyodbc.Error as e:
        if 'no read permission' in str(e):
            # Si l'erreur de permission est levée, accorde les permissions.
            cur.execute("GRANT SELECT ON MSysObjects TO Admin;")
            con.commit()
        else:
            # Si c'est une autre erreur, la relève.
            raise e

    con.close()
    return


def getNameTablesMS(request, bdd):
    bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')  # Obtenir le chemin absolu de la BDD

    con = pyodbc.connect(
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + bdd_path + ';'
    )
    cur = con.cursor()

    table_names = [[table.table_name] for table in cur.tables(tableType='TABLE') if not table.table_name.startswith('MSys')]
    con.close()

    return JsonResponse(table_names, safe=False)



def getAllInfoTableMS(request, table_name, bdd):
    bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')  # Obtenir le chemin absolu de la BDD

    table_name = urllib.parse.unquote(table_name)
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + bdd_path + ';'
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM [{table_name}]')
    resultats = [dict(zip([column[0] for column in cursor.description], row))
                 for row in cursor.fetchall()]
    conn.close()

    return JsonResponse(resultats, safe=False)


def getNameColumnsMS(request, bdd, table_name):
    table_name = urllib.parse.unquote(table_name)

    bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + bdd_path + ';'
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Wrap table_name in brackets
    cursor.execute(f"SELECT * FROM [{table_name}]")
    resultats = [column[0] for column in cursor.description]
    conn.close()

    return JsonResponse(resultats, safe=False)



def getAllInfoColumnMS(request, bdd, table_name, column_name):
    table_name = urllib.parse.unquote(table_name)
    column_name = urllib.parse.unquote(column_name)

    bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + bdd_path + ';'
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    cursor.execute(f"SELECT [{column_name}] FROM [{table_name}]")
    resultats = [row[0] for row in cursor.fetchall()]
    conn.close()

    return JsonResponse(resultats, safe=False)


def getPrimaryKeysForAllTablesMS(request, bdd):
    bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')  # Obtenir le chemin absolu de la BDD

    con = pyodbc.connect(
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + bdd_path + ';'
    )
    cur = con.cursor()

    table_names = [table.table_name for table in cur.tables(tableType='TABLE')]

    data = {}
    for table_name in table_names:
        cur.execute(f'SELECT * FROM "{table_name}"')
        table_info = [column[0] for column in cur.description]
        primary_key = table_info[0]  # Assuming the first column is the primary key
        data[table_name] = primary_key

    con.close()

    return JsonResponse(data)


def getCountMS(request, bdd, table_name):
    table_name = urllib.parse.unquote(table_name)

    filter_json = request.GET.get('filter', '{}')
    filters = json.loads(filter_json)

    # Ajouter les nouveaux paramètres
    column_name = request.GET.get('columnName', None)
    column_value = request.GET.get('columnValue', None)

    bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + bdd_path + ';'
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    base_query = f'SELECT COUNT(*) FROM [{table_name}]'
    params = []

    where_clauses = []
    if filters:
        where_clauses.extend(f'{key} LIKE ?' for key in filters.keys())
        params.extend('%' + value + '%' for value in filters.values())

    if column_name is not None and column_value is not None:
        where_clauses.append(f'{column_name} = ?')
        params.append(column_value)

    if where_clauses:
        query = base_query + ' WHERE ' + ' AND '.join(where_clauses)
        cursor.execute(query, params)
    else:
        cursor.execute(base_query)

    count = cursor.fetchone()[0]
    conn.close()

    return JsonResponse({'count': count}, safe=False)


def getInfoTableMS(request, bdd, table_name):
    page = int(request.GET.get('page', 1))
    limit = int(request.GET.get('limit', 10))
    table_name = urllib.parse.unquote(table_name)

    filter_param = request.GET.get('filter', '{}')
    filter_dict = json.loads(filter_param)

    sort_param = request.GET.get('sort', '{}')
    sort_dict = json.loads(sort_param)

    bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + bdd_path + ';'
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    offset = (page - 1) * limit

    sort_clause = ', '.join(f'[{k}] {v}' for k, v in sort_dict.items())
    where_clause = ' AND '.join(f'[{k}] LIKE ?' for k, v in filter_dict.items())
    params = ['%' + v + '%' for v in filter_dict.values()]

    sql_query = f'SELECT * FROM [{table_name}]'
    if where_clause:
        sql_query += f' WHERE {where_clause}'
    if sort_clause:
        sql_query += f' ORDER BY {sort_clause}'
    # MS Access doesn't support the LIMIT and OFFSET clauses directly
    # Alternative solutions are complicated and depend on the specific use case

    cursor.execute(sql_query, params)
    resultats = [list(row) for row in cursor.fetchall()]
    conn.close()

    return JsonResponse(resultats, safe=False)




def getRowMS(request, bdd, table_name, row_id):
    table_name = urllib.parse.unquote(table_name)

    bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + bdd_path + ';'
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    sql_query = f'SELECT * FROM [{table_name}]'
    cursor.execute(sql_query)

    # Assuming primary key is the first column
    primary_key = cursor.description[0][0]

    sql_query = f'SELECT * FROM [{table_name}] WHERE [{primary_key}] = ?'
    cursor.execute(sql_query, (row_id,))
    resultat = cursor.fetchone()

    # Convert to list for JsonResponse
    resultat_list = list(resultat) if resultat else None

    conn.close()

    return JsonResponse(resultat_list, safe=False) if resultat is not None else JsonResponse({'error': 'Data not found'}, safe=False)



# def getPrimaryKeyMS(request, bdd, table_name):
#     check_or_grant_permissions(request, bdd)
#     table_name = urllib.parse.unquote(table_name)
#
#     bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')
#     conn_str = (
#         r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
#         r'DBQ=' + bdd_path + ';'
#     )
#
#     conn = pyodbc.connect(conn_str)
#     cursor = conn.cursor()
#
#     # Query the system table MSysObjects to find the primary key
#     cursor.execute(f"""
#         SELECT MSysObjects.Name
#         FROM MSysObjects INNER JOIN MSysRelationships ON MSysObjects.Id = MSysRelationships.PrimaryTableId
#         WHERE (((MSysObjects.Name)="{table_name}") AND ((MSysObjects.Type)=1) AND ((MSysRelationships.JoinType)=1));
#     """)
#
#     primary_key_row = cursor.fetchone()
#     primary_key = primary_key_row.Name if primary_key_row else None
#
#     conn.close()
#
#     return JsonResponse({'primaryKey': primary_key}) if primary_key is not None else JsonResponse({'error': 'Primary key not found'}, safe=False)
#

# def getPrimaryKeyMS(request, bdd, table_name):
#     # check_or_grant_permissions(request, bdd)
#     table_name = urllib.parse.unquote(table_name)
#
#     bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')
#     conn_str = (
#         r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
#         r'DBQ=' + bdd_path + ';'
#     )
#
#     conn = pyodbc.connect(conn_str)
#     cursor = conn.cursor()
#
#     # Use pyodbc's built-in method to get primary key info
#     primary_keys = cursor.primaryKeys(table=table_name)
#
#     primary_key = None
#     for row in primary_keys:
#         primary_key = row.column_name
#         break  # Only get the first primary key (if there are multiple)
#
#     conn.close()
#
#     return JsonResponse({'primaryKey': primary_key}) if primary_key is not None else JsonResponse({'error': 'Primary key not found'}, safe=False)


# def getPrimaryKeyMS(request, bdd, table_name):
#     # check_or_grant_permissions(request, bdd)
#     table_name = urllib.parse.unquote(table_name)
#
#     bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')
#     conn_str = (
#         'Provider=Microsoft.ACE.OLEDB.12.0;'
#         'Data Source=' + bdd_path + ';'
#     )
#
#     conn = adodbapi.connect(conn_str)
#
#     schema = conn.getSchema('COLUMNS')
#     primary_key_row = schema[schema['TABLE_NAME'] == table_name]
#     primary_key = primary_key_row['COLUMN_NAME'].values[0]
#
#     conn.close()
#
#     return JsonResponse({'primaryKey': primary_key}) if primary_key is not None else JsonResponse({'error': 'Primary key not found'}, safe=False)


# def getPrimaryKeyMS(request, bdd, table_name):
#     table_name = urllib.parse.unquote(table_name)
#     bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')
#
#     db_engine = win32com.client.Dispatch("DAO.DBEngine.120")
#     db = db_engine.OpenDatabase(bdd_path)
#     tbd = db.TableDefs(table_name)
#
#     primary_key = None
#     for idx in tbd.Indexes:
#         if idx.Primary:
#             primary_key = [fld.Name for fld in idx.Fields]
#
#     return JsonResponse({'primaryKey': primary_key}) if primary_key is not None else JsonResponse({'error': 'Primary key not found'}, safe=False)
#

def getPrimaryKeyMS(request, bdd, table_name):
    pythoncom.CoInitialize()

    try:
        table_name = urllib.parse.unquote(table_name)
        bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')

        db_engine = win32com.client.Dispatch("DAO.DBEngine.120")
        db = db_engine.OpenDatabase(bdd_path)
        tbd = db.TableDefs(table_name)

        primary_key = None
        for idx in tbd.Indexes:
            if idx.Primary:
                primary_key = [fld.Name for fld in idx.Fields]
    finally:
        pythoncom.CoUninitialize()

    return JsonResponse({'primaryKey': primary_key}) if primary_key is not None else JsonResponse(
        {'error': 'Primary key not found'}, safe=False)


def getForeignKeysMS(request, bdd, table_name):
    pythoncom.CoInitialize()

    foreign_keys = []
    try:
        table_name = urllib.parse.unquote(table_name)
        bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')

        db_engine = win32com.client.Dispatch("DAO.DBEngine.120")
        db = db_engine.OpenDatabase(bdd_path)
        tbd = db.TableDefs(table_name)

        for rel in db.Relations:
            if rel.Table == table_name:
                foreign_keys.append([rel.ForeignTable, rel.Fields.Item(0).Name, rel.Fields.Item(0).ForeignName])

    finally:
        pythoncom.CoUninitialize()

    return JsonResponse(foreign_keys, safe=False)



def getForeignKeysForAllTablesMS(request, bdd):
    pythoncom.CoInitialize()

    foreign_keys = {}
    try:
        bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')

        db_engine = win32com.client.Dispatch("DAO.DBEngine.120")
        db = db_engine.OpenDatabase(bdd_path)

        for table in db.TableDefs:
            # Ignore system tables
            if table.Name.startswith('MSys'):
                continue

            for rel in db.Relations:
                if rel.Table == table.Name:
                    if table.Name not in foreign_keys:
                        foreign_keys[table.Name] = []
                    foreign_keys[table.Name].append([rel.Fields.Item(0).Name, rel.ForeignTable, rel.Fields.Item(0).ForeignName])

    finally:
        pythoncom.CoUninitialize()

    return JsonResponse(foreign_keys, safe=False)



def getDataByColumnValueMS(request, bdd, table_name, column_name, column_value):
    table_name = urllib.parse.unquote(table_name)
    column_name = urllib.parse.unquote(column_name)
    column_value = urllib.parse.unquote(column_value)

    bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + bdd_path + ';'
    )

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    sql_query = f'SELECT * FROM [{table_name}] WHERE [{column_name}] = ?'
    cursor.execute(sql_query, (column_value,))
    resultats = [list(row) for row in cursor.fetchall()]
    conn.close()

    return JsonResponse(resultats, safe=False)



def getTableDataMS(request, bdd, table_name):
    page = int(request.GET.get('page', 1))
    limit = int(request.GET.get('limit', 10))
    table_name = urllib.parse.unquote(table_name)

    columnName = request.GET.get('columnName', None)
    columnValue = request.GET.get('columnValue', None)

    filter_param = request.GET.get('filter', '{}')
    filter_dict = json.loads(filter_param)

    sort_param = request.GET.get('sort', '{}')
    sort_dict = json.loads(sort_param)

    bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')
    conn_str = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            r'DBQ=' + bdd_path + ';'
    )

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    sort_clause = ', '.join(f'[{k}] {v}' for k, v in sort_dict.items())
    where_clause = ' AND '.join(f'[{k}] LIKE "%{v}%"' for k, v in filter_dict.items())

    # Add column search criteria if column_name and column_value are provided
    if columnName and columnValue:
        column_name = urllib.parse.unquote(columnName)
        column_value = urllib.parse.unquote(columnValue)
        if where_clause:
            where_clause += f' AND [{column_name}] = ?'
        else:
            where_clause = f'[{column_name}] = ?'

    sql_query = f'SELECT * FROM [{table_name}]'
    if where_clause:
        sql_query += f' WHERE {where_clause}'
    if sort_clause:
        sql_query += f' ORDER BY {sort_clause}'

    if columnName and columnValue:
        cursor.execute(sql_query, (columnValue,))
    else:
        cursor.execute(sql_query)

    resultats = [list(row) for row in cursor.fetchall()]
    conn.close()

    # Use Python's slicing for pagination
    resultats = resultats[(page-1)*limit : page*limit]

    return JsonResponse(resultats, safe=False)
















# FONCTIONS GLOBALES


def getNameTables(request, bdd):
    if bdd in settings.SQLITE_DBS:
        return getNameTablesSQLITE(request, bdd)
    elif bdd in settings.ACCESS_DBS:
        return getNameTablesMS(request, bdd)
    else:
        return JsonResponse({'error': 'Invalid database type'}, safe=False)

def getAllInfoTable(request, table_name, bdd):
    if bdd in settings.SQLITE_DBS:
        return getAllInfoTableSQLITE(request, table_name, bdd)
    elif bdd in settings.ACCESS_DBS:
        return getAllInfoTableMS(request, table_name, bdd)
    else:
        return JsonResponse({'error': 'Invalid database type'}, safe=False)

def getNameColumns(request, bdd, table_name):
    if bdd in settings.SQLITE_DBS:
        return getNameColumnsSQLITE(request, bdd, table_name)
    elif bdd in settings.ACCESS_DBS:
        return getNameColumnsMS(request, bdd, table_name)
    else:
        return JsonResponse({'error': 'Invalid database type'}, safe=False)

def getAllInfoColumn(request, bdd, table_name, column_name):
    if bdd in settings.SQLITE_DBS:
        return getAllInfoColumnSQLITE(request, bdd, table_name, column_name)
    elif bdd in settings.ACCESS_DBS:
        return getAllInfoColumnMS(request, bdd, table_name, column_name)
    else:
        return JsonResponse({'error': 'Invalid database type'}, safe=False)

def getCount(request, bdd, table_name):
    if bdd in settings.SQLITE_DBS:
        return getCountSQLITE(request, bdd, table_name)
    elif bdd in settings.ACCESS_DBS:
        return getCountMS(request, bdd, table_name)
    else:
        return JsonResponse({'error': 'Invalid database type'}, safe=False)

def getInfoTable(request, bdd, table_name):
    if bdd in settings.SQLITE_DBS:
        return getInfoTableSQLITE(request, bdd, table_name)
    elif bdd in settings.ACCESS_DBS:
        return getInfoTableMS(request, bdd, table_name)
    else:
        return JsonResponse({'error': 'Invalid database type'}, safe=False)

def getRow(request, bdd, table_name, row_id):
    if bdd in settings.SQLITE_DBS:
        return getRowSQLITE(request, bdd, table_name, row_id)
    elif bdd in settings.ACCESS_DBS:
        return getRowMS(request, bdd, table_name, row_id)
    else:
        return JsonResponse({'error': 'Invalid database type'}, safe=False)

def getPrimaryKey(request, bdd, table_name):
    if bdd in settings.SQLITE_DBS:
        return getPrimaryKeySQLITE(request, bdd, table_name)
    elif bdd in settings.ACCESS_DBS:
        return getPrimaryKeyMS(request, bdd, table_name)
    else:
        return JsonResponse({'error': 'Invalid database type'}, safe=False)

def getForeignKeys(request, bdd, table_name):
    if bdd in settings.SQLITE_DBS:
        return getForeignKeysSQLITE(request, bdd, table_name)
    elif bdd in settings.ACCESS_DBS:
        return getForeignKeysMS(request, bdd, table_name)
    else:
        return JsonResponse({'error': 'Invalid database type'}, safe=False)

def getForeignKeysForAllTables(request, bdd):
    if bdd in settings.SQLITE_DBS:
        return getForeignKeysForAllTablesSQLITE(request, bdd)
    elif bdd in settings.ACCESS_DBS:
        return getForeignKeysForAllTablesMS(request, bdd)
    else:
        return JsonResponse({'error': 'Invalid database type'}, safe=False)

def getDataByColumnValue(request, bdd, table_name, column_name, column_value):
    if bdd in settings.SQLITE_DBS:
        return getDataByColumnValueSQLITE(request, bdd, table_name, column_name, column_value)
    elif bdd in settings.ACCESS_DBS:
        return getDataByColumnValueMS(request, bdd, table_name, column_name, column_value)
    else:
        return JsonResponse({'error': 'Invalid database type'}, safe=False)

def getPrimaryKeysForAllTables(request, bdd):
    if bdd in settings.SQLITE_DBS:
        return getPrimaryKeysForAllTablesSQLITE(request, bdd)
    elif bdd in settings.ACCESS_DBS:
        return getPrimaryKeysForAllTablesMS(request, bdd)
    else:
        return JsonResponse({'error': 'Invalid database type'}, safe=False)

def getTableData(request, bdd, table_name):
    if bdd in settings.SQLITE_DBS:
        return getTableDataSQLITE(request, bdd, table_name)
    elif bdd in settings.ACCESS_DBS:
        return getTableDataMS(request, bdd, table_name)
    else:
        return JsonResponse({'error': 'Invalid database type'}, safe=False)
