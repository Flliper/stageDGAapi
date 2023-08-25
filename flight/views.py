import json
import os
import urllib.parse
from django.contrib.auth.models import User
from django.http import JsonResponse
import sqlite3
import pyodbc
import adodbapi
import win32com.client
import pythoncom
from django.conf import settings
from rest_framework.authtoken.models import Token
import jwt

from django.contrib.auth import authenticate, login, logout
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework.response import Response
from rest_framework import status


from .serializers import UserSerializer


# FONCTIONS POUR SQLITE


def getNameTablesSQLITE(request, bdd):
    # Établissement d'une connexion à la base de données SQLite avec le nom donné.
    con = sqlite3.connect(f'{bdd}.db')

    # Création d'un curseur pour exécuter des requêtes SQL.
    cur = con.cursor()

    # Exécution de la requête SQL pour obtenir les noms des tables dans la base de données.
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")

    # Récupération de tous les résultats de la requête précédente.
    resultats = cur.fetchall()

    # Fermeture de la connexion à la base de données.
    con.close()

    # Retour des résultats sous forme de JsonResponse.
    return JsonResponse(resultats, safe=False)


def getAllInfoTableSQLITE(request, table_name, bdd):
    # Décode l'encodage de la URL pour obtenir le nom de la table correct.
    # Par exemple, convertit "%20" en espace.
    table_name = urllib.parse.unquote(table_name)

    con = sqlite3.connect(f'{bdd}.db')
    cur = con.cursor()

    # Exécution de la requête SQL pour obtenir toutes les informations de la table spécifiée.
    # On utilise des guillemets doubles autour du nom de la table pour gérer les cas où le nom contient des espaces ou d'autres caractères spéciaux.
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

    # Récupération du filtre en tant que chaîne JSON depuis la requête GET, et par défaut un objet vide.
    filter_json = request.GET.get('filter', '{}')
    # Conversion de la chaîne JSON en dictionnaire.
    filters = json.loads(filter_json)

    # Récupération des nouveaux paramètres (nom de colonne et valeur) depuis la requête GET.
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


def getNotNullColumnSQLITE(request, bdd, table_name):
    con = sqlite3.connect(f'{bdd}.db')
    cur = con.cursor()

    cur.execute(f"PRAGMA table_info('{table_name}')")
    columns = cur.fetchall()

    not_null_columns = [col[1] for col in columns if col[3] == 1]
    con.close()

    return JsonResponse({'not_null_columns': not_null_columns})













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
    # Obtenir le chemin absolu de la base de données en utilisant le nom de la BDD
    bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')

    # Établir une connexion à la base de données Microsoft Access en utilisant pyodbc
    con = pyodbc.connect(
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'  # Utilise le pilote ODBC pour Microsoft Access
        r'DBQ=' + bdd_path + ';'  # Spécifie le chemin de la base de données à connecter
    )
    cur = con.cursor()  # Créer un curseur pour exécuter des requêtes

    # Récupérer tous les noms de tables de la base de données
    # en excluant les tables système (commençant par "MSys")
    table_names = [[table.table_name] for table in cur.tables(tableType='TABLE') if not table.table_name.startswith('MSys')]

    con.close()  # Fermer la connexion à la base de données

    # Retourner les noms de tables sous forme de réponse JSON
    return JsonResponse(table_names, safe=False)



def getAllInfoTableMS(request, table_name, bdd):
    bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')  # Obtenir le chemin absolu de la BDD

    # Décoder les caractères spéciaux du nom de la table
    table_name = urllib.parse.unquote(table_name)

    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + bdd_path + ';'
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM [{table_name}]')

    # Convertir chaque ligne des résultats en un dictionnaire où les clés sont les noms des colonnes
    # et les valeurs sont les valeurs de chaque ligne.
    # cursor.description contient les détails de chaque colonne (nom, type, etc.).
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
        where_clauses.append(f'[{column_name}] = ?')
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


# fonction utilise les modules pythoncom et win32com.client pour interagir avec les bases de données Microsoft Access via leur interface COM
def getPrimaryKeyMS(request, bdd, table_name):
    # Initialiser le module pythoncom pour permettre l'interaction avec les objets COM sous Windows.
    pythoncom.CoInitialize()

    try:
        # Décoder les caractères spéciaux du nom de la table
        table_name = urllib.parse.unquote(table_name)

        # Obtenir le chemin absolu de la base de données en utilisant le nom de la BDD
        bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')

        # Initialiser l'objet DAO.DBEngine pour interagir avec les bases de données Microsoft Access
        db_engine = win32com.client.Dispatch("DAO.DBEngine.120")

        # Ouvrir la base de données avec le chemin spécifié
        db = db_engine.OpenDatabase(bdd_path)

        # Obtenir les définitions de la table à partir de son nom
        tbd = db.TableDefs(table_name)

        primary_key = None
        # Parcourir tous les index de la table
        for idx in tbd.Indexes:
            # Si l'index est identifié comme clé primaire
            if idx.Primary:
                # Obtenir le ou les noms des champs constituant la clé primaire
                primary_key = [fld.Name for fld in idx.Fields]
                break  # On sort de la boucle une fois la clé primaire trouvée
    finally:
        # Désinitialiser le module pythoncom pour libérer les ressources utilisées par les objets COM.
        pythoncom.CoUninitialize()

    # Si une clé primaire a été trouvée, renvoyer cette clé, sinon renvoyer une erreur.
    return JsonResponse({'primaryKey': primary_key}) if primary_key is not None else JsonResponse(
        {'error': 'Primary key not found'}, safe=False)


def getForeignKeysMS(request, bdd, table_name):
    pythoncom.CoInitialize()

    # Initialiser une liste vide pour stocker les clés étrangères trouvées.
    foreign_keys = []

    try:
        # Décoder les caractères spéciaux du nom de la table.
        table_name = urllib.parse.unquote(table_name)

        # Obtenir le chemin absolu de la base de données à partir du nom de la BDD.
        bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')

        # Initialiser l'objet DAO.DBEngine pour interagir avec les bases de données Microsoft Access.
        db_engine = win32com.client.Dispatch("DAO.DBEngine.120")

        # Ouvrir la base de données avec le chemin spécifié.
        db = db_engine.OpenDatabase(bdd_path)

        # Obtenir les définitions de la table à partir de son nom.
        tbd = db.TableDefs(table_name)

        # Parcourir toutes les relations (liens entre tables) de la base de données.
        for rel in db.Relations:
            # Si la table de la relation actuelle est la table spécifiée.
            if rel.Table == table_name:
                # Ajouter les détails de la clé étrangère à la liste :
                # - Le nom de la table liée (table étrangère).
                # - Le nom du champ dans la table actuelle.
                # - Le nom du champ correspondant dans la table liée.
                foreign_keys.append([rel.ForeignTable, rel.Fields.Item(0).Name, rel.Fields.Item(0).ForeignName])

    finally:
        # Désinitialiser le module pythoncom pour libérer les ressources utilisées par les objets COM.
        pythoncom.CoUninitialize()

    # Renvoyer les clés étrangères sous forme de réponse JSON.
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
    where_clause = ' AND '.join(f'[{k}] LIKE ?' for k, v in filter_dict.items())
    values = ['%' + v + '%' for v in filter_dict.values()]

    # Add column search criteria if column_name and column_value are provided
    if columnName and columnValue:
        column_name = urllib.parse.unquote(columnName)
        column_value = urllib.parse.unquote(columnValue)
        if where_clause:
            where_clause += f' AND "{column_name}" = ?'  # Use double quotes around the column name
            values.append(column_value)
        else:
            where_clause = f'"{column_name}" = ?'  # Use double quotes around the column name
            values.append(column_value)

    sql_query = f'SELECT * FROM [{table_name}]'
    if where_clause:
        sql_query += f' WHERE {where_clause}'
    if sort_clause:
        sql_query += f' ORDER BY {sort_clause}'

    cursor.execute(sql_query, values)

    resultats = [list(row) for row in cursor.fetchall()]
    conn.close()

    # Use Python's slicing for pagination
    resultats = resultats[(page-1)*limit : page*limit]

    return JsonResponse(resultats, safe=False)


def getNotNullColumnMS(request, bdd, table_name):
    bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + bdd_path + ';'
    )

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    columns = cursor.columns(table=table_name).fetchall()
    not_null_columns = [col.column_name for col in columns if col.nullable == 0]

    conn.close()

    return JsonResponse({'not_null_columns': not_null_columns})

















# FONCTIONS GLOBALES


def getNameTables(request, bdd):
    # Vérifier si le nom de la base de données donné (bdd) figure dans la liste des bases de données SQLite définies dans le fichier settings.py.
    if bdd in settings.SQLITE_DBS:
        # Si c'est le cas, appeler la fonction qui traite les bases de données SQLite pour obtenir les noms des tables.
        return getNameTablesSQLITE(request, bdd)

    # Vérifier si le nom de la base de données donné (bdd) figure dans la liste des bases de données Microsoft Access définies dans les paramètres.
    elif bdd in settings.ACCESS_DBS:
        # Si c'est le cas, appeler la fonction qui traite les bases de données Microsoft Access pour obtenir les noms des tables.
        return getNameTablesMS(request, bdd)

    # Si la base de données n'est ni de type SQLite ni de type Microsoft Access.
    else:
        # Renvoyer une réponse d'erreur indiquant un type de base de données non valide.
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


def getNotNullColumns(request, bdd, table_name):
    table_name = urllib.parse.unquote(table_name)

    if bdd in settings.SQLITE_DBS:
        return getNotNullColumnSQLITE(request, bdd, table_name)
    elif bdd in settings.ACCESS_DBS:
        return getNotNullColumnMS(request, bdd, table_name)
    else:
        return JsonResponse({'error': 'Invalid database type'}, safe=False)


## RECUPERER LES NOMS DES BDD

def getBDDNames(request):
    return JsonResponse({"SQLite": settings.SQLITE_DBS, "MS": settings.ACCESS_DBS})





# AUTHENTIFICATION ET AUTORISATION

# Désactive le middleware CSRF pour cette vue. Ceci est nécessaire lors de la création d'API REST.
# Néanmoins, cela peut présenter des risques de sécurité si ce n'est pas géré correctement.
@csrf_exempt
def login(request):
    # Vérifie si la requête est de type POST. La connexion doit généralement être effectuée avec une requête POST.
    if request.method == 'POST':

        # La requête POST contient des données sous forme JSON. Ces données sont converties en dictionnaire python.
        data = json.loads(request.body)

        # Récupère le nom d'utilisateur et le mot de passe de la requête.
        username = data.get('username')
        password = data.get('password')

        # `authenticate` est une fonction Django qui vérifie si le nom d'utilisateur et le mot de passe sont corrects.
        user = authenticate(username=username, password=password)

        # Si l'utilisateur est authentifié avec succès.
        if user is not None:

            # Récupère ou crée un token pour cet utilisateur.
            # Le token est souvent utilisé pour authentifier l'utilisateur pour les requêtes futures.
            token, created = Token.objects.get_or_create(user=user)

            # Renvoie une réponse JSON indiquant le succès de la connexion, le nom d'utilisateur et le token.
            return JsonResponse({'status': 'success', 'user': username, 'token': token.key})

        # Si l'authentification échoue, renvoie une erreur.
        else:
            return JsonResponse({'status': 'error', 'error': 'Invalid login credentials'})

    # Si la requête n'est pas de type POST, renvoie une erreur.
    else:
        return JsonResponse({'status': 'error', 'error': 'Invalid request method'})


@csrf_exempt
def logout_view(request):
    logout(request)
    return JsonResponse({"detail": "Success"})



@csrf_exempt
def signup(request):
    # La fonction `verify_token` est appelée pour vérifier le token de l'utilisateur.
    # Si le token est invalide ou manquant, une réponse d'erreur est renvoyée.
    if not verify_token(request):
        return JsonResponse({'error': 'Unauthorized'}, status=401)

    # Vérifier si la requête est de type POST. Les inscriptions doivent généralement être effectuées avec une requête POST.
    if request.method == 'POST':

        # La requête POST contient des données sous forme JSON.
        # Ces données sont décodées et converties en dictionnaire python.
        data = json.loads(request.body.decode('utf-8'))

        # `UserSerializer` est probablement un sérialiseur Django Rest Framework (DRF)
        # qui valide les données d'un utilisateur et les prépare pour la sauvegarde.
        serializer = UserSerializer(data=data)

        # Vérifie si les données fournies sont valides.
        if serializer.is_valid():
            # Si les données sont valides, elles sont sauvegardées en base de données.
            serializer.save()

            # Renvoie une réponse JSON avec les données sérialisées et un code de statut 201, indiquant que la création a réussi.
            return JsonResponse(serializer.data, status=201)

        # Si les données ne sont pas valides, renvoie une réponse JSON avec les erreurs et un code de statut 400.
        return JsonResponse(serializer.errors, status=400)


@csrf_exempt
def manageUser(request):
    if not verify_token(request):
        return JsonResponse({'error': 'Unauthorized'}, status=401)

    if request.method == 'POST':
        data = json.loads(request.body.decode('utf-8'))
        action = data.get("action", "")

        if action == "add":
            user_data = {k: v for k, v in data.items() if k in ["username", "email", "password"]}
            serializer = UserSerializer(data=user_data)
            if serializer.is_valid():
                serializer.save()
                return JsonResponse(serializer.data, status=201)
            return JsonResponse(serializer.errors, status=400)

        elif action == "delete":
            username_to_delete = data.get("usernameToDelete", "")
            try:
                user = User.objects.get(username=username_to_delete)
                print(user)
                user.delete()
                return JsonResponse({"status": "success"}, status=200)
            except User.DoesNotExist:
                return JsonResponse({"error": "User not found"}, status=404)

        else:
            return JsonResponse({"error": "Invalid action"}, status=400)


def verify_token(request):
    # Récupère le champ 'Authorization' de l'en-tête de la requête.
    auth_header = request.headers.get('Authorization')

    # Si l'en-tête 'Authorization' est manquant, renvoie False.
    if not auth_header:
        return False

    # Essaye de séparer le mot "Token" du token réel. Cela suppose que l'en-tête est formaté comme "Token <votre_token>".
    token_key = auth_header.split("Token ")[1]

    try:
        # Essaye de récupérer un objet Token correspondant à partir de la base de données.
        token = Token.objects.get(key=token_key)

        # Si le token existe dans la base de données, renvoie True.
        return True
    except Token.DoesNotExist:
        # Si aucun token correspondant n'est trouvé, renvoie False.
        return False


# Carte qui associe des représentations sous forme de chaînes de caractères de types Python à
# leurs équivalents dans une base de données SQL
TYPE_MAPPING = {
    "<class 'int'>": "INT",
    "<class 'str'>": "TEXT",
    "<class 'float'>": "REAL",
    "<class 'decimal.Decimal'>": "NUMERIC",
    "<class 'datetime.date'>": "DATE",
}

@csrf_exempt
def updateCellSQLITE(request, bdd):
    if request.method == 'POST':
        data = json.loads(request.body)
        table = data['table']
        primaryColumn = data['primaryColumn']
        primaryValue = data['primaryValue']
        column = data['column']
        newValue = data['newValue']

        con = sqlite3.connect(f'{bdd}.db')
        cur = con.cursor()
        query = f'UPDATE {table} SET {column} = ? WHERE {primaryColumn} = ?'
        cur.execute(query, (newValue, primaryValue))
        con.commit()
        con.close()

        return JsonResponse({'status': 'success'}, status=200)
    else:
        return JsonResponse({'error': 'Invalid request'}, status=400)



@csrf_exempt
def updateCellMS(request, bdd):
    if request.method == 'POST':
        data = json.loads(request.body)
        bdd = data['bdd']
        table = data['table']
        primaryColumn = data['primaryColumn']
        primaryValue = data['primaryValue']
        column = data['column']
        newValue = data['newValue']

        bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')
        conn_str = (
                r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                r'DBQ=' + bdd_path + ';'
        )
        con = pyodbc.connect(conn_str)
        cur = con.cursor()
        query = f'UPDATE [{table}] SET [{column}] = ? WHERE [{primaryColumn}] = ?'
        cur.execute(query, (newValue, primaryValue))
        con.commit()
        con.close()

        return JsonResponse({'status': 'success'}, status=200)
    else:
        return JsonResponse({'error': 'Invalid request'}, status=400)


@csrf_exempt
def updateCell(request, bdd):
    if not verify_token(request):
        return JsonResponse({'error': 'Unauthorized'}, status=401)
    if bdd in settings.SQLITE_DBS:
        return updateCellSQLITE(request, bdd)
    elif bdd in settings.ACCESS_DBS:
        return updateCellMS(request, bdd)
    else:
        return JsonResponse({'error': 'Invalid database type'}, safe=False)


@csrf_exempt
def manageTableSQLITE(request, bdd):
    if request.method == 'POST':
        data = json.loads(request.body)
        operation = data['operation']
        tableName = data['tableName']
        selectedTable = data['selectedTable']
        newTableName = data['newTableName']

        con = sqlite3.connect(f'{bdd}.db')
        cur = con.cursor()
        if operation == 'add':
            query = f'CREATE TABLE {tableName} (id INTEGER PRIMARY KEY)'
        elif operation == 'delete':
            query = f'DROP TABLE {selectedTable}'
        elif operation == 'rename':
            query = f'ALTER TABLE {selectedTable} RENAME TO {newTableName}'
        else:
            return JsonResponse({'error': 'Invalid operation'}, status=400)

        cur.execute(query)
        con.commit()
        con.close()

        return JsonResponse({'status': 'success'}, status=200)
    else:
        return JsonResponse({'error': 'Invalid request'}, status=400)


def rename_table_ms_access(cur, old_table_name, new_table_name):
    # Étape 1: Récupérez tous les noms de colonnes et leurs types de l'ancienne table
    cur.execute(f"SELECT * FROM {old_table_name}")
    columns = [(column[0], TYPE_MAPPING[str(column[1])]) for column in cur.description]

    # Étape 2: Définissez les colonnes pour la création de la nouvelle table
    column_definitions = ', '.join([f"{name} {type_}" for name, type_ in columns])

    # Étape 3: Créez la nouvelle table avec les mêmes colonnes que l'ancienne table
    cur.execute(f"CREATE TABLE {new_table_name} ({column_definitions})")

    # Étape 4: Copiez toutes les données de l'ancienne table vers la nouvelle table
    column_names_only = [name for name, _ in columns]
    cur.execute(f"INSERT INTO {new_table_name} SELECT {', '.join(column_names_only)} FROM {old_table_name}")

    # Étape 5: Supprimez l'ancienne table
    cur.execute(f"DROP TABLE {old_table_name}")


@csrf_exempt
def manageTableMS(request, bdd):
    if request.method == 'POST':
        data = json.loads(request.body)
        operation = data['operation']
        tableName = data['tableName']
        selectedTable = data['selectedTable']
        newTableName = data['newTableName']

        bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')
        conn_str = (
                r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                r'DBQ=' + bdd_path + ';'
        )
        con = pyodbc.connect(conn_str)
        cur = con.cursor()

        if operation == 'add':
            query = f'CREATE TABLE {tableName} (id COUNTER PRIMARY KEY)'
            cur.execute(query)
            con.commit()
        elif operation == 'delete':
            query = f'DROP TABLE {selectedTable}'
            cur.execute(query)
            con.commit()
        elif operation == 'rename':
            rename_table_ms_access(cur, selectedTable, newTableName)
            con.commit()
            con.close()
            return JsonResponse({'status': 'success'}, status=200)
        else:
            return JsonResponse({'error': 'Invalid operation'}, status=400)

        con.close()
        return JsonResponse({'status': 'success'}, status=200)

    else:
        return JsonResponse({'error': 'Invalid request'}, status=400)


@csrf_exempt
def manageTable(request, bdd):
    if not verify_token(request):
        return JsonResponse({'error': 'Unauthorized'}, status=401)
    if bdd in settings.SQLITE_DBS:
        return manageTableSQLITE(request, bdd)
    elif bdd in settings.ACCESS_DBS:
        return manageTableMS(request, bdd)
    else:
        return JsonResponse({'error': 'Invalid database type'}, safe=False)


@csrf_exempt
def manageColumnSQLITE(request, bdd):
    if request.method == 'POST':
        data = json.loads(request.body)
        operation = data['operation']
        columnName = data['columnName']
        selectedColumn = data['selectedColumn']
        selectedTable = data['selectedTable']
        newColumnName = data['newColumnName']

        con = sqlite3.connect(f'{bdd}.db')
        cur = con.cursor()
        if operation == 'add':
            query = f'ALTER TABLE {selectedTable} ADD COLUMN {columnName} TEXT'  # Change type as needed
        elif operation == 'delete':
            query = f'ALTER TABLE {selectedTable} DROP COLUMN {selectedColumn}'
        elif operation == 'rename':
            query = f'ALTER TABLE {selectedTable} RENAME COLUMN {selectedColumn} TO {newColumnName}'
        else:
            return JsonResponse({'error': 'Invalid operation'}, status=400)

        cur.execute(query)
        con.commit()
        con.close()

        return JsonResponse({'status': 'success'}, status=200)
    else:
        return JsonResponse({'error': 'Invalid request'}, status=400)


def drop_column_ms_access(cur, table_name, column_name):
    # Step 1: get all column names and their types from the table
    cur.execute(f"SELECT * FROM {table_name}")
    columns = [(column[0], TYPE_MAPPING[str(column[1])]) for column in cur.description]

    # Step 2: create a temporary table with the same columns, except the one to be deleted
    new_columns = [(name, type_) for name, type_ in columns if name != column_name]
    column_definitions = ', '.join([f"{name} {type_}" for name, type_ in new_columns])
    temp_table_name = table_name + "_temp"
    cur.execute(f"CREATE TABLE {temp_table_name} ({column_definitions})")

    # Step 3: copy all data from the original table to the temporary table
    column_names_only = [name for name, _ in new_columns]
    cur.execute(f"INSERT INTO {temp_table_name} SELECT {', '.join(column_names_only)} FROM {table_name}")

    # Step 4: drop the original table
    cur.execute(f"DROP TABLE {table_name}")

    # Step 5: recreate the original table with the desired structure
    cur.execute(f"CREATE TABLE {table_name} ({column_definitions})")

    # Step 6: copy all data from the temporary table to the original table
    cur.execute(f"INSERT INTO {table_name} SELECT {', '.join(column_names_only)} FROM {temp_table_name}")

    # Step 7: drop the temporary table
    cur.execute(f"DROP TABLE {temp_table_name}")


def rename_column_ms_access(cur, table_name, old_column_name, new_column_name):
    # Étape 1 : Récupérez tous les noms de colonnes et leurs types de la table
    cur.execute(f"SELECT * FROM {table_name}")
    columns = [(column[0], TYPE_MAPPING[str(column[1])]) for column in cur.description]

    # Étape 2 : Créez une liste de nouvelles colonnes, renommez la colonne spécifiée
    new_columns = [(new_column_name if name == old_column_name else name, type_) for name, type_ in columns]
    column_definitions = ', '.join([f"{name} {type_}" for name, type_ in new_columns])

    temp_table_name = table_name + "_temp"
    cur.execute(f"CREATE TABLE {temp_table_name} ({column_definitions})")

    # Étape 3 : Copiez toutes les données de la table d'origine vers la table temporaire
    old_column_names_only = [name for name, _ in columns]
    new_column_names_only = [new_column_name if name == old_column_name else name for name in old_column_names_only]
    cur.execute(f"INSERT INTO {temp_table_name} ({', '.join(new_column_names_only)}) SELECT {', '.join(old_column_names_only)} FROM {table_name}")

    # Étape 4 : Supprimez la table d'origine
    cur.execute(f"DROP TABLE {table_name}")

    # Étape 5 : Recréez la table d'origine avec la colonne renommée
    cur.execute(f"CREATE TABLE {table_name} ({column_definitions})")

    # Étape 6 : Copiez toutes les données de la table temporaire vers la table d'origine
    cur.execute(f"INSERT INTO {table_name} SELECT {', '.join(new_column_names_only)} FROM {temp_table_name}")

    # Étape 7 : Supprimez la table temporaire
    cur.execute(f"DROP TABLE {temp_table_name}")



@csrf_exempt
def manageColumnMS(request, bdd):
    if request.method == 'POST':
        data = json.loads(request.body)
        operation = data['operation']
        columnName = data['columnName']
        selectedColumn = data['selectedColumn']
        selectedTable = data['selectedTable']
        newColumnName = data['newColumnName']

        bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')
        conn_str = (
                r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                r'DBQ=' + bdd_path + ';'
        )
        con = pyodbc.connect(conn_str)
        cur = con.cursor()

        if operation == 'add':
            query = f'ALTER TABLE {selectedTable} ADD COLUMN {columnName} TEXT'  # Change type as needed
            cur.execute(query)
            con.commit()
        elif operation == 'delete':
            drop_column_ms_access(cur, selectedTable, selectedColumn)
            con.commit()
            con.close()
            return JsonResponse({'status': 'success'}, status=200)
        elif operation == 'rename':
            rename_column_ms_access(cur, selectedTable, selectedColumn, newColumnName)
            con.commit()
            con.close()
            return JsonResponse({'status': 'success'}, status=200)
        else:
            con.close()
            return JsonResponse({'error': 'Invalid operation'}, status=400)

        con.close()
        return JsonResponse({'status': 'success'}, status=200)
    else:
        return JsonResponse({'error': 'Invalid request'}, status=400)



@csrf_exempt
def manageColumn(request, bdd):
    if not verify_token(request):
        return JsonResponse({'error': 'Unauthorized'}, status=401)
    if bdd in settings.SQLITE_DBS:
        return manageColumnSQLITE(request, bdd)
    elif bdd in settings.ACCESS_DBS:
        return manageColumnMS(request, bdd)
    else:
        return JsonResponse({'error': 'Invalid database type'}, safe=False)


@csrf_exempt
def manageRowSQLite(request, bdd):
    if request.method == 'POST':
        data = json.loads(request.body)
        operation = data['operation']
        selectedTable = data['selectedTable']
        newRowData = data['newRowData']  # Expected to be a dict with column names as keys
        primaryKeyToDelete = data['primaryKeyToDelete']
        columns = data['columns']

        con = sqlite3.connect(f'{bdd}.db')
        cur = con.cursor()

        if operation == 'add':
            columns = ', '.join(newRowData.keys())
            values = ', '.join(f"'{value}'" for value in newRowData.values())
            query = f'INSERT INTO {selectedTable} ({columns}) VALUES ({values})'
        elif operation == 'delete':
            primaryKeyField = columns[0]
            query = f"DELETE FROM {selectedTable} WHERE {primaryKeyField}={primaryKeyToDelete}"

        else:
            return JsonResponse({'error': 'Invalid operation'}, status=400)

        cur.execute(query)
        con.commit()
        con.close()

        return JsonResponse({'status': 'success'}, status=200)
    else:
        return JsonResponse({'error': 'Invalid request'}, status=400)


@csrf_exempt
def manageRowMS(request, bdd):
    if request.method == 'POST':
        data = json.loads(request.body)
        operation = data['operation']
        selectedTable = data['selectedTable']
        newRowData = data['newRowData']  # Expected to be a dict with column names as keys
        primaryKeyToDelete = data['primaryKeyToDelete']
        columns = data['columns']

        bdd_path = os.path.join(os.getcwd(), f'{bdd}.accdb')
        conn_str = (
                r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                r'DBQ=' + bdd_path + ';'
        )
        con = pyodbc.connect(conn_str)
        cur = con.cursor()

        if operation == 'add':
            columns = ', '.join(newRowData.keys())
            values = ', '.join(f"'{value}'" for value in newRowData.values())
            query = f'INSERT INTO {selectedTable} ({columns}) VALUES ({values})'
        elif operation == 'delete':
            primaryKeyField = columns[0]
            query = f"DELETE FROM {selectedTable} WHERE {primaryKeyField}={primaryKeyToDelete}"

        else:
            return JsonResponse({'error': 'Invalid operation'}, status=400)

        cur.execute(query)
        con.commit()
        con.close()

        return JsonResponse({'status': 'success'}, status=200)
    else:
        return JsonResponse({'error': 'Invalid request'}, status=400)


@csrf_exempt
def manageRow(request, bdd):
    if not verify_token(request):
        return JsonResponse({'error': 'Unauthorized'}, status=401)
    if bdd in settings.SQLITE_DBS:
        return manageRowSQLite(request, bdd)
    elif bdd in settings.ACCESS_DBS:
        return manageRowMS(request, bdd)
    else:
        return JsonResponse({'error': 'Invalid database type'}, safe=False)

