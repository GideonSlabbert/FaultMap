import sqlite3
from sqlite3 import Error

# function to select all the contents from a table specified in the sql expression that is the input to the function
def select_all_from_table(sql):
    """ select values from specified table"""

    db_file = r"C:\FaultMap\pythonsqlite.db"

    conn = create_connection(db_file)

    try:
        cur = conn.cursor()
        cur.execute(sql)
        content_tuple = cur.fetchall()

        cases = []
        for row in content_tuple:
            cases.append(list(row))

    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
    return cases

def create_connection(db_file):
    """ create a database connection to the SQLite databases"""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn

def create_table(conn, sql):
    """ create a table from the create_table_sql statement"""
    try:
        cur = conn.cursor()
        cur.execute(sql)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

def insert_into_table(conn, sql, values):
    """ insert values into specified table"""
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
    return #cur.lastrowid

def delete_from_table(conn, sql, values):
    """ delete values from specified table"""
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
    return #cur.lastrowid

def drop_table(conn, sql):
    """ select values from specified table"""
    try:
        cur = conn.cursor()
        cur.execute(sql)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


def select_from_table_scenarios(case_id):
    """ select values from specified table"""

    db_file = r"C:\FaultMap\pythonsqlite.db"
    sql = ''' SELECT * FROM scenarios WHERE case_id = ?'''
    conn = create_connection(db_file)

    search_id = (case_id,);

    try:
        cur = conn.cursor()
        cur.execute(sql, search_id)
        content_tuple = cur.fetchall()

        scenarios = []
        for row in content_tuple:
            scenarios.append(list(row))

    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
    return scenarios

def select_from_table_where(sql,id):
    """ select values from specified table"""

    db_file = r"C:\FaultMap\pythonsqlite.db"
    conn = create_connection(db_file)

    search_id = (id,);

    try:
        cur = conn.cursor()
        cur.execute(sql, search_id)
        content_tuple = cur.fetchall()

        result = []
        for row in content_tuple:
            result.append(list(row))

    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
    return result


def convert_db_info_to_dict_weightcalc(case_id):
    sql = ''' SELECT * FROM methods WHERE case_id = ?'''
    # case_id = 1
    methods_table = select_from_table_where(sql, case_id)
    methods = []
    for method in methods_table:
        if method[2] == 1: methods.append('transfer_entropy_kernel')
        if method[3] == 1: methods.append('transfer_entropy_kraskov')
        if method[4] == 1: methods.append('cross_correlation')
        if method[5] == 1: methods.append('partial_correlation')

    sql = ''' SELECT * FROM scenarios WHERE case_id = ?'''
    case_id = 1
    scenarios_info = select_from_table_where(sql, case_id)
    scenarios = [i[1] for i in scenarios_info]

    caseconfig = {}
    caseconfig['datatype'] = 'file'
    caseconfig['methods'] = [i for i in methods]
    caseconfig['scenarios'] = [i for i in scenarios]
    scenario_ids = [i[0] for i in scenarios_info]

    for scenario_id in scenario_ids:
        sql = ''' SELECT * FROM settings WHERE scenario_id = ?'''
        # scenario_id = 1
        settings_info = select_from_table_where(sql, scenario_id)

        for setting in settings_info:
            caseconfig[setting[1]] = {
                'use_connections': bool(setting[3]),
                'transient': bool(setting[4]),
                'boxnum': setting[5],
                'boxsize': setting[6],
                'normalise': setting[7],
                'detrend': bool(setting[8]),
                'delaytype': setting[9],
                'sampling_rate': setting[10],
                'sub_sampling_interval': setting[11],
                'sampling_unit': setting[12],
                'testsize': setting[13],
                'startindex': setting[14],
                'sigtest': bool(setting[15]),
                'thresh_method': setting[16],
                'surr_method': setting[17],
                'allthresh': bool(setting[18]),
                'additional_parameters': {
                    'test_significance': bool(setting[19]),
                    'signifigance_permutations': setting[20],
                    'auto_embed': bool(setting[21])
                }
            }

    for scenario in scenarios_info:
        caseconfig[scenario[1]] = {
            'settings': [i[1] for i in settings_info],
            'data': scenario[3],
            'test_delays': scenario[4],
            'causevarindexes': scenario[5],
            'affectedvarindexes': scenario[6],
            'bandgap_filtering': bool(scenario[7]),
            'bidirectional_delays': bool(scenario[8]),
            'scalelimits': scenario[9]
        }

    caseconfig['methods_store'] = methods
    caseconfig['scenario_store'] = [i for i in scenarios]

    return caseconfig

def convert_db_info_to_dict_graphreduce(case_id):
    sql = ''' SELECT * FROM scenarios WHERE case_id = ?'''
    scenarios_info = select_from_table_where(sql,case_id)

    caseconfig = {}
    caseconfig['datatype'] = 'file'
    caseconfig['scenarios'] = [i[1] for i in scenarios_info]
    caseconfig['scenario_store'] = [i[1] for i in scenarios_info]

    for scenario in scenarios_info:
        scenario_id = scenario[0]
        scenario_name = scenario[1]
        sql = ''' SELECT * FROM graphreduce WHERE scenario_id = ?'''
        graphreduce_table = select_from_table_where(sql,scenario_id)
        caseconfig[scenario_name] = {
                                    'graph':graphreduce_table[0][2],
                                    'percentile':graphreduce_table[0][3],
                                    'depth':graphreduce_table[0][4],
                                    'weight_discretion':bool(graphreduce_table[0][5])
                                    }
    return caseconfig

def convert_db_info_to_dict_resultrecon(case_id):
    sql = ''' SELECT * FROM scenarios WHERE case_id = ?'''
    scenarios_info = select_from_table_where(sql,case_id)

    caseconfig = {}
    caseconfig['datatype'] = 'file'
    caseconfig['scenarios'] = [i[1] for i in scenarios_info]
    caseconfig['scenario_store'] = [i[1] for i in scenarios_info]

    for scenario in scenarios_info:
        scenario_id = scenario[0]
        scenario_name = scenario[1]
        sql = ''' SELECT * FROM resultreconstruction WHERE scenario_id = ?'''
        resultrecon_table = select_from_table_where(sql,scenario_id)
        caseconfig[scenario_name] = {
                                    'bias_correction':bool(resultrecon_table[0][2])
                                    }
    return caseconfig


def update_table_where(sql_update, sql_select, id):
    """ select values from specified table"""

    db_file = r"C:\FaultMap\pythonsqlite.db"
    conn = create_connection(db_file)

    update_id = (id,);
    result = []
    try:
        cur = conn.cursor()
        cur.execute(sql_update, update_id)
        conn.commit()
        cur.execute(sql_select, update_id)
        content_tuple = cur.fetchall()

        for row in content_tuple:
            result.append(list(row))

    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
    return result


def convert_db_info_to_dict_noderank(case_id):
    sql = ''' SELECT * FROM scenarios WHERE case_id = ?'''
    scenarios_info = select_from_table_where(sql, case_id)

    caseconfig = {}
    caseconfig['datatype'] = 'file'
    caseconfig['scenarios'] = [i[1] for i in scenarios_info]
    caseconfig['scenario_store'] = [i[1] for i in scenarios_info]

    sql = ''' SELECT * FROM methods WHERE case_id = ?'''
    methods_table = select_from_table_where(sql, case_id)
    methods = []
    for method in methods_table:
        if method[2] == 1: methods.append('transfer_entropy_kernel')
        if method[3] == 1: methods.append('transfer_entropy_kraskov')
        if method[4] == 1: methods.append('cross_correlation')
        if method[5] == 1: methods.append('partial_correlation')
    caseconfig['weight_methods'] = [i for i in methods]

    sql = ''' SELECT * FROM rank_methods WHERE case_id = ?'''
    rank_methods_table = select_from_table_where(sql, case_id)
    rank_methods = []
    for method in rank_methods_table:
        if method[2] == 1: rank_methods.append('eigenvector')
        if method[3] == 1: rank_methods.append('katz')
        if method[4] == 1: rank_methods.append('pagerank')
    caseconfig['rank_methods'] = [i for i in rank_methods]

    for scenario in scenarios_info:
        scenario_id = scenario[0]
        scenario_name = scenario[1]
        sql = ''' SELECT * FROM noderank WHERE scenario_id = ?'''
        noderank_table = select_from_table_where(sql, scenario_id)
        caseconfig[scenario_name] = {
            'settings': noderank_table[0][2],
            'connections': noderank_table[0][3],
            'm': noderank_table[0][4],
            'boxindex_type': noderank_table[0][5],
            'boxindex_start': noderank_table[0][6],
            'boxindex_end': noderank_table[0][7],
            'use_connections': bool(noderank_table[0][8]),
            'use_bias': bool(noderank_table[0][9]),
            'dummies': bool(noderank_table[0][10])
        }
    return caseconfig