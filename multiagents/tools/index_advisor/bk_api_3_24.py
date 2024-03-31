import sys
# sys.path.insert(0, '/home/wuy/DB-GPT')
# from configs import POSTGRESQL_CONFIG
POSTGRESQL_CONFIG = {
  "host": "182.92.179.3",
  "port": 5432,
  "user": "dbmind",
  "password": "DBMINDdbmind2020",
  "dbname": "wuy_test"
}
# from multiagents.tools.index_advisor.index_selection.selection_utils.postgres_dbms import PostgresDatabaseConnector
# from multiagents.tools.index_advisor.index_selection.selection_utils import selec_com
# from multiagents.tools.index_advisor.configs import get_index_result
from multiagents.tools.metrics import advisor
from multiagents.tools.metrics import get_workload_statistics
import ast
from multiagents.initialization import LANGUAGE

import re
import json
import sys
import os
import time
# sys.path.insert(0,'./')
from index_eab.eab_utils.workload import Workload, Table, Column, Query
# from index_eab.eab_utils.common_utils import get_columns_from_schema, read_row_query
from index_eab.eab_utils.postgres_dbms import PostgresDatabaseConnector

from index_eab.index_advisor.extend_algorithm import ExtendAlgorithm
from index_eab.index_advisor.drop_algorithm import DropAlgorithm

# from multiagents.tools.index_advisor.configs import get_index_result



FUNCTION_DEFINITION = {
    "optimize_index_selection": {
        "name": "optimize_index_selection",
        "description":
            "使用索引选择算法返回推荐的索引。" if LANGUAGE == "zh"
            else "returns the recommended index by running the index selection algorithm.",
        "parameters": {'type': 'object', 'properties': {}}
    }
}


INDEX_SELECTION_ALGORITHMS = {
    "extend": ExtendAlgorithm,
    "drop": DropAlgorithm

}
# def replace_placeholders(sql_string, substring, sampled_values):
#     # Define the regex pattern:
#     # 1. Locate the substring (e.g. "WHERE")
#     # 2. Followed by any number of whitespace characters (using \s*)
#     # 3. An operator (+/-/*/=/>/<)
#     # 4. Followed by any number of whitespace characters (using \s*)

#     # 5. A placeholder (e.g. $2, $3, etc.)

#     # print(f"in replace_placeholders, {sql_string, sampled_values}")
#     pattern = re.escape(substring) + r"\s*([\+\-*/=><])\s*(\$\d+)"

#     # Replace placeholders with value 2 using re.sub
#     def repl(match):
#         if len(sampled_values) > 1:
#             return substring + ' ' + match.group(1) + ' ' + str(sampled_values[0])
#         else:
#             return substring + ' ' + match.group(1) + ' ' + str(2)

#     return re.sub(pattern, repl, sql_string)

def replace_placeholders(sql_string, substring, sampled_values):


    pattern = r"\$\d+\b"
    return re.sub(pattern, str(sampled_values[0]) if len(sampled_values)>1 else '2', sql_string)

def read_row_query(sql_list, columns, column_sampled_values, _type="template"):
    workload = list()
    for query_id, query_text in enumerate(sql_list):
        # if type == "template" and exp_conf["queries"] \
        #         and query_id + 1 not in exp_conf["queries"]:
        #     continue
     
        # if 'insert' in query_text['sql'].lower():
        #     continue
        # if 'update' in query_text['sql'].lower():
        #     continue
        # if 'delete' in query_text['sql'].lower():
        #     continue

        # print(f"query_text['sql'] {query_text['sql']}")
        query = Query(query_id, query_text=query_text['sql'], frequency=query_text['frequency'])
        for column in columns:
            # column_tmp = [col for col in columns if column.name == col.name]
            if column.name in query.text.lower() and \
                    f"{column.table.name}" in query.text.lower():
                # column.name
                print(f"### query_text {query.text.lower()}")
                query.text = replace_placeholders(query.text.lower(), column.name, column_sampled_values[column])

                query.columns.append(column)

        workload.append(query)

    return workload

def get_ind_cost(connector, query, indexes, mode="hypo"):
    connector.create_indexes(indexes, mode)

    stmt = f"explain (format json) {query}"
    query_plan = connector.exec_fetch(stmt)[0][0]["Plan"]
    # drop view
    # self._cleanup_query(query)
    total_cost = query_plan["Total Cost"]

    if mode == "hypo":
        connector.drop_hypo_indexes()
    else:
        connector.drop_indexes()

    return total_cost

def get_index_result(algo, work_list, connector, columns, column_sampled_values,
                     sel_params="parameters", process=False, overhead=False):
    print(f"workload_list")
    script_path = os.path.abspath(__file__)
    script_dir = os.path.dirname(script_path)

    # exp_conf_file = script_dir + \
    #     f"/index_selection/selection_data/algo_conf/{algo}_config.json"
    # with open(exp_conf_file, "r") as rf:
    #     exp_config = json.load(rf)

    # config = selec_com.find_parameter_list(exp_config["algorithms"][0],
    #                                        params=sel_params)[0]
    parameters = {"budget_MB": 1500, "max_index_width": 2, "max_indexes": 5, "constraint": "storage"}
    algo='extend'
    
    workload = Workload(read_row_query(work_list, columns, column_sampled_values, _type=""))
    # connector.enable_simulation()
    # time.sleep(.1)
    try:
        connector.drop_hypo_indexes()
    except Exception as e:
        print(e)

    algorithm = INDEX_SELECTION_ALGORITHMS[algo](
        connector, parameters, process=process)

    indexes = algorithm.calculate_best_indexes(workload, overhead=overhead)

    if indexes == [] or indexes == None or indexes == "":
        return [], -1, -1

    if isinstance(indexes[0], list) and len(indexes) >= 1:
        indexes = indexes[0]

    # if indexes are of string type
    if not isinstance(indexes, list):
        indexes = [str(indexes)]
    else:
        indexes = [str(ind) for ind in indexes]

    cols = [ind.split(",") for ind in indexes]
    cols = [list(map(lambda x: x.split(".")[-1], col)) for col in cols]
    indexes = [
        f"{ind.split('.')[0]}#{','.join(col)}" for ind,
        col in zip(
            indexes,
            cols)]

    no_cost, ind_cost = list(), list()
    total_no_cost, total_ind_cost = 0, 0
    for sql in work_list:

        # if 'insert' in sql['sql'].lower():
        #     continue
        # if 'update' in sql['sql'].lower():
        #     continue
        # if 'delete' in sql['sql'].lower():
        #     continue

        # print(f"###  ### esti cost sql {sql}")
        if '$' not in sql['sql']:
            no_cost_ = get_ind_cost(connector, sql['sql'], [])
            print(f"no_cost_ {no_cost_}")
            total_no_cost += round(no_cost_*sql['frequency'], 2)
            no_cost.append(no_cost_)

            ind_cost_ = get_ind_cost(connector, sql['sql'], indexes)
            print(f"ind_cost_ {ind_cost_}")
            total_ind_cost += round(ind_cost_*sql['frequency'], 2)
            ind_cost.append(ind_cost_)

    return indexes, total_no_cost, total_ind_cost

def get_sampled_values(db_connector, column, table, sample_size=1):

        sql = f"select {column} from {table} limit ({sample_size})"

        rows = db_connector.exec_fetch(sql, one=False)

        sampled_values = []
        for row in rows:
            # if row is of string type, add quotes
            if isinstance(row[0], str):
                sampled_values.append(f"\'{row[0]}\'")
        # print(f" in get_sample_values, {table}.{column}: {sampled_values}")
        return sampled_values
    
def get_columns_from_db(db_connector):

    tables, columns = list(), list()
    column_sampled_values={}
    for table in db_connector.get_tables():
        # print(f"-------------- table {table}")
        table_object = Table(table)
        tables.append(table_object)
        # print(f"@@@@@@ cols: {db_connector.get_cols(table)}")
        for col in db_connector.get_cols(table):
 
            sampled_values = get_sampled_values(db_connector, col, table)
            # column_object = Column(col, sampled_values)
            
            column_object = Column(col)
            column_object.table=table_object
            # print(f"############## col {col}, {column_object}")
            column_sampled_values[column_object]=sampled_values
            table_object.add_column(column_object)
            columns.append(column_object)

    return tables, columns, column_sampled_values

def optimize_index_selection(**kwargs):
    """optimize_index_selection(start_time : int, end_time : int) returns the recommended index by running the algorithm 'Extend'.
        This method uses a recursive algorithm that considers only a limited subset of index candidates.
        The method exploits structures and properties that are typical for real-world workloads and the performance of indexes.
        It identifies beneficial indexes and does not construct similar indexes.
        The recursion only realizes index selections/extensions with significant additional performance per size ratio.

        The following is an example:
        Thoughts: I will use the \\\'optimize_index_selection\\\' command to recommend the index for the given workload.
        Reasoning: I need to recommend the effective index for the given workload. I will use the \\\'optimize_index_selection\\\' command to get the index from 'Extend' and return the result.
        Plan: - Use the \\\'optimize_index_selection\\\' command to get the index.
        Command: {"name": "optimize_index_selection",
                    "args": {"workload": "SELECT A.col1 from A join B where A.col2 = B.col2 and B.col3 > 2 group by A.col1"}}
        Result: Command optimize_index_selection returned: "A#col2; B#col2,col3"
    """
    # print(f"in optimize_index_selection, kwargs {kwargs}")  # kwargs are start_time and end_time.

    # 1. Split the workloads by database names
    databases = {}
    workload_statistics = get_workload_statistics()
    if isinstance(workload_statistics, str):
        workload_statistics = ast.literal_eval(workload_statistics)

    if workload_statistics==0:
        raise ValueError("failed to get worklaod_statistics, thus fail to recommmend indexes.") # added by wuy
    
    for query_template in workload_statistics:
        database_name = query_template["dbname"]

        if database_name not in databases:
            databases[database_name] = []

        databases[database_name].append({"sql": query_template["sql"], "frequency": query_template["calls"]})

    index_advice = "推荐的索引是：\n" if LANGUAGE == "zh" else f"Recommended indexes: \n"

    # print(f"databases {databases}")

    for dbname in databases:

        # 2. load db settings
        db_config = {"postgresql": POSTGRESQL_CONFIG}
        db_config["postgresql"]["dbname"] = dbname
        connector = PostgresDatabaseConnector(host=db_config['postgresql']['host'], port=db_config['postgresql']['port'], user=db_config['postgresql']['user'], password=db_config['postgresql']['password'], db_name=db_config['postgresql']['dbname'])

        tables, columns, column_sampled_values = get_columns_from_db(connector)  # todo sample data for each column
        # print(f"dbname {dbname}, tables {tables}, columns {columns}")

        # 3. read the workload queries
        workload = databases[dbname]  # list of dict
        # print(f"workload {workload}")
        

        # script_path = os.path.abspath(__file__)
        # script_dir = os.path.dirname(script_path)
        # # read from the logged queries recorded by the match_diagnose_knowledge function
        # workload_file = script_dir + \
        #                 f"/index_selection/selection_data/data_info/job_templates.sql"
        # workload = list()
        # with open(workload_file, "r") as rf:
        #     for line in rf.readlines():
        #         workload.append(line.strip())

        indexes, total_no_cost, total_ind_cost = get_index_result(advisor, workload, connector, columns, column_sampled_values)

        # if len(indexes) != 0:
        #     index_advice += (
        #         f"对数据库{dbname}，推荐的索引是：{indexes}，cost从原来的{total_no_cost}减少到{total_ind_cost}。\n" if LANGUAGE == "zh"
        #         else f"\t For {dbname}, the recommended indexes are: {indexes}, which reduces cost from {total_no_cost} to {total_ind_cost}.\n"
        #     )
        if len(indexes) != 0:
            index_advice += (
                f"对数据库{dbname}，推荐的索引是：{indexes}。\n" if LANGUAGE == "zh"
                else f"\t For {dbname}, the recommended indexes are: {indexes}.\n"
            )

    return index_advice



if __name__ == '__main__':
    kwargs={'start_time': '2023-10-15 23:09:49', 'end_time': '2023-10-15 23:12:49'}
    index_advice=optimize_index_selection(**kwargs)
    print(index_advice)