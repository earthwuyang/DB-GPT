import os
import sys
import copy
import json
import logging
import pickle
import argparse
import configparser
import pdb
import re


from .workload import Workload, Table, Column, Query
from .selec_const import job_table_alias


def get_parser():
    parser = argparse.ArgumentParser(
        description="the testbed of Index Selection Algorithms.")

    parser.add_argument("--exp_id", type=str, default="new_exp")
    parser.add_argument("--sel_params", type=str, default="parameters")
    parser.add_argument("--exp_conf_file", type=str,
                        default="../data_resource/selection_exp_conf/tpch_1gb_config.json")
    parser.add_argument("--work_type", type=str, default="not_template")
    parser.add_argument("--work_file", type=str,
                        default="/data/wz/index/attack/tpch_template_21.sql")

    parser.add_argument("--log_file", type=str,
                        default="./exp_res/{}/exp_runtime.log")
    parser.add_argument("--index_save_file", type=str,
                        default="./exp_res/{}/index_res.log")
    parser.add_argument("--res_save_file", type=str,
                        default="./exp_res/{}/sql_res.json")
    parser.add_argument("--db_conf_file", type=str,
                        default="../data_resource/db_info/db.conf")
    parser.add_argument("--schema_file", type=str,
                        default="../data_resource/db_info/schema_tpch_1gb.json")

    return parser


def get_conf(conf_file):
    conf = configparser.ConfigParser()
    conf.read(conf_file)

    return conf


def parse_command_line_args():
    arguments = sys.argv
    if "CRITICAL_LOG" in arguments:
        logging.getLogger().setLevel(logging.CRITICAL)
    if "ERROR_LOG" in arguments:
        logging.getLogger().setLevel(logging.ERROR)
    if "INFO_LOG" in arguments:
        logging.getLogger().setLevel(logging.INFO)
    for argument in arguments:
        if ".json" in argument:
            return argument


def set_logger(log_file):
    # logging.basicConfig(
    #     filename=log_file,
    #     filemode='w',
    #     format='%(asctime)s - %(levelname)s - %(message)s')

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s: - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    # log to file
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    # log to console
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)


def find_parameter_list(algorithm_config, params="parameters"):
    """
    # Parameter list example: {"max_indexes": [5, 10, 20]}
    # Creates config for each value.

    :param algorithm_config:
    :param params:
    :return:
    """
    parameters = algorithm_config[params]
    configs = []
    if parameters:
        # if more than one list --> raise
        # Only support one param list in each algorithm.
        counter = 0
        for key, value in parameters.items():
            if isinstance(value, list):
                counter += 1
        if counter > 1:
            raise Exception("Too many parameter lists in config.")

        for key, value in parameters.items():
            if isinstance(value, list):
                for i in value:
                    new_config = copy.deepcopy(algorithm_config)
                    new_config["parameters"][key] = i
                    configs.append(new_config)
    if len(configs) == 0:
        configs.append(algorithm_config)

    return configs


def get_columns_from_db(db_connector):
    # db_connector.create_connection()

    tables, columns = list(), list()
    for table in db_connector.get_tables():
        table_object = Table(table)
        tables.append(table_object)
        for col in db_connector.get_cols(table):
 
            sampled_values = db_connector.get_sampled_values(col, table)
            column_object = Column(col, sampled_values)
            table_object.add_column(column_object)
            columns.append(column_object)

    # db_connector.close()

    return tables, columns


# def get_columns_from_schema(schema_file):
#     tables, columns = list(), list()
#     with open(schema_file, "r") as rf:
#         db_schema = json.load(rf)

#     for item in db_schema:
#         table_object = Table(item["table"])
#         tables.append(table_object)
#         for col_info in item["columns"]:

#             column_object = Column(col_info["name"])
#             table_object.add_column(column_object)
#             columns.append(column_object)

#     return tables, columns


def replace_placeholders(sql_string, substring, sampled_values):
    # Define the regex pattern:
    # 1. Locate the substring (e.g. "WHERE")
    # 2. Followed by any number of whitespace characters (using \s*)
    # 3. An operator (+/-/*/=/>/<)
    # 4. Followed by any number of whitespace characters (using \s*)
    # 5. A placeholder (e.g. $2, $3, etc.)
    print(f"in replace_placeholders: {sql_string}: {sampled_values}")
    pattern = re.escape(substring) + r"\s*([\+\-*/=><])\s*(\$\d+)"

    # Replace placeholders with value 2 using re.sub
    def repl(match):
        if len(sampled_values) > 1:
            return substring + ' ' + match.group(1) + ' ' + str(sampled_values[0])
        else:
            return substring + ' ' + match.group(1) + ' ' + str(2)

    return re.sub(pattern, repl, sql_string)

def read_row_query(sql_list, exp_conf, columns, type="template"):
    workload = list()
    for query_id, query_text in enumerate(sql_list):
        if type == "template" and exp_conf["queries"] \
                and query_id + 1 not in exp_conf["queries"]:
            continue
        
        query = Query(query_id, query_text)
        for column in columns:
            # column_tmp = [col for col in columns if column.name == col.name]
            if column.name in query.text['sql'].lower() and \
                    f"{column.table.name}" in query.text['sql'].lower():
                # column.name

                query.text['sql'] = replace_placeholders(query.text['sql'].lower(), column.name, column.sampled_values)
                print(f"query text {query.text['sql']}")

                query.columns.append(column)

            # else:
            #     # if column.name in query.text and column.table.name in query.text:
            #     # if " " + column.name + " " in query.text and column.table.name in query.text:
            #     # todo(0329): newly modified. for JOB,
            #     #  SELECT COUNT(*), too many candidates.
            #     if "." in query.text['sql'].lower().split("from")[0] or \
            #             ("where" in query.text['sql'].lower() and (
            #                     "." in query.text['sql'].lower().split("where")[0] or
            #                     "." in query.text['sql'].lower().split("where")[-1].split(" ")[1])):
            #         if str(column) in query.text['sql'].lower():
            #             query.columns.append(column)
            #         if " as " in query_text.lower():
            #             tbl, col = str(column).split(".")
            #             if f" {job_table_alias[tbl]}.{col}" in query.text['sql'].lower() \
            #                     or f"({job_table_alias[tbl]}.{col}" in query.text['sql'].lower():
            #                 query.columns.append(column)
            #     # else:
                #     # todo(0408): newly added. check?
                #     # if column.name in query.text:
                #     if column.name in query.text['sql'].lower() and \
                #             f"{column.table.name}" in query.text['sql'].lower():
                #         query.columns.append(column)

            # todo(0408): newly added. check? (different table, same column name)
            # if column.name in query.text['sql'].lower() and \
            #         column.table.name in query.text['sql'].lower():
            #     query.columns.append(column)
            # if column.name in query.text:
            #     query.columns.append(column)

        workload.append(query)
    logging.info("Queries read.")
    return workload


# --- Unit conversions ---
# Storage
def b_to_mb(b):
    """
    1024?
    :param b:
    :return:
    """
    return b / 1000 / 1000


def mb_to_b(mb):
    return mb * 1000 * 1000


# Time
def s_to_ms(s):
    return s * 1000


# --- Index selection utilities ---
def indexes_by_table(indexes):
    indexes_by_table = {}
    for index in indexes:
        table = index.table()
        if table not in indexes_by_table:
            indexes_by_table[table] = []

        indexes_by_table[table].append(index)

    return indexes_by_table


def get_utilized_indexes(
        workload, indexes_per_query, cost_evaluation, detailed_query_information=False
):
    utilized_indexes_workload = set()
    query_details = {}
    
    for query, indexes in zip(workload.queries, indexes_per_query):

        if '$' not in query.text['sql']:
            sql = query.text['sql']
            (
                utilized_indexes_query,
                cost_with_indexes,
            ) = cost_evaluation.which_indexes_utilized_and_cost(sql, indexes)
            utilized_indexes_workload |= utilized_indexes_query

            if detailed_query_information:
                cost_without_indexes = cost_evaluation.calculate_cost(
                    Workload([sql]), indexes=[]
                )
                
                query_details[sql] = {
                    "cost_without_indexes": round(cost_without_indexes * query.text['frequency'], 2),
                    "cost_with_indexes": round(cost_with_indexes * query.text['frequency'], 2),
                    "utilized_indexes": utilized_indexes_query,
                }

    return utilized_indexes_workload, query_details
