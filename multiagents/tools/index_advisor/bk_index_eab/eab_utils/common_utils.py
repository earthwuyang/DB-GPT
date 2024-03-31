# -*- coding: utf-8 -*-
# @Project: index_eab
# @Module: common_utils
# @Author: Wei Zhou
# @Time: 2024/12/17 0:55

import random

from index_eab.eab_utils.workload import Workload, Query, Table, Column


# Storage
def b_to_mb(b):
    return b / 1000 / 1000


def mb_to_b(mb):
    return mb * 1000 * 1000


def get_utilized_indexes(
        workload, indexes_per_query, cost_evaluation, detailed_query_information=False
):
    utilized_indexes_workload = set()
    query_details = {}
    for query, indexes in zip(workload.queries, indexes_per_query):
        (
            utilized_indexes_query,
            cost_with_indexes,
        ) = cost_evaluation.which_indexes_utilized_and_cost(query, indexes)
        utilized_indexes_workload |= utilized_indexes_query

        if detailed_query_information:
            cost_without_indexes = cost_evaluation.calculate_cost(
                Workload([query]), indexes=[]
            )
            # todo: cost_with_indexes > cost_without_indexes, continue.
            query_details[query] = {
                "cost_without_indexes": cost_without_indexes,
                "cost_with_indexes": cost_with_indexes,
                "utilized_indexes": utilized_indexes_query,
            }

    return utilized_indexes_workload, query_details


def get_columns_from_schema(schema_load):
    tables, columns = list(), list()
    for item in schema_load:
        table_object = Table(item["table"])
        tables.append(table_object)
        for col_info in item["columns"]:
            column_object = Column(col_info["name"])
            table_object.add_column(column_object)
            columns.append(column_object)

    return tables, columns


def read_row_query(sql_list, columns, varying_frequencies=False, seed=666):
    random.seed(seed)

    workload = list()
    for query_id, query_text in enumerate(sql_list):
        if isinstance(query_text, list):
            if varying_frequencies:
                freq = query_text[-1]
            else:
                freq = 1
            query = Query(query_text[0], query_text[1], frequency=freq)
        elif isinstance(query_text, str):
            if varying_frequencies:
                freq = random.randint(1, 1000)
            else:
                freq = 1
            query = Query(query_id, query_text, frequency=freq)

        for column in columns:
            if column.name in query.text.lower() and \
                    f"{column.table.name}" in query.text.lower():
                query.columns.append(column)

        workload.append(query)
        print(f"append query {query.text}")

    return workload
