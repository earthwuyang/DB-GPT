# -*- coding: utf-8 -*-
# @Project: index_test
# @Module: test_eab_package
# @Author: Wei Zhou
# @Time: 2024/1/1 0:55

import json
import sys
sys.path.insert(0,'../')
from index_eab.eab_utils.workload import Workload
from index_eab.eab_utils.common_utils import get_columns_from_schema, read_row_query
from index_eab.eab_utils.postgres_dbms import PostgresDatabaseConnector

from index_eab.index_advisor.extend_algorithm import ExtendAlgorithm
from index_eab.index_advisor.drop_algorithm import DropAlgorithm
# from index_eab.index_advisor.anytime_algorithm import AnytimeAlgorithm
# from index_eab.index_advisor.db2advis_algorithm import DB2AdvisAlgorithm
# from index_eab.index_advisor.auto_admin_algorithm import AutoAdminAlgorithm
# from index_eab.index_advisor.relaxation_algorithm import RelaxationAlgorithm
# from index_eab.index_advisor.cophy_algorithm import CoPhyAlgorithm


def test_case():
    # 1. Configuration Setup
    host = "127.0.0.1"
    port = "5432"
    user = "wuy"
    password = ""
    db_name = "indexselection_tpch___0_1"

    connector = PostgresDatabaseConnector(autocommit=True, host=host, port=port,
                                          db_name=db_name, user=user, password=password)

    # 2. Data Preparation
    schema_load = "/home/wuy/DB/Index_EAB/configuration_loader/database/schema_tpch.json"
    with open(schema_load, "r") as rf:
        schema_list = json.load(rf)
    _, columns = get_columns_from_schema(schema_list)

    work_load = "/home/wuy/DB/Index_EAB/workload_generator/template_based/tpch_work_temp_multi_freq.json"
    with open(work_load, "r") as rf:
        work_list = json.load(rf)


    for work in work_list:
        workload = Workload(read_row_query(work, columns,
                                           varying_frequencies=True, seed=666))

        # 3. Index Advisor Evaluation
        config = {"budget_MB": 500, "max_index_width": 2, "max_indexes": 5, "constraint": "storage"}
        index_advisor = ExtendAlgorithm(connector, config)

        indexes = index_advisor.calculate_best_indexes(workload, columns=columns)

        print(indexes)
        break

    return indexes


if __name__ == "__main__":
    indexes = test_case()
    # print(indexes)
