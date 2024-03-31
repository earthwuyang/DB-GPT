# -*- coding: utf-8 -*-
# @Project: index_eab
# @Module: drop_algorithm
# @Author: Wei Zhou
# @Time: 2023/12/12 16:52

import logging

from index_eab.index_advisor.selection_algorithm import DEFAULT_PARAMETER_VALUES, SelectionAlgorithm

from index_eab.eab_utils.common_utils import mb_to_b, b_to_mb, get_utilized_indexes
from index_eab.eab_utils.candidate_generation import candidates_per_query, syntactically_relevant_indexes

# max_indexes: The algorithm stops as soon as it has selected `#max_indexes` indexes
DEFAULT_PARAMETERS = {"max_indexes": DEFAULT_PARAMETER_VALUES["max_indexes"]}


# This algorithm is a reimplementation of the Drop heuristic proposed by Whang in 1985.
# Details can be found in the original paper:
# Kyu-Young Whang: Index Selection in Relational Databases. FODO 1985: 487-500
class DropAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters=None, process=False,
                 cand_gen=None, is_utilized=None, sel_oracle=None):
        if parameters is None:
            parameters = {}
        SelectionAlgorithm.__init__(
            self, database_connector, parameters, DEFAULT_PARAMETERS,
            process, cand_gen, is_utilized, sel_oracle
        )
        self.max_indexes = self.parameters["max_indexes"]

        self.budget = mb_to_b(self.parameters["budget_MB"])
        self.constraint = self.parameters["constraint"]
        self.multi_column = self.parameters["multi_column"]

    def _calculate_best_indexes(self, workload, db_conf=None, columns=None):
        assert (
                self.max_indexes > 0
        ), "Calling the DropHeuristic with max_indexes < 1 does not make sense."
        logging.info("Calculating best indexes (drop heuristic)")
        logging.info("Parameters: " + str(self.parameters))

        # remaining_indexes is initialized as a set of all potential indexes
        if self.multi_column:
            if self.cand_gen is None or self.cand_gen == "permutation":
                candidates_query = candidates_per_query(
                    workload,
                    self.parameters["max_index_width"],
                    candidate_generator=syntactically_relevant_indexes,
                )

            if self.parameters["max_index_width"] > 1 and self.is_utilized:
                # Obtain the utilized indexes considering every single query
                candidates_query, _ = get_utilized_indexes(workload, candidates_query, self.cost_evaluation)
                remaining_indexes = candidates_query
            else:
                remaining_indexes = list()
                for cands in candidates_query:
                    remaining_indexes.extend(cands)

            remaining_indexes = set(sorted(remaining_indexes))

        # single column index
        else:
            remaining_indexes = set(workload.potential_indexes())

            if self.is_utilized:
                # Obtain the utilized indexes considering every single query
                candidates_query, _ = get_utilized_indexes(workload, [remaining_indexes], self.cost_evaluation)

        if self.constraint == "storage":
            cost = self.cost_evaluation.calculate_cost(
                workload, remaining_indexes
                , store_size=True  # newly added.
            )

            remaining_indexes = set([index for index in remaining_indexes if index.estimated_size < self.budget])

        if self.process:
            self.step["candidates"] = remaining_indexes

        init_cost = self.cost_evaluation.calculate_cost(
            workload, remaining_indexes
            , store_size=True 
        )

        while True:
            if self.constraint == "number":
                if len(remaining_indexes) <= self.max_indexes:
                    break
                    
            elif self.constraint == "storage":
                total_size = sum(index.estimated_size for index in remaining_indexes)
                if total_size <= self.budget:
                    break

            if self.process:
                self.step[self.layer] = list()

            # Drop index that, when dropped, leads to lowest cost
            lowest_cost = None
            index_to_drop = None
            index_to_drop_no = None
            for no, index in enumerate(remaining_indexes):
                if self.constraint == "number":
                    cost = self.cost_evaluation.calculate_cost(
                        workload, remaining_indexes - set([index])
                        , store_size=True
                    )
                elif self.constraint == "storage":
                    cost = self.cost_evaluation.calculate_cost(
                        workload, remaining_indexes - set([index])
                        , store_size=True
                    )

                if self.sel_oracle == "cost_per_sto":
                    cost = cost * b_to_mb(index.estimated_size)
                elif self.sel_oracle == "benefit_per_sto":
                    current_cost = self.cost_evaluation.calculate_cost(workload, remaining_indexes)
                    cost = -1 * (current_cost - cost) / b_to_mb(index.estimated_size)
                elif self.sel_oracle == "benefit_pure":
                    current_cost = self.cost_evaluation.calculate_cost(workload, remaining_indexes)
                    cost = -1 * (current_cost - cost)

                if not lowest_cost or cost < lowest_cost:
                    lowest_cost, index_to_drop, index_to_drop_no = cost, index, no

                if self.process:
                    self.step[self.layer].append({"combination": remaining_indexes - set([index]),
                                                  "candidate": index,
                                                  "oracle": cost})
            remaining_indexes.remove(index_to_drop)

            if self.process:
                self.step["selected"].append(index_to_drop_no)
                self.layer += 1

            logging.info(
                (
                    f"Dropping Index: {index_to_drop}. "
                    f"{len(remaining_indexes)} indexes remaining."
                )
            )

        return remaining_indexes
