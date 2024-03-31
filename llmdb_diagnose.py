
from llmdb.llmdb_parser import args
import time
import logging
import re
import json
from pydantic import BaseModel

from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from langchain.llms import OpenAI
from langchain.memory import ConversationBufferMemory

# Agent imports
from langchain.agents import load_tools
from langchain.agents import initialize_agent

# Tool imports
from langchain.agents import Tool

from langchain.tools.retriever import create_retriever_tool
from langchain.agents import AgentExecutor

# from langchain.chat_models import ChatOpenAI
# from langchain.schema import AIMessage, HumanMessage, SystemMessage

from llmdb.utils import extract_alert_info
from llmdb.metrics import current_diag_time, WORKLOAD_FILE_NAME, obtain_values_of_metrics
from llmdb.metric_monitor.api import whether_is_abnormal_metric, match_diagnose_knowledge

# from multiagents.llms.openai import OpenAIChat

from llmdb.prompt_templates import (
    ANOMALY_DESC_PROMPT, ANOMALY_TITLE_PROMPT, SUMMARY_PROMPT
)






def main(args):

    print(
        '<flow>{"title": "Enriching original system alerts to create a comprehensive context for the following analysis", "content": "", "isCompleted": 0, "isRuning": 1}</flow>')
    alert_str = ""
    alert_dict = []

    for alert_info in args.alerts:
        tmp_alert_str, tmp_alert_dict = extract_alert_info(alert_info)
        alert_str = alert_str + \
            f"{len(alert_dict)+1}. " + tmp_alert_str + "\n\n"
        alert_dict.append(tmp_alert_dict)


    anomaly_desc_prompt = PromptTemplate(input_variables=["anomaly_str"], template=ANOMALY_DESC_PROMPT)
    memory=ConversationBufferMemory(memory_key="chat_history")
    llm_chain=LLMChain(llm=OpenAI(model='gpt-3.5-turbo-instruct'), prompt=anomaly_desc_prompt, verbose=False, memory=memory)
    anomaly_desc = llm_chain.predict_and_parse(anomaly_str=alert_str)

    # logging.info(f"anomaly_desc:\n{anomaly_desc}")
    # ''' anomaly_desc:
    # During the examination, it was found that from 09:14:49 to 09:15:49 on 15th October 2023, the node:ins:stdload1[ins=] was critically destabilized as its load exceeded its limit by 75%. This anomaly was beyond 100% of the normal level, equating to 1.75 on the scale, which triggered a warning (WARN level alert). The alert has since been resolved. To further analyze this anomaly, information such as related processes, concurrent system activities during the anomaly, as well as workloads and slow queries should be referenced and examined to gain more insight into the root cause of this anomaly.
    # '''

    # logging.info(f"alert_dict:\n{alert_dict}")  # [{'alert_name': 'NodeLoadHigh', 'alert_status': 'resolved', 'alert_level': 'WARN', 'alert_desc': 'node:ins:stdload1[ins=] = 1.75 > 100%', 'alert_exporter': '172.27.58.65:9100', 'start_time': '1697332489', 'end_time': '1697332549'}]
    # logging.info(f"alert_str:\n{alert_str}")
    # ''' alert_str:
    # 1. Alert Starts At: 2023-10-15 09:14:49
    # Alert Ends At: 2023-10-15 09:15:49
    # Alert Status: resolved
    # Alert Description: node:ins:stdload1[ins=] = 1.75 > 100%
    # Alert Level: WARN
    # '''

    print(f'<flow>{{"title": "Enriching original system alerts to create a comprehensive context for the following analysis", "content": "Alert information enriched", "isCompleted": 1, "isRuning": 0}}</flow>')
    
    print(f"- 调用工具API {'match_diagnose_knowledge'}\n")
    knowledge_str, abnormal_metric_detailed_values = match_diagnose_knowledge(start_time=alert_dict[0]['start_time'], end_time=alert_dict[0]['end_time'], metric_name='CpuExpert', alert_metric="", diag_id="0", enable_prometheus=False)
    print(f"{knowledge_str}") 
    summaryprompt=PromptTemplate(input_variables=["knowledge_str", "anomaly_desc"], template=SUMMARY_PROMPT)
    llm_chain2=LLMChain(llm=OpenAI(model='gpt-3.5-turbo-instruct'), prompt=summaryprompt, verbose=False)
    summary = llm_chain2.predict_and_parse(anomaly_desc=anomaly_desc, knowledge_str=knowledge_str)
    print(f"### summary ###\n")
    print(summary)

    # TODO: need more prompts to let LLM review and reflect on their proposed analysis and solutions.
    # e.g. In summary, the diagnosis and solutions proposed are generally correct, but could benefit from more detailed explanations and context to make them more understandable and actionable.
    


if __name__ == "__main__":

    # read from the anomalies with alerts. for each anomaly,
    with open(args.anomaly_file, "r") as f:
        anomaly_json = json.load(f)

    args.start_at_seconds = anomaly_json["start_time"]
    args.end_at_seconds = anomaly_json["end_time"]

    slow_queries = []
    workload_statistics = []
    workload_sqls = ""

    if args.enable_slow_query_log == True:
        # [slow queries] read from query logs
        # /var/lib/pgsql/12/data/pg_log/postgresql-Mon.log
        slow_queries = anomaly_json["slow_queries"]
    if args.enable_workload_statistics_view == True:
        # workload_statistics = db.obtain_historical_queries_statistics(topn=50)
        with open(WORKLOAD_FILE_NAME, 'r') as f:
            workload_statistics = json.load(f)["workload_statistics"]
    if args.enable_workload_sqls == True:
        workload_sqls = anomaly_json["workload"]

    if "alerts" in anomaly_json and anomaly_json["alerts"] != []:
        args.alerts = anomaly_json["alerts"]  # possibly multiple alerts for a single anomaly
    else:
        args.alerts = []

    if "labels" in anomaly_json and anomaly_json["labels"] != []:
        args.labels = anomaly_json["labels"]
    else:
        args.labels = []
    args.start_at_seconds = anomaly_json["start_time"]
    args.end_at_seconds = anomaly_json["end_time"]
    args.diag_id = "0"

    # kwargs={'start_time': '2023-10-15 23:09:49', 'end_time': '2023-10-15 23:12:49'}
    # index_advice=optimize_index_selection(**kwargs)
    # print(index_advice)
    # exit()

    # count the time to run main function
    start_time = time.time()
    main(args)
    end_time = time.time()
    print(f"****Diagnose Finished!****\n****During Time{current_diag_time}****")
