from llmdb.metric_monitor.anomaly_detection import detect_anomalies
from prometheus_service.prometheus_abnormal_metric import prometheus_metrics
from llmdb.metrics import *
from multiagents.utils.markdown_format import generate_prometheus_chart_content
from server.knowledge_base.kb_doc_api import search_docs, fetch_expert_kb_names
from multiagents.initialization import LANGUAGE


def metric_analysis_results(agent_name, kb_name, alert_metric, diag_id, enable_prometheus, start_time, end_time):

    if "cpu" in agent_name or "workload" in agent_name or "index" in agent_name or "query" in agent_name:
        metric_prefix = "cpu"
    elif "write" in agent_name or "io" in agent_name:
        metric_prefix = "io"
    elif "mem" in agent_name or "write" in agent_name:
        metric_prefix = "memory"
    else:
        metric_prefix = "cpu"

    metrics_list = prometheus_metrics[f"{metric_prefix}_metrics"]
    alert_metric_list = []
    alert_metric_list.append(alert_metric)
    if enable_prometheus == True:
        detailed_alert_metric = obtain_values_of_metrics(-1,
            alert_metric_list, int(start_time), int(end_time))

        detailed_metrics = obtain_values_of_metrics(-1,
            metrics_list, int(start_time), int(end_time))
    else:  # obtain alert metrics from testing_case.json
        detailed_alert_metric = obtain_values_of_metrics(diag_id,
            alert_metric_list, -1, 1)  

        detailed_metrics = obtain_values_of_metrics(diag_id,
            metrics_list, -1, 1)

    # identify the abnormal metrics
    top5_abnormal_metrics = {}
    top5_abnormal_metrics_map = {}
    for metric_name, metric_values in detailed_metrics.items():
        anomaly_value, is_abnormal = detect_anomalies(np.array(metric_values))
        if is_abnormal:
            # maintain the top 5 abnormal metrics
            if len(top5_abnormal_metrics) < 5:
                top5_abnormal_metrics[metric_name] = processed_values(metric_values, language=LANGUAGE)
                top5_abnormal_metrics_map[metric_name] = anomaly_value
                # sort top5_abnormal_metrics_map by keys in descending order
            else:
                # identify the min value of top5_abnormal_metrics_map together with the key
                min_abnormal_value = min(top5_abnormal_metrics_map.values())
                # identify the key of min_abnormal_value
                min_abnormal_value_key = list(top5_abnormal_metrics_map.keys())[list(top5_abnormal_metrics_map.values()).index(min_abnormal_value)]

                if anomaly_value > min_abnormal_value:

                    top5_abnormal_metrics[metric_name] = processed_values(metric_values, language=LANGUAGE) # convert time-series data into feature values

                    top5_abnormal_metrics.pop(min_abnormal_value_key)
                    top5_abnormal_metrics_map.pop(min_abnormal_value_key)

    abnormal_metric_detailed_values = []
    for metric_name in top5_abnormal_metrics:
        new_metric_name = metric_name.replace("irate(", "")
        metric_chart_data = {"values": detailed_metrics[metric_name], "title": f"{new_metric_name} (for {metric_prefix} expert)", "type": "line"}

        abnormal_metric_detailed_values.append(metric_chart_data)

    if detailed_alert_metric != {} and detailed_alert_metric != None:
        for detailed_values in detailed_alert_metric.values():
            # draw the metric chart
            chart_metric_values = [[i, str(value)] for i, value in enumerate(detailed_values)]
            chart_content = generate_prometheus_chart_content(alert_metric, chart_metric_values, x_label_format="%H:%M", size=(400, 225))
            with open(f"./alert_results/{current_diag_time}/{alert_metric}.html", "w") as f:
                f.write(chart_content)

            if LANGUAGE == "zh":
                alert_metric_str = f"告警指标{alert_metric}的统计信息是：\n [chart] ./alert_results/{current_diag_time}/{alert_metric}.html\n\n"
            else:
                alert_metric_str = f"The statistics of alert metric {alert_metric} are:\n [chart] ./alert_results/{current_diag_time}/{alert_metric}.html\n\n"
    else:
        alert_metric_str = ""

    metric_str = "所有的异常指标是：\n" if LANGUAGE == "zh" else "The abnormal metrics are:\n"
    for i, metric_name in enumerate(top5_abnormal_metrics):

        if metric_name in detailed_metrics:
            metric_values = detailed_metrics[metric_name]

            # draw the metric chart
            chart_metric_values = [[i, str(value)] for i, value in enumerate(metric_values)]
            chart_content = generate_prometheus_chart_content(metric_name, chart_metric_values, x_label_format="%H:%M", size=(400, 225))
            import os
            if not os.path.exists(f'./alert_results/{current_diag_time}'):
                os.mkdir(f'./alert_results/{current_diag_time}')
            with open(f"./alert_results/{current_diag_time}/{metric_name}.html", "w") as f:
                f.write(chart_content)
            if LANGUAGE == "zh":
                metric_str += f"{i+1}. 指标 {metric_name} 的异常部分为： {top5_abnormal_metrics[metric_name]} \n [chart] ./alert_results/{current_diag_time}/{metric_name}.html \n"
            else:
                metric_str += f"{i+1}. {metric_name} contains abnormal patterns: {top5_abnormal_metrics[metric_name]} \n [chart] ./alert_results/{current_diag_time}/{metric_name}.html \n"
    if top5_abnormal_metrics == {}:
        metric_str = ""
    else:
        metric_str = metric_str + "\n"

    docs_query = [metric_name for metric_name in top5_abnormal_metrics]   # node_procs_running, node_procs_blocked, node_entropy_available_bits, node_load1
    matched_docs = search_docs(str(docs_query), knowledge_base_name=kb_name, top_k=5, score_threshold=0.4)

    docs_str = ""
    if matched_docs != []:
        matched_docs_str = ""
        for i, matched_doc in enumerate(matched_docs):
            if 'desc' in matched_doc.metadata:
                matched_docs_str = matched_docs_str + f"{i+1}. {matched_doc.metadata['desc']} \n"
        if LANGUAGE == "zh":
            docs_str = f"匹配到的与上述异常指标相关的知识为：\n{matched_docs_str}\n\n"
        else:
            docs_str = f"The matched knowledge for analyzing above abnormal metrics is:\n{matched_docs_str}\n\n"

    return alert_metric_str + metric_str + docs_str, abnormal_metric_detailed_values



def workload_analysis_results(agent_name, kb_name, diag_id):

    workload_state = get_workload_sqls(diag_id)

    if workload_state != "":
        if LANGUAGE == "zh":
            workload_str = f"工作负载是：\n{workload_state}\n\n"
        else:
            workload_str = f"The workload queries are:\n{workload_state}\n\n"

        matched_docs = search_docs("workload", knowledge_base_name=kb_name, top_k=5, score_threshold=0.4)

        if matched_docs != []:
            docs_str = ""
            if matched_docs != []:
                matched_docs_str = ""
                for i, matched_doc in enumerate(matched_docs):
                    if 'desc' in matched_doc.metadata:
                        matched_docs_str = matched_docs_str + f"{i+1}. {matched_doc.metadata['desc']} \n"
                if LANGUAGE == "zh":
                    docs_str = f"匹配到的与负载相关的知识是：\n{matched_docs_str}\n\n"
                else:
                    docs_str = f"The matched knowledge for analyzing above workload queries is:\n{matched_docs_str}\n\n"

            workload_str = workload_str + docs_str
    else:
        workload_str = ""

    return workload_str


def slow_query_analysis_results(agent_name, kb_name, diag_id):

    # get the slow queries from the anomaly file
    slow_queries = get_slow_queries(diag_id)
    if not isinstance(slow_queries, list):
        slow_queries = []

    if slow_queries != []:
        # concate the sqls in the dict slow_queries into a string

        # concat_slow_queries = "\n".join([f'\t {i+1}. the slow query comes from {sql["dbname"]} database, takes {sql["execution_time"]} seconds, and its statement is "{sql["sql"]}"' for i, sql in enumerate(slow_queries)])
        if LANGUAGE == "zh":
            concat_slow_queries = "\n".join([f'第{i+1}条慢查询是 "{sql}"' for i, sql in enumerate(slow_queries)])
            slow_queries_str = f"所有需要优化的慢查询如下：\n{concat_slow_queries}\n\n"
        else:
            concat_slow_queries = "\n".join([f'{i+1}. The slow query statement is "{sql}"' for i, sql in enumerate(slow_queries)])
            slow_queries_str = f"The slow queries that should be optimized are:\n{concat_slow_queries}\n\n"

        matched_docs = search_docs("slow query", knowledge_base_name=kb_name, top_k=5, score_threshold=0.4)

        if matched_docs != []:
            docs_str = ""
            if matched_docs != []:
                matched_docs_str = ""
                for i, matched_doc in enumerate(matched_docs):
                    if 'desc' in matched_doc.metadata:
                        matched_docs_str = matched_docs_str + f"{i+1}. {matched_doc.metadata['desc']} \n"

                if LANGUAGE == "zh":
                    docs_str = f"匹配到的与慢查询相关的知识是：\n{matched_docs_str}\n\n"
                else:
                    docs_str = f"The matched knowledge for analyzing above slow queries is:\n{matched_docs_str}\n\n"

            slow_queries_str = slow_queries_str + docs_str
        else:
            slow_queries_str = ""
    else:
        slow_queries_str = ""

    return slow_queries_str