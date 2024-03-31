
ANOMALY_DESC_PROMPT='''
Please describe the following anomaly event in natural language:  
{anomaly_str}

Note do not miss any important information in each line of the event. And the description templates should be like: 

1. "During the inspection, it was found that from xx to xx time, the database's CPU usage was relatively high, approximately 80% to 90%. It is a *critical problem*. I should next enrich the alert information with more details, such as related processes and concurrent system activities that might contribute to the anomaly, as well as  workloads and slow queries during the period of anomaly."

2. "During the inspection, it was found that from xx to xx time, the database's CPU usage was relatively high, approximately 80% to 90%. It is just a *warning*. I should next enrich the alert information with more details, such as related processes and concurrent system activities that might contribute to the anomaly, as well as  workloads and slow queries during the period of anomaly."
'''



ANOMALY_TITLE_PROMPT='''
Please give a title for the following anomaly event within 15 words:
{anomaly_str}

Note the title template is like: 
Analysis Report of High CPU Usage
'''


SUMMARY_PROMPT='''
Based on knowledge {knowledge_str} given by experts by searching knowledge base, please describe the root cause of the anomaly {anomaly_desc} and give potential solutions.
'''