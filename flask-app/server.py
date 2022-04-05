from flask import Flask, request
from pydeephaven import Session

import time
import sys

app = Flask(__name__)
session = None

#Simple retry loop in case the server tries to launch before Deephaven is ready
count = 0
max_count = 5
while (count < max_count):
    try:
        session = Session(host="envoy") #"envoy" is the host within the docker application
        count = max_count
    except:
        print("Failed to connect to Deephaven... Waiting to try again")
        time.sleep(2)
        count += 1

if session is None:
    sys.exit(f"Failed to connect to Deephaven after {max_count} attempts")

#Initializes Deephaven with the table and an update method.
#The session.run_script() method is used to execute Python code in Deephaven.
init = """
from deephaven import DynamicTableWriter
from deephaven.time import to_datetime, now
import deephaven.dtypes as dht

prometheus_alerts_table_writer = DynamicTableWriter({
    "PrometheusDateTime": dht.DateTime,
    "Job": dht.string,
    "Instance": dht.string,
    "PrometheusQuery": dht.string,
    "Status": dht.string,
    "AlertIngestDateTime": dht.DateTime
})
prometheus_alerts = prometheus_alerts_table_writer.table

def update_prometheus_alerts(date_time_string, job, instance, prometheus_query, status):
    date_time = to_datetime(date_time_string)
    prometheus_alerts_table_writer.write_row(date_time, job, instance, prometheus_query, status, now())
"""
session.run_script(init)

#Template to trigger the table update
update_template = """
update_prometheus_alerts("{date_time_string}", "{job}", "{instance}", "{prometheus_query}", "{status}")
"""

#Template to join the 2 dynamic tables on time stamps
join_tables_on_time_stamps = """
prometheus_alerts_metrics = prometheus_alerts.aj(table=prometheus_metrics, on=["Job", "Instance", "PrometheusQuery", "PrometheusDateTime"], joins=["Value", "MetricTimeStamp = PrometheusDateTime"])
"""

setup_scripts_executed = False

#Script to generate plots
plots_script = """
from deephaven.plot.figure import Figure

cat_hist_plot = Figure().plot_cat_hist(series_name="Count By Category", t=prometheus_alerts.where(["Status = `firing`"]), category="PrometheusQuery").chart_title(title="Alert Count By Category").show()
pie_plot = Figure().plot_pie(series_name="Percentage By Category", t=prometheus_alerts.where(["Status = `firing`"]).count_by(col="Status", by=["PrometheusQuery"]), category="PrometheusQuery", y="Status").chart_title(title="% Of Alerts By Category").show()

line_plot = Figure().plot_xy(series_name="go_memstats_alloc_bytes", t=prometheus_metrics.where(["PrometheusQuery = `go_memstats_alloc_bytes`"]), x="PrometheusDateTime", y="Value")\
    .plot_xy(series_name="go_memstats_heap_idle_bytes", t=prometheus_metrics.where(["PrometheusQuery = `go_memstats_heap_idle_bytes`"]), x="PrometheusDateTime", y="Value")\
    .plot_xy(series_name="go_memstats_frees_total", t=prometheus_metrics.where(["PrometheusQuery = `go_memstats_frees_total`"]), x="PrometheusDateTime", y="Value")\
    .x_twin()\
    .plot_xy(series_name="go_memstats_alloc_bytes alarm", t=prometheus_alerts_metrics.where(["PrometheusQuery = `go_memstats_alloc_bytes`"]).update(["Alarm = Status.equals(`firing`) ? 1 : 0"]), x="PrometheusDateTime", y="Alarm")\
    .plot_xy(series_name="go_memstats_heap_idle_bytes alarm", t=prometheus_alerts_metrics.where(["PrometheusQuery = `go_memstats_heap_idle_bytes`"]).update(["Alarm = Status.equals(`firing`) ? 1 : 0"]), x="PrometheusDateTime", y="Alarm")\
    .plot_xy(series_name="go_memstats_frees_total alarm", t=prometheus_alerts_metrics.where(["PrometheusQuery = `go_memstats_frees_total`"]).update(["Alarm = Status.equals(`firing`) ? 1 : 0"]), x="PrometheusDateTime", y="Alarm")\
    .show()

"""

@app.route('/', methods=['POST'])
def receive_alert():
    request_json = request.json
    date_time_string = None
    job = None
    instance = None
    prometheus_query = None
    status = None

    #For every alert, build the method call to update the alerts table
    for alert in request_json["alerts"]:
        status = alert["status"]
        #Dates come in the format yyyy-mm-ddThh:mm:ss.sssZ, we need to
        #convert to yyyy-mm-ddThh:mm:ss.sss TZ for Deephaven
        if status == "firing":
            date_time_string = alert["startsAt"][0:-1] + " UTC"
        elif status == "resolved":
            date_time_string = alert["endsAt"][0:-1] + " UTC"
        job = alert["labels"]["job"]
        instance = alert["labels"]["instance"]
        prometheus_query = alert["labels"]["alertname"]

        #Executes the alert table update in Deephaven
        session.run_script(update_template.format(date_time_string=date_time_string, job=job, instance=instance,
                prometheus_query=prometheus_query, status=status))

    #If we haven't executed the setup scripts, run those
    global setup_scripts_executed
    if not setup_scripts_executed:
        session.run_script(join_tables_on_time_stamps)
        session.run_script(plots_script)
        setup_scripts_executed = True

    return "Request received"

app.run(port=5000, host="0.0.0.0")
