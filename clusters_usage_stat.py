import os
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.sdk.api_client import ApiClient
from datetime import datetime,timedelta,date
import pandas as pd
from tabulate import tabulate


class ClusterUsage:

  def __init__(self) -> None:
    self.init_client()

  def init_client(self):
    from dotenv import load_dotenv
    load_dotenv(verbose=True)
    api_client = ApiClient(
      host  = os.getenv('host'),
      token = os.getenv('token')
    )
    self.clusters_api  = ClusterApi(api_client)
    return self
    
  def get_clusters(self, names):
    if not self.clusters_api:
      self.init_client()
    clusters=[]
    if names and len(names):
      for name in names:
        cluster = self.clusters_api.get_cluster_by_name(name)
        clusters.append(cluster)
    else:
      clusters = self.clusters_api.list_clusters()
      clusters = [cluster for cluster in clusters["clusters"] if not cluster["cluster_name"].startswith("job-") ]
    self.clusters = clusters
    return self

  def get_events(self, start_date, end_date):
    if not self.clusters:
      self.get_clusters()
    start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    start_time = str(int(start_datetime.timestamp())) + "000"
    end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
    #add one day to make to then end of day
    end_datetime+=timedelta(days=1)
    end_time = str(int(end_datetime.timestamp())) + "000"

    results = []
    for cluster in self.clusters:
      events = self.clusters_api.get_events(cluster_id=cluster["cluster_id"],\
        start_time=start_time, end_time=end_time, order="ASC", event_types=["TERMINATING","STARTING"], \
          offset=0, limit=500)
      
      for event in events["events"]:
        timestamp = event["timestamp"]
        type=event["type"]
        if "user" in event["details"]:
          user=event["details"]["user"]
        else:
          user="INACTIVITY"

        results.append({"cluster_name":cluster["cluster_name"],"timestamp":timestamp, "type":type,"user":user})

    self.cluster_events = pd.DataFrame(results)
    return self
    
  def aggregate(self, group_by):
    if self.cluster_events is None :
      raise Exception("no events to aggregate!")

    # to by-pass the "pandasql.sqldf.PandaSQLException: (sqlite3.OperationalError) no such table: cluster_events" error
    cluster_events = self.cluster_events
    
    from pandasql import sqldf
    # pysqldf = lambda q: sqldf(q, locals())

    cols = len(group_by.split(","))
    group_by_clause = ",".join([str(i) for i in range(1,cols+1)])
    df = sqldf(f"""
    with src_table as (
      select cluster_name, type, user, timestamp,
      lag(timestamp,1,0) over (partition by cluster_name order by timestamp asc) as start_time, 
      last_value(type) over (partition by cluster_name order by timestamp asc RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING) as latest_state,
      last_value(timestamp) over (partition by cluster_name order by timestamp asc RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING) as latest_stamp,
      lag(user,1,"") over (partition by cluster_name order by timestamp asc) as previous_user
      from cluster_events
    )
    ,
    derived_table as (
      select cluster_name, type, user as event_user, start_time, timestamp, latest_state, latest_stamp,
      case 
        when type='TERMINATING' then (timestamp-start_time)/(1000 * 60)
        when (type='STARTING' and latest_stamp=timestamp) then (strftime('%s', 'now') * 1000 - timestamp)/(1000 * 60)
        else 0
      end as mins,
      case 
        when type='TERMINATING' then previous_user
        else user
      end as user
      from src_table 
    )
    select {group_by},printf("%.2f",CAST(sum(mins) as REAL)/60) as hours from derived_table group by {group_by_clause} order by hours desc;
    """, locals())
    self.aggregated_events = df
    return self

  def pretty_result(self, tablefmt='psql'):
    s = tabulate(self.aggregated_events, headers='keys', tablefmt=tablefmt,showindex=False)
    print(s)
    return s

  def distinct_users(self):
    if self.aggregated_events is None:
      raise Exception("no aggregated result!")

    aggregated_events = self.aggregated_events
    from pandasql import sqldf
    df = sqldf(f"""
      select distinct user from aggregated_events
    """, locals())
    return df["user"].tolist()

class EmailUtils:

  @staticmethod
  def send_email(_to, _from, subject, body):
    import smtplib
    from email.message import EmailMessage
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    # msg = EmailMessage()
    # msg.set_content(body)
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = _from
    msg['To'] = _to

    msg.attach(MIMEText(content, 'html'))

    s = smtplib.SMTP('localhost')
    s.send_message(msg)
    s.quit()

    return True

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser()

  parser.add_argument("--start_date", required=False, 
    help="start date to retrieve usage data, e.g: 2022-12-31",
    default="")

  parser.add_argument("--end_date", required=False, 
    help="end date to retrieve usage data (inclusive), e.g: 2022-12-31",
    default="")
  
  parser.add_argument("--clusters", required=False, 
    help="cluster names for the statistic, more than one clusters need to be seprated by comma. e.g: cluster_a,cluster_b",
    default="")
  
  parser.add_argument("--group_by",required=False, 
    help="statistic data group by, supported colomns include cluster_name, user (the one who start cluster)", 
    default="user,cluster_name")

  parser.add_argument("--send_email",required=False, 
    help="if to send email to users listed in the usage result, true or false", 
    default="false")

  parser.add_argument("--report_fmt",required=False, 
    help="format for the usage report", 
    default="psql")

  args = parser.parse_args()
  if args.start_date == "":
    week_day = datetime.weekday(datetime.now())
    monday = datetime.now() - timedelta(days=week_day)
    args.start_date = monday.strftime("%Y-%m-%d")
  if args.end_date == "":
    args.end_date = date.today().strftime("%Y-%m-%d")

  cluster_names = []
  if args.clusters and len(args.clusters) > 0:
    cluster_names = args.clusters.split(",")
  
  cluster_usage = ClusterUsage()
  cluster_usage.get_clusters(cluster_names)\
    .get_events(start_date=args.start_date, end_date=args.end_date)\
    .aggregate(group_by=args.group_by)

  content = cluster_usage.pretty_result(tablefmt=args.report_fmt)

  content = f"""
  Usage from {args.start_date} to {args.end_date}:

  {content}
  """

  if args.send_email == "true":
    users = cluster_usage.distinct_users()
    _to = ",".join(users)
    EmailUtils.send_email(_to, "databricks", "databricks weekly usage report", content)