import pandas as pd
import pyodbc
import matplotlib.pyplot as plt
import numpy as np
import plotly.express as px

# function that import "calls.csv" file to the dataframe and return it. Return a dataframe.
def getDataFrame(path):
    csvFile = pd.read_csv(path)
    data = pd.DataFrame(csvFile, columns=['id', 'agentid', 'duration', 'date', 'inbound'])
    return data

# define the connection to the sql server. Return a connection variable.
def connectionDB(driver,server,database):
    # windows autentication
    connection = pyodbc.connect('Driver={'+driver+'};'
                               'Server='+server+';'
                               'Database='+database+';'
                               'Trusted_Connection=yes;')
    return connection

#function that verifies if SQL table already exist on Database. Return True if exists and False if doesn't exists.
def checkTableExists(tablename,cursor):

    cursor.execute("""
        SELECT COUNT(1)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if cursor.fetchone()[0] == 1:
        return True

    return False

#function that create and insert all values on table "calls_history"
def createAndLoadTable(cursor,df,conn):
    #Create table if needed
    sqlCreateTable = "IF NOT EXISTS (SELECT [name] FROM sys.tables WHERE [name] = 'calls_history' ) CREATE TABLE MyDB.dbo.calls_history (id int NOT NULL PRIMARY KEY, AgentId TINYINT NOT NULL, Duration float(2) NOT NULL, Date DATE NOT NULL,Inbound BIT)"

    cursor.execute(sqlCreateTable)
    conn.commit()

    for row in df.itertuples():
        cursor.execute('''
                    INSERT INTO MyDB.dbo.calls_history (id, AgentId, Duration,Date,Inbound)
                    VALUES (?,?,?,?,?)
                    ''',
                       row.id,
                       row.agentid,
                       row.duration,
                       row.date,
                       row.inbound)
    conn.commit()

#function that calculate and draw graph with average per agent
def avgByAgentPlot(cursor):
    sqlAvg = '''SELECT [AgentId],ROUND(AVG(Duration),2) as avgPerAgent
                      FROM [MyDB].[dbo].[calls_history]
                      group by [AgentId]
                      order by avgPerAgent asc'''

    cursor.execute(sqlAvg)

    rows = cursor.fetchall()

    agentBar = []
    avgBar = []

    for row in rows:
        agentBar.append(row[0])
        avgBar.append(float(row[1]))

    agentBar.sort()
    y_pos = np.arange(len(agentBar))

    # Create bars
    plt.bar(y_pos, avgBar)
    plt.xticks(y_pos, agentBar)

    plt.ylabel('Average (Min)')
    plt.xlabel('Agent ID')
    plt.title('Average per Agent')

    plt.grid(True)

    plt.show()

#function that return a dataframe only for Average By Day By Agent
def dfAvgByDayByAgent(conn):
    sqlDayAvg = '''SELECT Date,AgentId,Duration
                      FROM [MyDB].[dbo].[calls_history]
                      order by Date asc'''

    df = pd.read_sql_query(sqlDayAvg, conn)

    return df

#function that calculate and draw interactive graph with average per day per agent
def interactivePlotAvgByDayByAgent(df):
    df['Date'] = pd.to_datetime(df['Date'])

    df_f = df.groupby(['Date', 'AgentId']).mean().reset_index()
    df_f['AgentId'] = df_f['AgentId'].apply(str)

    fig = px.scatter(df_f, x="Date", y="Duration", color="AgentId", hover_name="AgentId",
                     title="<b> Average per Day per Agent </b>")

    fig.update_traces(marker=dict(size=12,
                                  line=dict(width=2, color='black')),
                      selector=dict(mode='markers'))

    fig.update_layout(legend_title='<b> Agent ID filter: </b>')

    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='LightGray')
    fig.update_yaxes(title= 'Average ( Min )',showgrid=True, gridwidth=1, gridcolor='LightGray')

    fig.show()

#function that calculate and draw line graph with average per day per agent but with monthly visualization
def linePlotAvgByDayByAgent_Monthly(df):

    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%m %Y')

    df_f = df.groupby(['Date', 'AgentId']).mean().reset_index()
    # convert date from (%m %Y) to (%B %Y) format, change the month to string value ( ex: 5 to May)
    df_f['Date'] = pd.to_datetime(df_f['Date']).dt.strftime('%B %Y')

    x = df_f['Date'].unique()

    y_agent1 = df_f[df_f['AgentId'] == 1]['Duration'].to_numpy()  # avg of agent 1
    y_agent2 = df_f[df_f['AgentId'] == 2]['Duration'].to_numpy()  # avg of agent 2
    y_agent3 = df_f[df_f['AgentId'] == 3]['Duration'].to_numpy()  # avg of agent 3
    y_agent4 = df_f[df_f['AgentId'] == 4]['Duration'].to_numpy()  # avg of agent 4
    y_agent5 = df_f[df_f['AgentId'] == 5]['Duration'].to_numpy()  # avg of agent 5

    plt.plot(x, y_agent1, label='Agent 1')
    plt.plot(x, y_agent2, label='Agent 2')
    plt.plot(x, y_agent3, label='Agent 3')
    plt.plot(x, y_agent4, label='Agent 4')
    plt.plot(x, y_agent5, label='Agent 5')

    # labels
    plt.xlabel('Date ( Months ) ')
    plt.ylabel('Average ( Min )')
    plt.xticks(np.arange(len(x)), x, rotation=25)
    plt.title('Average per Agent Monthly ')

    plt.legend(loc='upper right')
    plt.grid(True)

    plt.show()