from DataManipulation import getDataFrame, connectionDB, checkTableExists, \
    createAndLoadTable, avgByAgentPlot, interactivePlotAvgByDayByAgent, \
    dfAvgByDayByAgent, linePlotAvgByDayByAgent_Monthly

def menu(path,driver,server,database):
    #inicialize dataframe and connection
    df = getDataFrame(path)
    conn = connectionDB(driver,server,database)
    cursor = conn.cursor()
    df_AbDbA = dfAvgByDayByAgent(conn)

    # validate if the table is already created
    if (checkTableExists('calls_history', cursor) == False):
        createAndLoadTable(cursor, df, conn)

    print('\n')
    print('## MENU ##')
    print('1 - Draw a bar graph with average per agent')
    print('2 - Draw an interactive graph with average per day per agent')
    print('3 - Draw a line graph with average per day per agent (monthly visualization)')
    print('4 - Quit')

    while True:
        try:
            op = int(input("Insert an option -> "))
            if op == 1:
                avgByAgentPlot(cursor)
                print("\n[StatusInfo] Graph was successfully drawn.")
                menu(path,driver,server,database)
            elif op == 2:
                interactivePlotAvgByDayByAgent(df_AbDbA)
                print("\n[StatusInfo] This option opens a new tab on your browser.")
                menu(path,driver,server,database)
            elif op == 3:
                linePlotAvgByDayByAgent_Monthly(df_AbDbA)
                print("\n[StatusInfo] Graph was successfully drawn.")
                menu(path,driver,server,database)
            elif op == 4:
                break
            else:
                print('\n[StatusInfo] Invalid option. Please, choose another option between 1-4.')
                menu(path,driver,server,database)
        except ValueError:
            print("\n[StatusInfo] Invalid value. Please, insert numeric value and choose an option between 1-4.")

    cursor.close()
    conn.close()
    exit()


###########
#Run code #
###########

#path of "calls.csv" file
path = r'C:\Users\jmenderu\Desktop\ChallengeTalkDesk\calls.csv'

#SQL configs ( only with windows autentication ) #
driver = 'SQL Server'
server = 'localhost'
database = 'MyDB'

menu(path,driver,server,database)