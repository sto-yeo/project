# Created by Gabriel Martell

'''
Version 1.2 (04/13/2024)
=========================================================
queries.py (Carleton University COMP3005 - Database Management Student Template Code)

This is the template code for the COMP3005 Database Project 1, and must be accomplished on an Ubuntu Linux environment.
Your task is to ONLY write your SQL queries within the prompted space within each Q_# method (where # is the question number).

You may modify code in terms of testing purposes (commenting out a Qn method), however, any alterations to the code, such as modifying the time, 
will be flagged for suspicion of cheating - and thus will be reviewed by the staff and, if need be, the Dean. 

To review the Integrity Violation Attributes of Carleton University, please view https://carleton.ca/registrar/academic-integrity/ 

=========================================================
'''

# Imports
import psycopg
import csv
import subprocess
import os
import re

# Connection Information
''' 
The following is the connection information for this project. These settings are used to connect this file to the autograder.
You must NOT change these settings - by default, db_host, db_port and db_username are as follows when first installing and utilizing psql.
For the user "postgres", you must MANUALLY set the password to 1234.

This can be done with the following snippet:

sudo -u postgres psql
\password postgres

'''
root_database_name = "project_database"
query_database_name = "query_database"
db_username = 'postgres'
db_password = '1234'
db_host = 'localhost'
db_port = '5432'

# Directory Path - Do NOT Modify
dir_path = os.path.dirname(os.path.realpath(__file__))

# Loading the Database after Drop - Do NOT Modify
#================================================
def load_database(conn):
    drop_database(conn)

    cursor = conn.cursor()
    # Create the Database if it DNE
    try:
        conn.autocommit = True
        cursor.execute(f"CREATE DATABASE {query_database_name};")
        conn.commit()

    except Exception as error:
        print(error)

    finally:
        cursor.close()
        conn.autocommit = False
    conn.close()
    
    # Connect to this query database.
    dbname = query_database_name
    user = db_username
    password = db_password
    host = db_host
    port = db_port
    conn = psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port)

    # Import the dbexport.sql database data into this database
    try:
        command = f'psql -h {host} -U {user} -d {query_database_name} -a -f "{os.path.join(dir_path, "dbexport.sql")}" > /dev/null 2>&1'
        env = {'PGPASSWORD': password}
        subprocess.run(command, shell=True, check=True, env=env)

    except Exception as error:
        print(f"An error occurred while loading the database: {error}")
    
    # Return this connection.
    return conn    

# Dropping the Database after Query n Execution - Do NOT Modify
#================================================
def drop_database(conn):
    # Drop database if it exists.

    cursor = conn.cursor()

    try:
        conn.autocommit = True
        cursor.execute(f"DROP DATABASE IF EXISTS {query_database_name};")
        conn.commit()

    except Exception as error:
        print(error)
        pass

    finally:
        cursor.close()
        conn.autocommit = False

# Reconnect to Root Database - Do NOT Modify
#================================================
def reconnect():
    dbname = root_database_name
    user = db_username
    password = db_password
    host = db_host
    port = db_port
    return psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port)

# Getting the execution time of the query through EXPLAIN ANALYZE - Do NOT Modify
#================================================
def get_time(cursor, sql_query):
    # Prefix your query with EXPLAIN ANALYZE
    explain_query = f"EXPLAIN ANALYZE {sql_query}"

    try:
        # Execute the EXPLAIN ANALYZE query
        cursor.execute(explain_query)
        
        # Fetch all rows from the cursor
        explain_output = cursor.fetchall()
        
        # Convert the output tuples to a single string
        explain_text = "\n".join([row[0] for row in explain_output])
        
        # Use regular expression to find the execution time
        # Look for the pattern "Execution Time: <time> ms"
        match = re.search(r"Execution Time: ([\d.]+) ms", explain_text)
        if match:
            execution_time = float(match.group(1))
            return f"Execution Time: {execution_time} ms"
        else:
            print("Execution Time not found in EXPLAIN ANALYZE output.")
            return f"NA"
        
    except Exception as error:
        print(f"[ERROR] Error getting time.\n{error}")


# Write the results into some Q_n CSV. If the is an error with the query, it is a INC result - Do NOT Modify
#================================================
def write_csv(execution_time, cursor, i):
    # Collect all data into this csv, if there is an error from the query execution, the resulting time is INC.
    try:
        colnames = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        filename = f"{dir_path}/Q_{i}.csv"

        with open(filename, 'w', encoding='utf-8', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            
            # Write column names to the CSV file
            csvwriter.writerow(colnames)
            
            # Write data rows to the CSV file
            csvwriter.writerows(rows)

    except Exception as error:
        execution_time[i-1] = "INC"
        print(error)
    
#================================================
        
'''
The following 10 methods, (Q_n(), where 1 < n < 10) will be where you are tasked to input your queries.
To reiterate, any modification outside of the query line will be flagged, and then marked as potential cheating.
Once you run this script, these 10 methods will run and print the times in order from top to bottom, Q1 to Q10 in the terminal window.
'''
def Q_1(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT player_name,
	avg(statsbomb_xg)
FROM
	(SELECT second_join.player_id,
			statsbomb_xg,
			competition_id,
			season_id,
			player_name
		FROM
			(SELECT player_id,
					statsbomb_xg,
					competition_id,
					season_id
				FROM
					(SELECT player_id,
							statsbomb_xg,
							match_id
						FROM shot
						LEFT JOIN "event" ON "event".event_id = shot.event_id) AS first_join
				LEFT JOIN "match" ON "match".match_id = first_join.match_id) AS second_join
		LEFT JOIN player ON player.player_id = second_join.player_id
		WHERE competition_id = 11
			AND season_id = 90)
GROUP BY player_name,
	player_id
ORDER BY avg(statsbomb_xg) DESC """

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[0] = (time_val)

    write_csv(execution_time, cursor, 1)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_2(conn, execution_time):

    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT player_name,
	count(player_id) as shots
FROM
	(SELECT second_join.player_id,
			statsbomb_xg,
			competition_id,
			season_id,
			player_name
		FROM
			(SELECT player_id,
					statsbomb_xg,
					competition_id,
					season_id
				FROM
					(SELECT player_id,
							statsbomb_xg,
							match_id
						FROM shot
						LEFT JOIN "event" ON "event".event_id = shot.event_id) AS first_join
				LEFT JOIN "match" ON "match".match_id = first_join.match_id) AS second_join
		LEFT JOIN player ON player.player_id = second_join.player_id
		WHERE competition_id = 11
			AND season_id = 90)
GROUP BY player_name,
	player_id
ORDER BY count(player_id) DESC """

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[1] = (time_val)

    write_csv(execution_time, cursor, 2)

    cursor.close()
    new_conn.close()

    return reconnect()
    
def Q_3(conn, execution_time):

    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT player_name,
	count(player_id) as shots from
	(SELECT second_join.player_id,
			first_time,
			competition_id,
			season_id,
			player_name
		FROM
			(SELECT player_id,
					first_time,
					competition_id,
					season_id
				FROM
					(SELECT player_id,
							first_time,
							match_id
						FROM shot
						LEFT JOIN "event" ON "event".event_id = shot.event_id where first_time = TRUE) AS first_join
				LEFT JOIN "match" ON "match".match_id = first_join.match_id) AS second_join
		LEFT JOIN player ON player.player_id = second_join.player_id
		WHERE competition_id = 11
			AND (season_id = 90 or season_id = 4 or season_id = 42))
			GROUP BY player_name,
	player_id
ORDER BY count(player_id) DESC 
 """

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[2] = (time_val)

    write_csv(execution_time, cursor, 3)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_4(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT join2.team_name,
	count(join2.team_name) AS passes_count
FROM
	(SELECT join1.event_id,
			join1.match_id,
			competition_id,
			season_id,
			join1.team_name
		FROM
			(SELECT pass.event_id,
					match_id,
					"event".team_name
				FROM pass
				LEFT JOIN "event" ON "event".event_id = pass.event_id) AS join1
		LEFT JOIN "match" ON "match".match_id = join1.match_id
		WHERE competition_id = 11
			AND season_id = 90) AS join2
GROUP BY join2.team_name
ORDER BY passes_count DESC """

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[3] = (time_val)

    write_csv(execution_time, cursor, 4)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_5(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT join3.player_name,
	count(join3.recipient_id)
FROM
	(SELECT join2.recipient_id,
			player_name
		FROM
			(SELECT join1.recipient_id
				FROM
					(SELECT pass.event_id,
							match_id,
							pass.recipient_id
						FROM pass
						LEFT JOIN "event" ON "event".event_id = pass.event_id
						WHERE pass.recipient_id IS NOT NULL) AS join1
				LEFT JOIN "match" ON "match".match_id = join1.match_id
				WHERE competition_id = 2
					AND season_id = 44) AS join2
		LEFT JOIN player ON player.player_id = join2.recipient_id) AS join3
GROUP BY (join3.player_name)
ORDER BY count(join3.recipient_id) DESC """

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[4] = (time_val)

    write_csv(execution_time, cursor, 5)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_6(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """ 
SELECT join2.team_name,
	count(join2.team_name)
FROM
	(SELECT join1.team_name
		FROM
			(SELECT shot.event_id,
					match_id,
					"event".team_name
				FROM shot
				LEFT JOIN "event" ON "event".event_id = shot.event_id) AS join1
		LEFT JOIN "match" ON "match".match_id = join1.match_id
		WHERE competition_id = 2
			AND season_id = 44) AS join2
GROUP BY (join2.team_name)
ORDER BY count(join2.team_name) DESC"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[5] = (time_val)

    write_csv(execution_time, cursor, 6)

    cursor.close()
    new_conn.close()

    return reconnect()


def Q_7(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """ SELECT player_name,
	total
FROM
	(SELECT player_id,
			count(player_id) AS total
		FROM
			(SELECT player_id,
					match_id
				FROM pass
				LEFT JOIN "event" ON "event".event_id = pass.event_id
				WHERE technique = 'Through Ball') AS join1
		LEFT JOIN "match" ON "match".match_id = join1.match_id
		WHERE competition_id = 11
			AND season_id = 90
		GROUP BY player_id
		ORDER BY count(player_id) DESC) AS join2
LEFT JOIN player ON player.player_id = join2.player_id
ORDER BY total DESC"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[6] = (time_val)

    write_csv(execution_time, cursor, 7)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_8(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """ SELECT team_name,
	count(team_name)
FROM
	(SELECT team_name,
			match_id
		FROM pass
		LEFT JOIN "event" ON "event".event_id = pass.event_id
		WHERE technique = 'Through Ball') AS join1
LEFT JOIN "match" ON "match".match_id = join1.match_id
WHERE competition_id = 11
	AND season_id = 90
GROUP BY team_name
ORDER BY count(team_name) DESC"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[7] = (time_val)

    write_csv(execution_time, cursor, 8)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_9(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT player_name,
	total
FROM
	(SELECT player_id,
			count(player_id) AS total
		FROM
			(SELECT player_id,
					match_id
				FROM dribble
				LEFT JOIN "event" ON "event".event_id = dribble.event_id
				WHERE outcome = 'Complete') AS join1
		LEFT JOIN "match" ON "match".match_id = join1.match_id
		WHERE competition_id = 11
			AND (season_id = 90
								OR season_id = 4
								OR season_id = 42)
		GROUP BY player_id
		ORDER BY count(player_id) DESC) AS join2
LEFT JOIN player ON player.player_id = join2.player_id
ORDER BY total DESC """

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[8] = (time_val)

    write_csv(execution_time, cursor, 9)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_10(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT player_name,
	total
FROM
	(SELECT player_id,
			count(player_id) AS total
		FROM
			(SELECT player_id,
					match_id
				FROM "event"

				WHERE "type" = 'Dribbled Past') AS join1
		LEFT JOIN "match" ON "match".match_id = join1.match_id
		WHERE competition_id = 11
			AND season_id = 90
								
		GROUP BY player_id
		ORDER BY count(player_id) DESC) AS join2
LEFT JOIN player ON player.player_id = join2.player_id
ORDER BY total DESC """

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[9] = (time_val)

    write_csv(execution_time, cursor, 10)

    cursor.close()
    new_conn.close()

    return reconnect()

# Running the queries from the Q_n methods - Do NOT Modify
#=====================================================
def run_queries(conn):

    execution_time = [0,0,0,0,0,0,0,0,0,0]

    conn = Q_1(conn, execution_time)
    conn = Q_2(conn, execution_time)
    conn = Q_3(conn, execution_time)
    conn = Q_4(conn, execution_time)
    conn = Q_5(conn, execution_time)
    conn = Q_6(conn, execution_time)
    conn = Q_7(conn, execution_time)
    conn = Q_8(conn, execution_time)
    conn = Q_9(conn, execution_time)
    conn = Q_10(conn, execution_time)

    for i in range(10):
        print(execution_time[i])

''' MAIN '''
try:
    if __name__ == "__main__":

        dbname = root_database_name
        user = db_username
        password = db_password
        host = db_host
        port = db_port

        conn = psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        
        run_queries(conn)
except Exception as error:
    print(error)
    #print("[ERROR]: Failure to connect to database.")
#_______________________________________________________
