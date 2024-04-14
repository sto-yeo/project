import psycopg
import json
# Connect to an existing database

def load_competition():
    with psycopg.connect("dbname=project_database user=postgres password=1234") as conn:

        # Open a cursor to perform database operations
        with conn.cursor() as cur:


            competition = open(r"D:\open-data\data\competitions.json")
            competition_data = json.load(competition)

            for i in range (0,len(competition_data)):
                cur.execute(
                    "INSERT INTO season (season_id, season_name) values (%s,%s) on conflict do nothing",
                    (competition_data[i]["season_id"], competition_data[i]["season_name"])
                )

                cur.execute(
                    "INSERT INTO competition (competition_id, season_id, country_name, competition_name, competition_gender, competition_youth, competition_international) \
                        values (%s,%s,%s,%s,%s,%s,%s);",\
                        (competition_data[i]["competition_id"], competition_data[i]["season_id"], competition_data[i]["country_name"], competition_data[i]["competition_name"], competition_data[i]["competition_gender"], competition_data[i]["competition_youth"], competition_data[i]["competition_international"])
                    
                )

            """
            # Query the database and obtain data as Python objects.
            cur.execute("SELECT * FROM test")
            cur.fetchone()
            # will return (1, 100, "abc'def")

            # You can use `cur.fetchmany()`, `cur.fetchall()` to return a list
            # of several records, or even iterate on the cursor
            for record in cur:
                print(record)
            """
            # Make the changes to the database persistent
            conn.commit()

def load_matches():
    with psycopg.connect("dbname=project_database user=postgres password=1234") as conn:
        with conn.cursor() as cur:

            
            compe_season = [[11,4], [11,42], [11,90], [2, 44]]
            for i in range (0, len(compe_season)):

                path = 'D:\open-data\data\matches\%s\%s.json' % (compe_season[i][0], compe_season[i][1])
                match = open(path, encoding="utf8")
                match_data = json.load(match)


                for j in range(0, len(match_data)):    
                    
                    cur.execute(
                        "INSERT INTO team (team_id, team_name) values (%s, %s) on conflict do nothing",
                        (match_data[j]["home_team"]["home_team_id"], match_data[j]["home_team"]["home_team_name"])
                    )
                    cur.execute(
                        "INSERT INTO team (team_id, team_name) values (%s, %s) on conflict do nothing",
                        (match_data[j]["away_team"]["away_team_id"], match_data[j]["away_team"]["away_team_name"])
                    )
                    if "referee" in match_data[j]:
                        referee_name = match_data[j]["referee"]["name"]
                        referee_country = match_data[j]["referee"]["country"]["name"]                        
                    else:
                        referee_name = None
                        referee_country = None
                    
                    if "managers" in match_data[j]["home_team"]:
                        home_team_manager_id = match_data[j]["home_team"]["managers"][0]["id"]
                        home_team_manager_dob = match_data[j]["home_team"]["managers"][0]["dob"]
                        home_team_manager_country = match_data[j]["home_team"]["managers"][0]["country"]["name"]
                    else:
                        home_team_manager_id = None
                        home_team_manager_dob = None
                        home_team_manager_country =None

                    if "managers" in match_data[j]["away_team"]:
                        away_team_manager_id = match_data[j]["away_team"]["managers"][0]["id"]
                        away_team_manager_dob = match_data[j]["away_team"]["managers"][0]["dob"]
                        away_team_manager_country = match_data[j]["away_team"]["managers"][0]["country"]["name"]
                    else:
                        away_team_manager_id = None
                        away_team_manager_dob = None
                        away_team_manager_country = None

                    if "stadium" in match_data[j]:
                        stadium_name = match_data[j]["stadium"]["name"]
                        stadium_country = match_data[j]["stadium"]["country"]["name"]                        
                    else:
                        stadium_name = None
                        stadium_country = None

                    cur.execute(                        
                            "INSERT INTO match (match_id, competition_id, season_id, match_date, kickoff, stadium_name, stadium_country, referee_name, referee_country, home_team_id, home_team_manager_id, home_team_manager_dob, home_team_manager_country, home_team_group, home_team_country, away_team_id, away_team_manager_id, away_team_manager_dob, away_team_manager_country, away_team_group, away_team_country, home_score, away_score, match_week, competition_stage)\
                                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);",\
                                (match_data[j]["match_id"], match_data[j]["competition"]["competition_id"], match_data[j]["season"]["season_id"],\
                                match_data[j]["match_date"], match_data[j]["kick_off"], stadium_name, stadium_country,  referee_name,  referee_country,\
                                match_data[j]["home_team"]["home_team_id"], home_team_manager_id,  home_team_manager_dob,  home_team_manager_country, match_data[j]["home_team"]["home_team_group"], match_data[j]["home_team"]["country"]["name"], \
                                match_data[j]["away_team"]["away_team_id"], away_team_manager_id,  away_team_manager_dob,  away_team_manager_country, match_data[j]["away_team"]["away_team_group"], match_data[j]["away_team"]["country"]["name"], \
                                match_data[j]["home_score"], match_data[j]["away_score"], match_data[j]["match_week"], match_data[j]["competition_stage"]["name"])
                        )
                    
            conn.commit()


def get_all_matches():
    all_matches = []
    compe_season = [[11,4], [11,42], [11,90], [2, 44]]
    for i in range (0, len(compe_season)):

        path = 'D:\open-data\data\matches\%s\%s.json' % (compe_season[i][0], compe_season[i][1])

        match = open(path, encoding="utf8")
        match_data = json.load(match)


        for j in range(0, len(match_data)):    
            all_matches.append(match_data[j]["match_id"])
            
    return all_matches

def load_lineup(all_matches):
    with psycopg.connect("dbname=project_database user=postgres password=1234") as conn:
        with conn.cursor() as cur:

            for i in range (0, len(all_matches)):

                path = 'D:\open-data\data\lineups\%s.json' % (all_matches[i])
                lineup = open(path, encoding="utf8")
                lineup_data = json.load(lineup)
                match_id = all_matches[i]


                for j in range(0, len(lineup_data)):
                    team_id = lineup_data[j]["team_id"]
                    for k in range(0, len(lineup_data[j]["lineup"])):
                        player_id = lineup_data[j]["lineup"][k]["player_id"]
                        player_name = lineup_data[j]["lineup"][k]["player_name"]
                        player_nickname = lineup_data[j]["lineup"][k]["player_nickname"]
                        player_country = lineup_data[j]["lineup"][k]["country"]["name"]
                        jersey_number = lineup_data[j]["lineup"][k]["jersey_number"]
                        card =[]
                        if lineup_data[j]["lineup"][k]["cards"]:
                            for a in range(0, len(lineup_data[j]["lineup"][k]["cards"])):
                                card.append(lineup_data[j]["lineup"][k]["cards"][a]["card_type"])

                        if not lineup_data[j]["lineup"][k]["positions"]:
                            position = None
                            from_time = None
                            to_time = None
                            start_reason = None
                            end_reason = None
                        else:
                            position = lineup_data[j]["lineup"][k]["positions"][0]["position"]
                            from_time = lineup_data[j]["lineup"][k]["positions"][0]["from"]
                            to_time = lineup_data[j]["lineup"][k]["positions"][0]["to"]
                            start_reason = lineup_data[j]["lineup"][k]["positions"][0]["start_reason"]
                            end_reason = lineup_data[j]["lineup"][k]["positions"][0]["end_reason"]
                        
                        cur.execute(
                            "INSERT INTO player (player_id, player_name, player_nickname, country) \
                                values (%s, %s,%s,%s) on conflict do nothing;",
                                (player_id, player_name, player_nickname, player_country)

                        )

                        cur.execute("""
                            INSERT INTO lineup (match_id, team_id, player_id, jersey_number, card, position, "from", "to", start_reason, end_reason) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);""", (match_id, team_id, player_id, jersey_number, card, position, from_time, to_time, start_reason, end_reason)
                        )
                    
                     

            conn.commit()

def load_events(all_matches):
    with psycopg.connect("dbname=project_database user=postgres password=1234") as conn:
        with conn.cursor() as cur:

            for i in range (0, len(all_matches)):

                path = 'D:\open-data\data\events\%s.json' % (all_matches[i])
                event = open(path, encoding="utf8")
                event_data = json.load(event)
                match_id = all_matches[i]
                print(path)

                for j in range(0, len(event_data)):
                    event_id = event_data[j]["id"]
                    period = event_data[j]["period"]
                    timestamp = event_data[j]["timestamp"]
                    minute = event_data[j]["minute"]
                    second = event_data[j]["second"]
                    type = event_data[j]["type"]["name"]
                    possession = event_data[j]["possession"]
                    possession_team_id = event_data[j]["possession_team"]["id"]
                    play_patter = event_data[j]["play_pattern"]["name"]
                    team_name = event_data[j]["team"]["name"]
                    if "player" in event_data[j]:
                        player_id = event_data[j]["player"]["id"]
                    else:
                        player_id = None

                    if "position" in event_data[j]:
                        position = event_data[j]["position"]["name"]
                    else: 
                        position = None

                    if "location" in event_data[j]:
                        location = event_data[j]["location"]
                    else:
                        location = None
                    
                    if "duration" in event_data[j]:
                        duration = event_data[j]["duration"]
                    else:
                        duration = None

                    if "under_pressure" in event_data[j]:
                        under_pressure = event_data[j]["under_pressure"]
                    else:
                        under_pressure = False

                    if "out" in event_data[j]:
                        out = event_data[j]["out"]
                    else:
                        out = False

                    if "related_events" in event_data[j]:
                        related_events = event_data[j]["related_events"]
                    else:
                        related_events = []
                    
                    if type == "Shot":
                        key_pass_id = None
                        end_location = None
                        aerial_won = False
                        follows_dribble = False
                        first_time = False
                        open_goal = False
                        statsbomb_xg = None
                        deflected = False
                        body_part = None
                        type = None
                        outcome = None
                        technique = None
                        if "shot" in event_data[j]:
                            if "key_pass_id" in event_data[j]["shot"]:
                                key_pass_id = event_data[j]["shot"]["key_pass_id"]
                            if "end_location" in event_data[j]["shot"]:
                                end_location = event_data[j]["shot"]["end_location"]
                            if "aerial_won" in event_data[j]["shot"]:
                                aerial_won = True
                            if "follows_dribble" in event_data[j]["shot"]:
                                follows_dribble = True
                            if "first_time" in event_data[j]["shot"]:
                                first_time = True
                            if "open_goal" in event_data[j]["shot"]:
                                open_goal = True
                            if "statsbomb_xg" in event_data[j]["shot"]:
                                statsbomb_xg = event_data[j]["shot"]["statsbomb_xg"]
                            if "deflected" in event_data[j]["shot"]:
                                deflected = True
                            if "body_part" in event_data[j]["shot"]:
                                body_part = event_data[j]["shot"]["body_part"]["name"]
                            if "type" in event_data[j]["shot"]:
                                type = event_data[j]["shot"]["type"]["name"]
                            if "outcome" in event_data[j]["shot"]:
                                outcome = event_data[j]["shot"]["outcome"]["name"]
                            if "technique" in event_data[j]["shot"]:
                                technique = event_data[j]["shot"]["technique"]["name"]
                        cur.execute("""INSERT INTO shot (event_id, key_pass_id, end_location, aerial_won, follows_dribble, first_time, open_goal, statsbomb_xg, deflected,technique, body_part, "type", outcome) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (event_id, key_pass_id, end_location, aerial_won, follows_dribble, first_time, open_goal, statsbomb_xg, deflected, technique, body_part, type, outcome))








                        

            conn.commit()

all_matches = get_all_matches()
print(all_matches)
#load_competition()
#load_matches()
#load_lineup(all_matches)
load_events(all_matches)