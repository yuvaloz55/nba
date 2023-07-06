import pandas as pd
import time
import numpy as np
from nba_api.stats.endpoints import PlayByPlayV2
from nba_api.stats.endpoints import LeagueGameLog
from nba_api.stats.endpoints import BoxScoreTraditionalV2

# Define the start time
start_time = time.time()

# Define the NBA season the code will iterate on
seasons='2022-23'
# Define the season_type the code will iterate on
season_type='Playoffs'

# Create the LeagueGameFinder instance
# game_finder = LeagueGameLog(season=seasons,     date_from_nullable="2023-04-01", date_to_nullable="2023-04-14", league_id='00')
game_finder = LeagueGameLog(season=seasons, season_type_all_star=season_type, league_id='00')
# Retrieve the game data
game_data = game_finder.get_data_frames()[0]
# Convert the game_data into a unique list--->list of all games in the season (season_type)
unique_game_ids = game_data['GAME_ID'].drop_duplicates().tolist()

# Step 1: Create the basic stats file for each player
player_stats=[]
for game in unique_game_ids:
    data = BoxScoreTraditionalV2(game_id=game)
    data = data.get_data_frames()[0]
    data = data[data['MIN'].notnull()] # Removes all players that didn't play
    data['STOCKS'] = data[['STL', 'BLK']].sum(axis=1) # Calculates the stocks
    data['SEASON'] = seasons
    data['SEASON_TYPE'] = season_type
    player_stats.append(data)

player_stats_df = pd.concat(player_stats, ignore_index=True)
# Aggregate the DataFrame
aggregated_player_stats_df = player_stats_df.groupby(['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ABBREVIATION', 'SEASON', 'SEASON_TYPE']).agg({
        'GAME_ID': 'count',
        'STOCKS': 'sum'
    }).reset_index()

# Step 2: Calculate the value stocks
results=[]
for game in unique_game_ids:
    play = PlayByPlayV2(game_id=game) # Fetching all of the play by play data for the game
    df = play.get_data_frames()[0]

    # Check if the description contains "BLOCK" or "STEAL"
    df['HOME_STOCKS'] = df['HOMEDESCRIPTION'].str.contains("BLOCK|STEAL", case=False, regex=True).fillna(False).astype(int)
    df['AWAY_STOCKS'] = df['VISITORDESCRIPTION'].str.contains("BLOCK|STEAL", case=False, regex=True).fillna(False).astype(int)

    # Check if EVENTMSGTYPE is equal to 1 or (EVENTMSGTYPE is equal to 3 and SCORE is not null) ---->checks if it was a made basket or free throw
    df['HOME_BUCKET'] = ((df['HOMEDESCRIPTION'].notnull() & (df['EVENTMSGTYPE'] == 1) | (df['HOMEDESCRIPTION'].notnull() & (df['EVENTMSGTYPE'] == 3) & (df['SCORE'].notnull()))).astype(int))
    df['AWAY_BUCKET'] = ((df['VISITORDESCRIPTION'].notnull() & (df['EVENTMSGTYPE'] == 1) | (df['VISITORDESCRIPTION'].notnull() & (df['EVENTMSGTYPE'] == 3) & (df['SCORE'].notnull()))).astype(int))

    # Extract home and away scores from SCORE column
    df['SCORE_FULL'] = df['SCORE'].ffill()
    df[['AWAY_SCORE', 'HOME_SCORE']] = df['SCORE_FULL'].str.split(' - ', expand=True)

    # Remove records with 0 in all columns (AKA non-stocks or buckets), but keep timeouts and end of quarters
    df = df.drop(df[(df['HOME_STOCKS'] == 0) & (df['AWAY_STOCKS'] == 0) & (df['HOME_BUCKET'] == 0) & (df['AWAY_BUCKET'] == 0) & (~df['EVENTMSGTYPE'].isin([9, 12, 13]))].index)

    # Add column to check if basket came after a stock
    df['HOME_BASKET_AFTER_STOCK'] = ((df['HOME_STOCKS'].shift() == 1) & (df['HOME_BUCKET'] == 1)).astype(int)
    df['AWAY_BASKET_AFTER_STOCK'] = ((df['AWAY_STOCKS'].shift() == 1) & (df['AWAY_BUCKET'] == 1)).astype(int)

    # Calculate points scored in each play for home team
    df['HOME_POINTS_SCORED'] = df['HOME_SCORE'].astype(float) - df['HOME_SCORE'].shift().astype(float)
    # Check if there were 2 successful free throws
    df['UPDATED_HOME_POINTS_SCORED'] = df['HOME_POINTS_SCORED']
    mask = (df['EVENTMSGTYPE'] == 3) & (df['EVENTMSGTYPE'].shift(-1) == 3) & (df['PCTIMESTRING'] == df['PCTIMESTRING'].shift(-1))
    # Calculate HOME_POINTS_SCORED based on current and next HOME_SCORE
    df.loc[mask, 'UPDATED_HOME_POINTS_SCORED'] = df['HOME_POINTS_SCORED'].astype(float) + df['HOME_POINTS_SCORED'].shift(-1).astype(float)

    # Calculate points scored in each play for away team
    df['AWAY_POINTS_SCORED'] = df['AWAY_SCORE'].astype(float) - df['AWAY_SCORE'].shift().astype(float)
    # Check if there were 2 successful free throws
    df['UPDATED_AWAY_POINTS_SCORED'] = df['AWAY_POINTS_SCORED']
    mask = (df['EVENTMSGTYPE'] == 3) & (df['EVENTMSGTYPE'].shift(-1) == 3) & (df['PCTIMESTRING'] == df['PCTIMESTRING'].shift(-1))
    # Calculate HOME_POINTS_SCORED based on current and next HOME_SCORE
    df.loc[mask, 'UPDATED_AWAY_POINTS_SCORED'] = df['AWAY_POINTS_SCORED'].astype(float) + df['AWAY_POINTS_SCORED'].shift(-1).astype(float)

    # Extract minutes and seconds from PCTIMESTRING column
    df[['MINUTES', 'SECONDS']] = df['PCTIMESTRING'].str.split(':', expand=True)

    # Calculate the difference in time between each play
    df['MINUTES_DIFF'] = df['MINUTES'].shift().astype(float) - df['MINUTES'].astype(float)
    df['SECONDS_DIFF'] = (df['SECONDS'].shift().astype(float) - df['SECONDS'].astype(float))/60
    df['TIME_DIFF'] = (df['MINUTES_DIFF']+df['SECONDS_DIFF'])*60

    # Add columns to attribute player_id from previous record
    df['ATTRIBUTED_PLAYER2_ID'] = df['PLAYER2_ID'].shift()
    df['ATTRIBUTED_PLAYER3_ID'] = df['PLAYER3_ID'].shift()
    df['ATTRIBUTED_PLAYER_ID'] = 0

    # Assign PLAYER2_ID if not equal to 0
    df.loc[df['ATTRIBUTED_PLAYER2_ID'] != 0, 'ATTRIBUTED_PLAYER_ID'] = df['ATTRIBUTED_PLAYER2_ID']

    # Assign PLAYER3_ID if PLAYER2_ID is equal to 0 and PLAYER3_ID is not equal to 0
    df.loc[(df['ATTRIBUTED_PLAYER2_ID'] == 0) & (df['ATTRIBUTED_PLAYER3_ID'] != 0), 'ATTRIBUTED_PLAYER_ID'] = df['ATTRIBUTED_PLAYER3_ID']

    # Assign player name and team from previous record
    df['ATTRIBUTED_PLAYER'] = df['PLAYER2_NAME'].shift().fillna(df['PLAYER3_NAME'].shift())
    df['ATTRIBUTED_TEAM'] = df['PLAYER2_TEAM_ABBREVIATION'].shift().fillna(df['PLAYER3_TEAM_ABBREVIATION'].shift())

    # Filter the DataFrame based on BASKET_AFTER_STOCK and TIME_DIFF
    filtered_home_df = df[(df['HOME_BASKET_AFTER_STOCK'] == 1) & (df['TIME_DIFF'] >= 0) & (df['TIME_DIFF'] <= 7)]
    filtered_away_df = df[(df['AWAY_BASKET_AFTER_STOCK'] == 1) & (df['TIME_DIFF'] >= 0) & (df['TIME_DIFF'] <= 7)]

    # Aggregate the filtered DataFrame
    aggregated_home_df = filtered_home_df.groupby(['ATTRIBUTED_PLAYER_ID', 'ATTRIBUTED_PLAYER', 'ATTRIBUTED_TEAM']).agg({
        'HOME_BASKET_AFTER_STOCK': 'count',
        'UPDATED_HOME_POINTS_SCORED': 'sum'
    }).reset_index()

    aggregated_away_df = filtered_away_df.groupby(['ATTRIBUTED_PLAYER_ID', 'ATTRIBUTED_PLAYER', 'ATTRIBUTED_TEAM']).agg({
        'AWAY_BASKET_AFTER_STOCK': 'count',
        'UPDATED_AWAY_POINTS_SCORED': 'sum'
    }).reset_index()

    # Rename columns in aggregated_home_df
    aggregated_home_df = aggregated_home_df.rename(columns={
        'HOME_BASKET_AFTER_STOCK': 'VALUE_STOCK',
        'UPDATED_HOME_POINTS_SCORED': 'POINTS_OFF_STOCK'
    })

    # Rename columns in aggregated_away_df
    aggregated_away_df = aggregated_away_df.rename(columns={
        'AWAY_BASKET_AFTER_STOCK': 'VALUE_STOCK',
        'UPDATED_AWAY_POINTS_SCORED': 'POINTS_OFF_STOCK'
    })

    aggregated_df = pd.concat([aggregated_home_df, aggregated_away_df])
    results.append(aggregated_df)

results_df = pd.concat(results, ignore_index=True)
aggregated_results_df = results_df.groupby(['ATTRIBUTED_PLAYER_ID', 'ATTRIBUTED_PLAYER', 'ATTRIBUTED_TEAM']).agg({
        'VALUE_STOCK': 'sum',
        'POINTS_OFF_STOCK': 'sum'
    }).reset_index()

# Creating the merged dataset
merged_df = pd.merge(aggregated_player_stats_df, aggregated_results_df, left_on='PLAYER_ID', right_on='ATTRIBUTED_PLAYER_ID', how='left')
# Removing unnecessary columns
columns_to_drop = ['ATTRIBUTED_PLAYER_ID', 'ATTRIBUTED_PLAYER', 'ATTRIBUTED_TEAM']
merged_df = merged_df.drop(columns_to_drop, axis=1)
# Renaming GAME_ID column
merged_df = merged_df.rename(columns={'GAME_ID': 'GAMES_PLAYED'})
# Filling null values with zero
merged_df[['VALUE_STOCK', 'POINTS_OFF_STOCK']] = merged_df[['VALUE_STOCK', 'POINTS_OFF_STOCK']].fillna(0)
# Calculating additional metrics
# merged_df['VALUE_STOCK_RATE'] = np.where(merged_df['STOCKS'] != 0, merged_df['VALUE_STOCK'] / merged_df['STOCKS'], 0)
# merged_df['PTS_PER_STOCK'] = np.where(merged_df['STOCKS'] != 0, merged_df['POINTS_OFF_STOCK'] / merged_df['STOCKS'], 0)
# merged_df['PTS_PER_VALUE_STOCK'] = np.where(merged_df['VALUE_STOCK'] != 0, merged_df['POINTS_OFF_STOCK'] / merged_df['VALUE_STOCK'], 0)
# merged_df['STOCK_PTS_PER_GAME'] = merged_df['POINTS_OFF_STOCK'] / merged_df['GAMES_PLAYED']

# Exporting to Excel
aggregated_results_df.to_excel ('stocks.xlsx', index=False)
aggregated_player_stats_df.to_excel ('player_stats.xlsx', index=False)
merged_df.to_excel('stocks_merged.xlsx', index=False)

# Calculating the total runtime
end_time = time.time()
runtime = end_time-start_time
print("Runtime: {:.2f} seconds".format(runtime))
