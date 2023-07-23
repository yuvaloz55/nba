import pandas as pd
import numpy as np
from nba_api.stats.endpoints import PlayByPlayV2

games = ['0042200404', '0042200405', '0042200403', '0042200402', '0042200401']

all_data = []
for g in games:
    play = PlayByPlayV2(game_id=g)
    df = play.get_data_frames()[0]

    # Define a function to determine the team based on description
    def determine_team(home_description, visitor_description):
        if pd.notnull(home_description) and pd.notnull(visitor_description):
            if 'STEAL' in visitor_description or 'BLOCK' in visitor_description:
                return 'HOME'
            elif 'STEAL' in home_description or 'BLOCK' in home_description:
                return 'AWAY'
        elif pd.notnull(home_description):
            return 'HOME'
        elif pd.notnull(visitor_description):
            return 'AWAY'
        return 'UNKNOWN'  # Provide a default value for the 'TEAM' column

    # Apply the function to create the "TEAM" column
    df['TEAM'] = df.apply(lambda row: determine_team(row['HOMEDESCRIPTION'], row['VISITORDESCRIPTION']), axis=1)

    # Define conditions and corresponding values for the "CURRENT_PLAY" column
    conditions = [
        df['HOMEDESCRIPTION'].str.contains('STEAL', case=False, na=False) | df['VISITORDESCRIPTION'].str.contains('STEAL', case=False, na=False),
        df['HOMEDESCRIPTION'].str.contains('BLOCK', case=False, na=False) | df['VISITORDESCRIPTION'].str.contains('BLOCK', case=False, na=False),
        df['EVENTMSGTYPE'] == 2,
        df['EVENTMSGTYPE'] == 1,
        (df['EVENTMSGTYPE'] == 4) & (df['TEAM'] == df['TEAM'].shift(1)),
        (df['EVENTMSGTYPE'] == 4) & (df['TEAM'] != df['TEAM'].shift(1)),
    ]
    choices = ['STEAL', 'BLOCK', 'FG_MISSED', 'FG_MADE', 'OFFENSIVE REBOUND', 'DEFENSIVE REBOUND']

    # Use numpy.select to apply the conditions and set values for the "CURRENT_PLAY" column
    df['CURRENT_PLAY'] = np.select(conditions, choices, default='OTHER')
    df['NEXT_PLAY'] = df['CURRENT_PLAY'].shift(-1)

    # Extract minutes and seconds from PCTIMESTRING column
    df[['MINUTES', 'SECONDS']] = df['PCTIMESTRING'].str.split(':', expand=True)

    # Calculate the difference in time between each play
    df['MINUTES_DIFF'] = df['MINUTES'].shift().astype(float) - df['MINUTES'].astype(float)
    df['SECONDS_DIFF'] = (df['SECONDS'].shift().astype(float) - df['SECONDS'].astype(float))/60
    df['TIME_DIFF'] = (df['MINUTES_DIFF']+df['SECONDS_DIFF'])*60

    # Add the "ATTRIBUTED_PLAYER" column based on the logic
    df['ATTRIBUTED_PLAYER'] = np.where(df['CURRENT_PLAY'] == 'STEAL', df['PLAYER2_NAME'],
                                       np.where(df['CURRENT_PLAY'] == 'BLOCK', 'PLAYER3_NAME', df['PLAYER1_NAME']))

    all_data.append(df)

combined_df = pd.concat(all_data)
combined_df.to_excel('play_by_play_2.xlsx', index=False)
