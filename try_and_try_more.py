import re
from datetime import datetime
from itertools import tee

from nba_api.stats.endpoints import PlayByPlayV2
from nba_api.stats.endpoints import LeagueGameFinder


_PLAYOFFS = "Playoffs"
_2023 = "2022-23"
_HOME_LOG = "HOMEDESCRIPTION"
_AWAY_LOG = "VISITORDESCRIPTION"
_STOCK_PATTERN = "BLOCK|STEAL"
_TIME_FORMAT = "%M:%S"
_MAX_TIME_DELTA = 7


def pairwise(iterable):
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def _get_game_ids_by_season_and_type(season, season_type):
    games_data = LeagueGameFinder(season_nullable=season, season_type_nullable=season_type).get_normalized_dict()[
        "LeagueGameFinderResults"
    ]
    return (dictionary["GAME_ID"] for dictionary in games_data)


def _play_is_stock(play):
    if not (play[_HOME_LOG] and play[_AWAY_LOG]):
        return False
    if not play[_HOME_LOG]:
        return re.search(_STOCK_PATTERN, play[_AWAY_LOG])
    if not play[_AWAY_LOG]:
        return re.search(_STOCK_PATTERN, play[_HOME_LOG])
    return re.search(_STOCK_PATTERN, play[_HOME_LOG] + play[_AWAY_LOG])


def _is_time_valid(next_play, play):
    return (
        abs(
            (
                datetime.strptime(next_play["PCTIMESTRING"], _TIME_FORMAT)
                - datetime.strptime(play["PCTIMESTRING"], _TIME_FORMAT)
            ).total_seconds()
        )
        > _MAX_TIME_DELTA
    )


def _is_scoring_play(play):
    """
    suppose to check here if the score is changed from the last play or something. for now just return True.
    In that case value stocks will be counted just as block or assists that their next play ended with less than 7 sec.
    """
    return True


def _next_play_is_valid(next_play, play):
    if _is_time_valid(next_play, play) and _is_scoring_play(next_play):
        return True


def _get_player_id_if_play_is_value_stock(next_play, play):
    if _play_is_stock(play) and _next_play_is_valid(next_play, play):
        return play["PLAYER3_ID"] if not play["PLAYER2_ID"] else play["PLAYER2_ID"]


if __name__ == "__main__":
    game_ids = _get_game_ids_by_season_and_type(_2023, _PLAYOFFS)
    for game_id in game_ids:
        play_by_play = PlayByPlayV2(game_id=game_id).get_normalized_dict()["PlayByPlay"]
        players_id = [
            _get_player_id_if_play_is_value_stock(next_play, play)
            for next_play, play in pairwise(play_by_play)
            if _get_player_id_if_play_is_value_stock(next_play, play)
        ]
        print(
            f"for game ID {game_id} we got {len(players_id)} players that blocked or stealed with next play ended less than 7 secods"
        )
