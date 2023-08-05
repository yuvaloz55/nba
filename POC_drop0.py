import collections
import enum
import logging
from datetime import datetime
from itertools import tee
from typing import Iterator, cast

import attrs
import pandas as pd
from nba_api.stats.endpoints import LeagueGameFinder, PlayByPlayV2

_TYPE = "Playoffs"
_YEAR = "2022-23"
_TIME_FORMAT = "%M:%S"
_MAX_TIME_DELTA = 7
_PUBLISH_FLAG = True
_NBA_LEAGUE_ID = "00"


def _setup_logger() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")


@enum.unique
class Plays(enum.IntEnum):
    FG_MADE = 1
    FG_MISSED = 2
    OFFENSIVE_REBOUND = 4
    TURNOVER = 5


@attrs.frozen
class InitFollowPlays:
    init_play: Plays
    follow_plays: tuple[Plays]

    @classmethod
    def from_play(cls, init_play, follow_plays):
        return cls(init_play, follow_plays)


@attrs.frozen
class PlayerInitFollowData:
    player_name: str | None
    init_play: str
    follow_play: str

    @classmethod
    def from_data(cls, player_name, init_play, follow_play):
        return cls(player_name, init_play, follow_play)


@attrs.frozen
class GameDetails:
    game_id: str
    matchup: str

    @classmethod
    def from_game_data(cls, game_data):
        return cls(game_data["GAME_ID"], game_data["MATCHUP"])


_OFFENSIVE_REBOUND_FLOW = InitFollowPlays.from_play(
    Plays.OFFENSIVE_REBOUND, (Plays.FG_MADE, Plays.FG_MISSED, Plays.TURNOVER)
)


def _pairwise(iterable: Iterator) -> zip:
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def _remove_duplicates(games_data: list[dict[str, str]]) -> list[GameDetails]:
    unique_games_id = []
    unique_games_data = []
    for game_data in games_data:
        if game_data["GAME_ID"] in unique_games_id:
            continue

        unique_games_id.append(game_data["GAME_ID"])
        unique_games_data.append(GameDetails.from_game_data(game_data))

    return unique_games_data


def _get_game_ids_by_season_and_type(season: str, season_type: str) -> list[GameDetails]:
    games_data = LeagueGameFinder(
        season_nullable=season, season_type_nullable=season_type, league_id_nullable=_NBA_LEAGUE_ID
    ).get_normalized_dict()["LeagueGameFinderResults"]
    logging.info("Fetching all games for season year %s and type %s", season, season_type)
    return _remove_duplicates(games_data)


def _is_time_valid(play_timestamp: str, next_play_timestamp: str) -> bool:
    return (
        datetime.strptime(play_timestamp, _TIME_FORMAT) - datetime.strptime(next_play_timestamp, _TIME_FORMAT)
    ).total_seconds() <= _MAX_TIME_DELTA


def _is_follow_play(play_event: int, follow_plays: tuple[Plays]) -> bool:
    for follow_play in follow_plays:
        if play_event == follow_play:
            return True

    return False


def _get_init_and_follow_play(
    play: dict[str, str | int], next_play: dict[str, str | int], init_and_follow_plays: InitFollowPlays
) -> PlayerInitFollowData | None:
    play_event = cast(int, play["EVENTMSGTYPE"])
    next_play_event = cast(int, next_play["EVENTMSGTYPE"])
    play_timestamp = cast(str, play["PCTIMESTRING"])
    next_play_timestamp = cast(str, next_play["PCTIMESTRING"])
    if play_event == init_and_follow_plays.init_play and _is_time_valid(play_timestamp, next_play_timestamp):
        if _is_follow_play(next_play_event, init_and_follow_plays.follow_plays):
            return PlayerInitFollowData(play["PLAYER1_NAME"], Plays(play_event).name, Plays(next_play_event).name)

    return None


def _update_games_data_with_plays(
    game_data: collections.defaultdict, player_init_follow_data: PlayerInitFollowData | None
) -> collections.defaultdict:
    if player_init_follow_data is not None:
        game_data[player_init_follow_data.player_name][player_init_follow_data.init_play] += 1
        game_data[player_init_follow_data.player_name][player_init_follow_data.follow_play] += 1

    return game_data


def _update_games_data(
    all_games_data: collections.defaultdict, game_id: str, init_and_follow_plays: InitFollowPlays
) -> collections.defaultdict:
    play_by_play_data = PlayByPlayV2(game_id=game_id).get_normalized_dict()["PlayByPlay"]
    for play, next_play in _pairwise(play_by_play_data):
        player_init_follow_data = _get_init_and_follow_play(play, next_play, init_and_follow_plays)
        all_games_data = _update_games_data_with_plays(all_games_data, player_init_follow_data)
    return all_games_data


def _get_data_from_all_games_id(
    games_details: GameDetails, init_and_follow_plays: InitFollowPlays
) -> collections.defaultdict[collections.defaultdict, int]:
    all_games_data = collections.defaultdict(lambda: collections.defaultdict(int))
    for game_details in games_details:
        logging.info("Updating data from game id %s: %s", game_details.game_id, game_details.matchup)
        all_games_data = _update_games_data(all_games_data, game_details.game_id, init_and_follow_plays)
    return all_games_data


def _publish_xlsx(games_data: collections.defaultdict, publish: bool) -> None:
    if publish:
        logging.info("Finish collecting data and publishing dataframe")
        games_dataframe = pd.DataFrame.from_dict(games_data, orient="index")
        games_dataframe.to_excel("POC_drop0.xlsx")


if __name__ == "__main__":
    _setup_logger()
    init_and_follow_plays = _OFFENSIVE_REBOUND_FLOW
    publish_to_excel = _PUBLISH_FLAG
    games_details = _get_game_ids_by_season_and_type(_YEAR, _TYPE)
    all_games_data = _get_data_from_all_games_id(games_details, init_and_follow_plays)
    games_dataframe = _publish_xlsx(all_games_data, publish_to_excel)
