from dataclasses import dataclass


@dataclass
class Match:
    bet365_id: str
    sport_id: str
    league_id: str
    home_team_id: str
    away_team_id: str
    event_time: str
    time_status: int
    final_score: str | None = None
    id: int | None = None
    retrieved_at: str | None = None
    updated_at: str | None = None

    @property
    def is_finished(self) -> bool:
        return self.final_score is not None and self.time_status == 3


@dataclass
class GameMap:
    match_id: int
    map_number: int
    id: int | None = None
    created_at: str | None = None


@dataclass
class MapStatistic:
    map_id: int
    stat_name: str
    home_value: str | None = None
    away_value: str | None = None
    id: int | None = None
    created_at: str | None = None