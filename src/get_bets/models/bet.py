from dataclasses import dataclass


@dataclass
class Event:
    event_id: str
    league_name: str
    match_date: str
    home_team_id: int
    away_team_id: int
    home_team_name: str
    away_team_name: str
    FI: str | None = None
    bet365_key: str | None = None
    match_timestamp: int | None = None
    status: str = "upcoming"

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "FI": self.FI,
            "bet365_key": self.bet365_key,
            "home_team_id": self.home_team_id,
            "away_team_id": self.away_team_id,
            "league_name": self.league_name,
            "match_date": self.match_date,
            "match_timestamp": self.match_timestamp,
            "status": self.status,
        }


@dataclass
class BettingLine:
    event_id: str
    market_type: str
    selection: str
    odds: float
    line: str
    map_number: int | None = None
    odds_type: str = "main"


@dataclass
class Bet:
    event_id: str
    market_type: str
    selection: str
    odds: float
    line: str
    roi_average: float
    fair_odds: float
    odds_type: str = "main"
    map_number: int | None = None
    stake: float = 1.0
    potential_win: float = 0.0

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "odds_type": self.odds_type,
            "market_type": self.market_type,
            "selection": self.selection,
            "odds": self.odds,
            "line": self.line,
            "map_number": self.map_number,
            "roi_average": self.roi_average,
            "fair_odds": self.fair_odds,
            "stake": self.stake,
            "potential_win": self.potential_win,
        }


@dataclass
class TeamStats:
    team_name: str
    stat_type: str
    values: list[float]

    def calculate_probability(self, handicap: float, is_over: bool) -> float:
        if not self.values:
            return 0.0
        
        if is_over:
            wins = sum(1 for v in self.values if v > handicap)
        else:
            wins = sum(1 for v in self.values if v < handicap)
        
        return wins / len(self.values)