from typing import Dict


def is_lol_event(event: Dict) -> bool:
    league_name = event.get("league", {}).get("name", "").strip()

    if not league_name.startswith("LOL -"):
        return False

    non_lol_keywords = [
        "VALORANT",
        "CS2",
        "CS:GO",
        "DOTA",
        "OVERWATCH",
        "RAINBOW SIX",
        "R6",
        "ROCKET LEAGUE",
        "FIFA",
        "CALL OF DUTY",
        "COD",
        "STARCRAFT",
        "WARCRAFT",
        "HEARTHSTONE",
        "FORTNITE",
        "PUBG",
    ]

    return not any(kw.lower() in league_name.lower() for kw in non_lol_keywords)
