#!/usr/bin/env python3
"""
Inspeciona 1 jogo aleatório de ontem (LOL) e salva todo o payload da API.
- Usa Bet365Client do seu projeto
- Chama upcoming(yesterday) -> escolhe 1 evento -> result(event_id) [+ tenta prematch(event_id)]
- Salva JSONs brutos em results/raw_api/
"""

import asyncio
import json
import os
import random
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

# Permite importar src.core.bet365_client a partir deste arquivo em scripts/
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.shared.core.bet365_client import Bet365Client  # noqa: E402

LOL_SPORT_ID = 151
OUTDIR = Path(__file__).parent.parent / "results" / "raw_api"
OUTDIR.mkdir(parents=True, exist_ok=True)


@dataclass
class PickedEvent:
    event_id: str
    league_name: str
    home_name: str
    away_name: str
    raw_event: Dict[str, Any]


def day_str_for(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def is_lol_event(ev: Dict[str, Any]) -> bool:
    league_name = (ev.get("league") or {}).get("name", "").strip()
    return league_name.startswith("LOL -")


def is_final_like(ev: Dict[str, Any]) -> bool:
    """
    Heurística simples: se o campo 'time_status' do evento veio 3/ "3".
    Nem sempre upcoming reflete finalizados, mas ajuda a priorizar jogos encerrados.
    """
    ts = ev.get("time_status")
    try:
        return int(ts) == 3
    except Exception:
        return str(ts) == "3"


def summarize_top_keys(tag: str, payload: Dict[str, Any]) -> None:
    print(f"\n===== RESUMO DAS CHAVES ({tag}) =====")
    if not isinstance(payload, dict):
        print(f"({tag}) não é dict, tipo={type(payload)}")
        return
    keys = list(payload.keys())
    print(f"Total de chaves no nível 1: {len(keys)}")
    print("Algumas chaves:", ", ".join(keys[:20]))
    # Se for o envelope padrão {success, results, ...}
    results = payload.get("results")
    if isinstance(results, list) and results:
        first = results[0]
        if isinstance(first, dict):
            print(f"Chaves do primeiro item em results ({tag}):")
            print(", ".join(list(first.keys())[:40]))


async def fetch_yesterday_events(
    client: Bet365Client, seed: Optional[int] = None
) -> List[Dict[str, Any]]:
    yesterday = datetime.now() - timedelta(days=1)
    day = day_str_for(yesterday)
    print(
        f"🔎 Buscando eventos de ontem ({yesterday.strftime('%Y-%m-%d')}) -> day={day}"
    )
    data = await client.upcoming(sport_id=LOL_SPORT_ID, day=day)
    if not data or data.get("success") != 1:
        print("⚠️  upcoming não retornou success=1 ou veio vazio.")
        return []
    events = data.get("results", [])
    lol_events = [e for e in events if is_lol_event(e)]
    print(f"📦 Eventos retornados: {len(events)} | LOL filtrados: {len(lol_events)}")
    random.Random(seed).shuffle(lol_events)
    return lol_events


def pick_one_event(events: List[Dict[str, Any]]) -> Optional[PickedEvent]:
    if not events:
        return None
    # Prioriza “finalizados”; se não houver, usa qualquer um
    finals = [e for e in events if is_final_like(e)]
    pool = finals if finals else events
    ev = random.choice(pool)
    ev_id = str(ev.get("id"))
    league_name = (ev.get("league") or {}).get("name") or "?"
    home_name = (ev.get("home") or {}).get("name") or "?"
    away_name = (ev.get("away") or {}).get("name") or "?"
    return PickedEvent(
        event_id=ev_id,
        league_name=league_name,
        home_name=home_name,
        away_name=away_name,
        raw_event=ev,
    )


async def fetch_event_payloads(client: Bet365Client, event_id: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {"result": None, "prematch": None}
    # RESULT
    try:
        print(f"⬇️  Baixando result({event_id})…")
        res = await client.result(event_id)
        out["result"] = res
    except Exception as e:
        print(f"❌ Erro em result({event_id}): {e}")

    # PREMATCH (opcional, só se o client tiver)
    try:
        if hasattr(client, "prematch"):
            print(f"⬇️  Baixando prematch({event_id})…")
            pre = await client.prematch(event_id)  # pode não existir no seu client
            out["prematch"] = pre
        else:
            print("ℹ️  Bet365Client não possui método prematch(). Pulando…")
    except Exception as e:
        print(f"❌ Erro em prematch({event_id}): {e}")

    return out


def save_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


async def main(seed: Optional[int] = None):
    print("🧪 INSPEÇÃO DE JOGO ALEATÓRIO (ONTEM) — LOL")
    client = Bet365Client()
    try:
        events = await fetch_yesterday_events(client, seed=seed)
        if not events:
            print("🙁 Sem eventos para inspecionar.")
            return

        picked = pick_one_event(events)
        if not picked:
            print("🙁 Não foi possível sortear um evento.")
            return

        print("\n🎯 Evento sorteado:")
        print(f"   ID: {picked.event_id}")
        print(f"   Liga: {picked.league_name}")
        print(f"   Jogo: {picked.home_name} vs {picked.away_name}")
        print(f"   time_status (do upcoming): {picked.raw_event.get('time_status')}")

        payloads = await fetch_event_payloads(client, picked.event_id)

        # Sumário rápido no console
        if payloads.get("result") is not None:
            summarize_top_keys("result", payloads["result"])
        if payloads.get("prematch") is not None:
            summarize_top_keys("prematch", payloads["prematch"])

        # Salvar brutos
        date_tag = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        base = OUTDIR / f"{date_tag}_{picked.event_id}"

        save_json(picked.raw_event, base.with_suffix(".upcoming.json"))
        if payloads.get("result") is not None:
            save_json(payloads["result"], base.with_suffix(".result.json"))
        if payloads.get("prematch") is not None:
            save_json(payloads["prematch"], base.with_suffix(".prematch.json"))

        print("\n💾 Arquivos salvos:")
        print(f" - {os.path.relpath(base.with_suffix('.upcoming.json'))}")
        if payloads.get("result") is not None:
            print(f" - {os.path.relpath(base.with_suffix('.result.json'))}")
        if payloads.get("prematch") is not None:
            print(f" - {os.path.relpath(base.with_suffix('.prematch.json'))}")

        print(
            "\n✅ Pronto! Agora você pode abrir esses JSONs e mapear tudo que a API retorna."
        )
        print(
            "   Dica: no bot, comece mapeando `results[0]` -> campos-chave como `ss`, `time_status`, `period_stats`, etc."
        )

    finally:
        await client.close()


if __name__ == "__main__":
    # Use: python inspect_random_yesterday.py
    # Ou:   python inspect_random_yesterday.py com SEED fixa (reprodutibilidade)
    # Para mudar a seed, exporte SEED=123 e chame o script; ou edite abaixo.
    asyncio.run(main(seed=None))
