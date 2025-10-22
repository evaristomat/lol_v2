import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

# Caminhos
DB_PATH = Path("data/lol_bets.db")
OUTPUT_DIR = Path("resultados")
OUTPUT_DIR.mkdir(exist_ok=True)


# Função para carregar tabelas SQLite
def load_data():
    with sqlite3.connect(DB_PATH) as conn:
        bets = pd.read_sql_query("SELECT * FROM bets", conn)
        events = pd.read_sql_query("SELECT * FROM events", conn)
        teams = pd.read_sql_query("SELECT * FROM teams", conn)
    return bets, events, teams


# Função para preparar DataFrame consolidado
def preprocess(bets, events, teams):
    # preserve original bet id
    bets = bets.copy()
    if "id" in bets.columns:
        bets = bets.rename(columns={"id": "bet_id"})

    # merge events (usando sufixos para evitar colisões de coluna)
    df = bets.merge(events, on="event_id", how="left", suffixes=("_bet", "_event"))

    # merge home/away team names
    teams_map = teams.set_index("id")["name"]
    df["home_team"] = df["home_team_id"].map(teams_map)
    df["away_team"] = df["away_team_id"].map(teams_map)

    # Normalizar/obter created_at: prefira created_at do bets, caia para created_at do events se necessário
    created_bet = df.columns[df.columns.str.endswith("created_at_bet")]
    created_event = df.columns[df.columns.str.endswith("created_at_event")]

    if len(created_bet) > 0:
        df["created_at"] = pd.to_datetime(df[created_bet[0]], errors="coerce")
    elif "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    else:
        # tenta pegar qualquer coluna que contenha 'created_at'
        cand = [c for c in df.columns if "created_at" in c]
        if cand:
            df["created_at"] = pd.to_datetime(df[cand[0]], errors="coerce")
        else:
            df["created_at"] = pd.NaT

    # Converter match_date se existir
    if "match_date" in df.columns:
        df["match_date"] = pd.to_datetime(df["match_date"], errors="coerce")
    else:
        df["match_date"] = pd.NaT

    # Calcular lucro com base em actual_win (regras do usuário):
    # actual_win == 0 -> aposta pendente (não considerada nas estatísticas)
    # actual_win == -1 -> aposta perdida -> lucro = -stake
    # actual_win > 0 -> aposta ganha -> lucro = actual_win
    def calcular_lucro(aw, stake):
        try:
            if pd.isna(aw):
                return 0
            awf = float(aw)
        except Exception:
            return 0

        if awf == 0:
            return 0
        if awf == -1:
            return -(float(stake) if pd.notna(stake) else 0.0)
        if awf > 0:
            return awf
        return 0

    df["stake"] = pd.to_numeric(df["stake"], errors="coerce").fillna(0.0)
    df["actual_win"] = pd.to_numeric(df["actual_win"], errors="coerce")
    df["profit"] = df.apply(
        lambda r: calcular_lucro(r["actual_win"], r["stake"]), axis=1
    )

    # marcar status pendente para filtragem fácil (True se actual_win == 0 OR actual_win is NULL)
    df["is_pending"] = df["actual_win"].apply(
        lambda x: True if pd.isna(x) or float(x) == 0 else False
    )

    return df


# Estatísticas gerais
def general_stats(df):
    # remover pendentes
    df_validas = df[~df["is_pending"]].copy()

    total_bets = len(df_validas)
    total_stake = df_validas["stake"].astype(float).sum()
    total_profit = df_validas["profit"].astype(float).sum()
    roi = (total_profit / total_stake) * 100 if total_stake > 0 else 0.0
    win_rate = (
        (len(df_validas[df_validas["profit"] > 0]) / total_bets) * 100
        if total_bets > 0
        else 0.0
    )

    print(f"\n===== ESTATÍSTICAS GERAIS =====")
    print(f"Apostas válidas: {total_bets}")
    print(f"Stake total: {total_stake:.2f}")
    print(f"Lucro total: {total_profit:.2f}")
    print(f"ROI: {roi:.2f}%")
    print(f"Win Rate: {win_rate:.2f}%")

    return df_validas


# Estatísticas por categoria
def group_stats(df_validas, column, name):
    # proteger contra colunas inexistentes
    if column not in df_validas.columns:
        print(f"Coluna '{column}' não existe — pulando {name}.")
        return

    grouped = (
        df_validas.groupby(column)
        .agg(
            count=("bet_id", "count"),
            stake_sum=("stake", "sum"),
            profit_sum=("profit", "sum"),
            profit_mean=("profit", "mean"),
        )
        .sort_values("profit_sum", ascending=False)
    )

    # evitar divisão por zero ao calcular ROI
    stakes = df_validas.groupby(column)["stake"].sum()
    grouped["roi"] = (grouped["profit_sum"] / stakes) * 100

    grouped.to_csv(OUTPUT_DIR / f"stats_{name}.csv")
    print(f"\nTop {name} pelo lucro:")
    print(grouped.head(10))


# Gráficos mensais
def plot_monthly(df_validas):
    # usa match_date; cai para created_at se match_date for NaT
    df_plot = df_validas.copy()
    df_plot.loc[df_plot["match_date"].isna(), "match_date"] = df_plot.loc[
        df_plot["match_date"].isna(), "created_at"
    ]

    # atribuição usando .loc para evitar SettingWithCopyWarning
    df_plot.loc[:, "month"] = df_plot["match_date"].dt.to_period("M").astype(str)

    monthly = df_plot.groupby("month")["profit"].sum()

    plt.figure(figsize=(8, 4))
    monthly.plot(kind="bar")
    plt.title("Lucro Mensal")
    plt.xlabel("Mês")
    plt.ylabel("Lucro Total")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "lucro_mensal.png")
    plt.close()

    monthly_count = df_plot.groupby("month")["bet_id"].count()
    plt.figure(figsize=(8, 4))
    monthly_count.plot(kind="bar")
    plt.title("Número de Apostas por Mês")
    plt.xlabel("Mês")
    plt.ylabel("Quantidade de Apostas")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "apostas_mensais.png")
    plt.close()


# Execução principal
def main():
    bets, events, teams = load_data()
    df = preprocess(bets, events, teams)

    df_validas = general_stats(df)

    for col, name in [
        ("market_type", "market_type"),
        ("odds_type", "odds_type"),
        ("strategy", "strategy"),
        ("league_name", "league"),
    ]:
        group_stats(df_validas, col, name)

    plot_monthly(df_validas)

    df.to_csv(OUTPUT_DIR / "bets_detalhadas.csv", index=False)
    print("\nRelatórios salvos em:", OUTPUT_DIR.resolve())


if __name__ == "__main__":
    main()
