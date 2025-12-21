import os
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

DATA_PATH = "data/prices.csv"
TICKERS = ["^GSPC"]  # S&P 500 (indice)

def ensure_data_folder():
    os.makedirs("data", exist_ok=True)

def load_existing():
    if not os.path.exists(DATA_PATH):
        return pd.DataFrame(columns=["date", "ticker", "adj_close"])
    df = pd.read_csv(DATA_PATH, parse_dates=["date"])
    return df

def get_start_date(existing_df):
    if existing_df.empty:
        # se non abbiamo dati, scarichiamo 10 anni di storico
        return (datetime.today() - timedelta(days=365 * 10)).strftime("%Y-%m-%d")
    last_date = existing_df["date"].max()
    # ripartiamo dal giorno successivo all’ultimo già salvato
    return (last_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

def fetch_new_data(start_date):
    df = yf.download(
        tickers=TICKERS,
        start=start_date,
        progress=False,
        auto_adjust=False,
        group_by="column",
    )

    if df.empty:
        return pd.DataFrame(columns=["date", "ticker", "adj_close"])

    # Se scarichi un solo ticker, Adj Close è una colonna semplice
    adj = df[["Adj Close"]].reset_index()
    adj.rename(columns={"Date": "date", "Adj Close": "adj_close"}, inplace=True)
    adj["ticker"] = "^GSPC"
    adj = adj[["date", "ticker", "adj_close"]]

    return adj.dropna(subset=["adj_close"])

def main():
    ensure_data_folder()

    existing = load_existing()
    start_date = get_start_date(existing)

    new_data = fetch_new_data(start_date)

    if new_data.empty:
        print("Nessun nuovo dato da aggiungere (probabilmente oggi non c’è ancora la chiusura).")
        return

    combined = pd.concat([existing, new_data], ignore_index=True)
    combined.drop_duplicates(subset=["date", "ticker"], keep="last", inplace=True)
    combined.sort_values(["date", "ticker"], inplace=True)

    combined.to_csv(DATA_PATH, index=False)
    print(f"Aggiornato {DATA_PATH}. Ultima data: {combined['date'].max().date()}")

if __name__ == "__main__":
    main()
