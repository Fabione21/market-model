import os
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

INDEX_TICKERS = ["^GSPC"]  # S&P 500

def load_equity_universe_1000() -> list[str]:
    sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    sp400_url = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"
    sp600_url = "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"

    def tickers_from(url: str, preferred: str) -> list[str]:
        tables = pd.read_html(url)
        df = tables[0]
        col = preferred
        if col not in df.columns:
            for alt in ["Ticker symbol", "Symbol", "Ticker"]:
                if alt in df.columns:
                    col = alt
                    break
        return df[col].astype(str).str.strip().tolist()

    t500 = tickers_from(sp500_url, "Symbol")
    t400 = tickers_from(sp400_url, "Symbol")
    t600 = tickers_from(sp600_url, "Symbol")

    seen = set()
    combined = []
    for t in (t500 + t400 + t600):
        t = t.replace(".", "-")  # es. BRK.B -> BRK-B su Yahoo
        if t and t not in seen:
            seen.add(t)
            combined.append(t)
        if len(combined) >= 1000:
            break
    return combined

def ensure_dirs():
    os.makedirs("data/prices", exist_ok=True)
    os.makedirs("data/indices", exist_ok=True)

def month_path(kind: str, dt: pd.Timestamp) -> str:
    y = f"{dt.year:04d}"
    m = f"{dt.month:02d}.csv"
    base = f"data/{kind}/{y}"
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, m)

def load_month_file(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=["date", "ticker", "adj_close"])
    return pd.read_csv(path, parse_dates=["date"])

def append_to_month_files(kind: str, df_long: pd.DataFrame) -> None:
    if df_long.empty:
        return
    df_long["date"] = pd.to_datetime(df_long["date"])
    for (year, month), chunk in df_long.groupby([df_long["date"].dt.year, df_long["date"].dt.month]):
        dt = pd.Timestamp(year=year, month=month, day=1)
        path = month_path(kind, dt)
        existing = load_month_file(path)
        combined = pd.concat([existing, chunk], ignore_index=True)
        combined.drop_duplicates(subset=["date", "ticker"], keep="last", inplace=True)
        combined.sort_values(["date", "ticker"], inplace=True)
        combined.to_csv(path, index=False)

def fetch_adj_close_long(tickers: list[str], start: str) -> pd.DataFrame:
    df = yf.download(
        tickers=tickers,
        start=start,
        progress=False,
        auto_adjust=False,
        threads=True,
        group_by="column",
    )
    if df.empty:
        return pd.DataFrame(columns=["date", "ticker", "adj_close"])

    adj = df["Adj Close"] if isinstance(df.columns, pd.MultiIndex) else df["Adj Close"]

    if isinstance(adj, pd.Series):
        out = adj.reset_index()
        out.columns = ["date", "adj_close"]
        out["ticker"] = tickers[0]
        return out[["date", "ticker", "adj_close"]].dropna(subset=["adj_close"])

    out = adj.reset_index()
    if "Date" in out.columns:
        out = out.rename(columns={"Date": "date"})
    out = out.melt(id_vars=["date"], var_name="ticker", value_name="adj_close")
    out = out.dropna(subset=["adj_close"])
    return out[["date", "ticker", "adj_close"]]

def latest_date_in_folder(folder: str):
    if not os.path.exists(folder):
        return None
    latest = None
    for root, _, files in os.walk(folder):
        for f in files:
            if f.endswith(".csv"):
                path = os.path.join(root, f)
                try:
                    df = pd.read_csv(path, usecols=["date"], parse_dates=["date"])
                    if not df.empty:
                        m = df["date"].max()
                        latest = m if latest is None else max(latest, m)
                except Exception:
                    continue
    return latest

def main():
    ensure_dirs()

    last_prices = latest_date_in_folder("data/prices")
    last_indices = latest_date_in_folder("data/indices")
    last = None
    for x in [last_prices, last_indices]:
        if x is not None:
            last = x if last is None else max(last, x)

    if last is None:
        start_dt = datetime.today() - timedelta(days=365 * 10)
    else:
        start_dt = (pd.to_datetime(last) + pd.Timedelta(days=1)).to_pydatetime()

    start = start_dt.strftime("%Y-%m-%d")
    print(f"Start download from: {start}")

    equity = []
    with open("config/equity_universe.txt", "r", encoding="utf-8") as f:
        for line in f:
            t = line.strip()
            if t and not t.startswith("#"):
                equity.append(t)
    equity = equity[:1000]

    idx_long = fetch_adj_close_long(INDEX_TICKERS, start=start)
    append_to_month_files("indices", idx_long)

    batch_size = 100
    parts = []
    for i in range(0, len(equity), batch_size):
        batch = equity[i:i + batch_size]
        parts.append(fetch_adj_close_long(batch, start=start))

    prices_long = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["date","ticker","adj_close"])
    append_to_month_files("prices", prices_long)

    print("Done.")

if __name__ == "__main__":
    main()
