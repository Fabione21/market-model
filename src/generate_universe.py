import os
from typing import List, Set, Tuple
import pandas as pd

# Fonte affidabile (CSV già pronti, simboli in stile Yahoo)
BASE = "https://yfiua.github.io/index-constituents"

SOURCES = {
    # USA
    "sp500": f"{BASE}/constituents-sp500.csv",
    "nasdaq100": f"{BASE}/constituents-nasdaq100.csv",
    "dowjones": f"{BASE}/constituents-dowjones.csv",

    # Europa
    "ftse100": f"{BASE}/constituents-ftse100.csv",
    "dax": f"{BASE}/constituents-dax.csv",

    # Asia/HK
    "hsi": f"{BASE}/constituents-hsi.csv",

    # Cina (Emergenti)
    "csi300": f"{BASE}/constituents-csi300.csv",
    "csi500": f"{BASE}/constituents-csi500.csv",
    "csi1000": f"{BASE}/constituents-csi1000.csv",
}

# Italia: lista mantenuta nel repo (stabile, senza scraping).
# Puoi aggiungere righe qui quando vuoi.
ITALY_MI = [
    "ENEL.MI","ENI.MI","ISP.MI","UCG.MI","STM.MI","G.MI","PRY.MI","SRG.MI","TEN.MI","TRN.MI",
    "HER.MI","BAMI.MI","LDO.MI","AMP.MI","BPE.MI","CPR.MI","MONC.MI","ERG.MI","DIA.MI","A2A.MI",
    "INW.MI","IG.MI","MB.MI","SPM.MI","BZU.MI","PST.MI","NEXI.MI","TIT.MI","UNI.MI","REC.MI",
    "AZM.MI","BMED.MI","BMPS.MI","BPSO.MI","SFER.MI","LUX.MI","RACE.MI","IP.MI","CVAL.MI","BFF.MI",
]

OUT_PATH = "config/equity_universe.txt"

def read_symbols(url: str) -> List[str]:
    df = pd.read_csv(url)
    # il repo pubblica colonne tipo "Symbol"
    col = None
    for c in ["Symbol", "symbol", "Ticker", "ticker"]:
        if c in df.columns:
            col = c
            break
    if col is None:
        raise ValueError(f"Non trovo la colonna Symbol nel CSV: {url}")

    symbols = df[col].astype(str).str.strip().tolist()
    # pulizia minima: rimuovi vuoti e indici (^)
    symbols = [s for s in symbols if s and not s.startswith("^")]
    return symbols

def take_unique(items: List[str], n: int, used: Set[str]) -> List[str]:
    picked = []
    for x in items:
        if x not in used:
            used.add(x)
            picked.append(x)
        if len(picked) >= n:
            break
    return picked

def main():
    os.makedirs("config", exist_ok=True)

    # Target: 1000 titoli globali, bilanciati per aree.
    # (È una scelta pratica per avere “mondo” senza esplodere di complessità.)
    targets = [
        ("USA",    ["sp500", "nasdaq100", "dowjones"], 550),
        ("EUROPA", ["ftse100", "dax"],                 200),
        ("ITALIA", [],                                  40),
        ("HK",     ["hsi"],                              50),
        ("CINA",   ["csi300", "csi500", "csi1000"],     160),
    ]

    used: Set[str] = set()
    final: List[Tuple[str, str]] = []  # (group, ticker)

    # Italia (hardcoded)
    for t in take_unique(ITALY_MI, 40, used):
        final.append(("ITALIA", t))

    # Altri gruppi (da CSV online)
    for group, keys, n in targets:
        if group == "ITALIA":
            continue
        pool: List[str] = []
        for k in keys:
            pool.extend(read_symbols(SOURCES[k]))
        picked = take_unique(pool, n, used)
        for t in picked:
            final.append((group, t))

    # Se per qualche motivo siamo sotto 1000 (es. ticker duplicati o rimossi),
    # riempiamo pescando ancora dalla Cina (che è ampia) e poi dagli USA.
    def fill_from(keys: List[str], want: int, label: str):
        nonlocal final, used
        pool = []
        for k in keys:
            pool.extend(read_symbols(SOURCES[k]))
        picked = take_unique(pool, want, used)
        for t in picked:
            final.append((label, t))

    if len(final) < 1000:
        fill_from(["csi1000", "csi500", "csi300"], 1000 - len(final), "FILL")

    if len(final) < 1000:
        fill_from(["sp500"], 1000 - len(final), "FILL")

    # Taglia esattamente a 1000 (se abbiamo riempito troppo)
    final = final[:1000]

    # Scrivi file
    lines = []
    lines.append("# Global equity universe (~1000) - generated automatically")
    lines.append("# One ticker per line. Lines starting with # are comments.")
    current = None
    for group, t in final:
        if group != current:
            lines.append("")
            lines.append(f"# =========================")
            lines.append(f"# {group}")
            lines.append(f"# =========================")
            current = group
        lines.append(t)

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).strip() + "\n")

    print(f"✅ Scritto {OUT_PATH} con {len(final)} ticker unici.")

if __name__ == "__main__":
    main()
