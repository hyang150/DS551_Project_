"""
downloader.py — Stock data download & local cache.
Supports CLI selection of symbols and date range.
"""

import pandas as pd
import yfinance as yf
from pathlib import Path
from rich.console import Console

console = Console()

# Default config
DEFAULT_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
DEFAULT_START   = "2015-01-01"
DEFAULT_END     = "2025-01-01"

# Extended symbol pool — used for the large-dataset experiment
EXTENDED_SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "META", "TSLA", "BRK-B", "JPM", "V",
    "UNH", "XOM", "JNJ", "WMT", "PG",
    "MA", "HD", "CVX", "MRK", "ABBV",
    "KO", "PEP", "COST", "AVGO", "TMO",
    "MCD", "CSCO", "ACN", "ABT", "DHR",
]


def fetch_stock_data(
    symbols: list[str] | None = None,
    start: str = DEFAULT_START,
    end: str = DEFAULT_END,
    data_dir: Path = Path.cwd() / "data",
) -> pd.DataFrame:
    """
    Download OHLCV data from Yahoo Finance with a local Parquet + CSV cache.

    Parameters
    ----------
    symbols : list[str]
        List of ticker symbols; defaults to the 5 large-caps when None.
    start, end : str
        Date range in YYYY-MM-DD format.
    data_dir : Path
        Directory used for the on-disk cache.

    Returns
    -------
    pd.DataFrame  (Date, Symbol, Open, High, Low, Close, Volume)
    """
    if symbols is None:
        symbols = DEFAULT_SYMBOLS

    data_dir.mkdir(parents=True, exist_ok=True)

    # Cache filename keyed by symbol count + date range
    tag = f"{len(symbols)}stocks_{start[:4]}_{end[:4]}"
    parquet_path = data_dir / f"{tag}.parquet"
    csv_path     = data_dir / f"{tag}.csv"

    # Cache hit
    if parquet_path.exists():
        console.print(f"[green][+] Loaded from local cache: {parquet_path.name}[/green]")
        df = pd.read_parquet(parquet_path)
        console.print(f"    {len(df):,} rows, {df['Symbol'].nunique()} stocks")
        return df

    console.print(f"[cyan][*] Downloading {len(symbols)} stocks from Yahoo Finance ({start} ~ {end})...[/cyan]")

    frames = []
    failed = []
    for sym in symbols:
        try:
            df = yf.download(sym, start=start, end=end, auto_adjust=True, progress=False)
            df.reset_index(inplace=True)

            # yfinance >=0.2.x can return MultiIndex columns
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [col[0] for col in df.columns]

            df["Symbol"] = sym
            df = df[["Date", "Symbol", "Open", "High", "Low", "Close", "Volume"]]
            df["Date"] = pd.to_datetime(df["Date"]).dt.date
            frames.append(df)
            console.print(f"  [green]✓[/green] {sym}: {len(df)} rows")
        except Exception as e:
            console.print(f"  [red]✗[/red] {sym}: {e}")
            failed.append(sym)

    if not frames:
        raise RuntimeError("All downloads failed — check network connection")

    combined = pd.concat(frames, ignore_index=True)
    combined.sort_values(["Symbol", "Date"], inplace=True)

    # Persist to disk
    combined.to_parquet(parquet_path, index=False)
    combined.to_csv(csv_path, index=False)

    console.print(f"\n[green][+] Data saved ({len(combined):,} rows, {combined['Symbol'].nunique()} stocks)[/green]")
    console.print(f"    Parquet → {parquet_path.name}")
    console.print(f"    CSV     → {csv_path.name}")

    if failed:
        console.print(f"[yellow]    Skipped failures: {failed}[/yellow]")

    return combined
