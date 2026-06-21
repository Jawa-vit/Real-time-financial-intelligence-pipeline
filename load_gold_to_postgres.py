import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine

# PostgreSQL connection
engine = create_engine(
    "postgresql+psycopg2://finance_user:finance_pass@localhost:5432/finance"
)

gold_path = Path("data/gold")

dfs = []

for folder in gold_path.glob("symbol=*"):
    df = pd.read_parquet(folder)
    df["symbol"] = folder.name.replace("symbol=", "")
    dfs.append(df)

final_df = pd.concat(dfs, ignore_index=True)

print("\nLoaded Data:")
print(final_df.head())

print("\nTotal Rows:", len(final_df))
final_df = final_df.drop(columns=["prev_close"], errors="ignore")

final_df.to_sql(
    "gold_daily_metrics",
    engine,
    if_exists="append",
    index=False
)

print("\nSUCCESS: Data loaded into PostgreSQL!")