from pathlib import Path
import sys

import pandas as pd
from sqlalchemy import text


ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from database.db_utils import get_engine


def get_metal_data(metal_name: str) -> pd.DataFrame:
    query = text("""
        SELECT as_of_date, metric, value, unit, source
        FROM clean.observations
        WHERE metal = :metal
        ORDER BY as_of_date, metric
    """)
    df = pd.read_sql(query, get_engine(), params={"metal": metal_name})
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    return df


def pivot_metric(df: pd.DataFrame, metric_name: str) -> pd.Series:
    data = df[df["metric"] == metric_name].copy()
    data = data.sort_values("as_of_date")
    data = data.drop_duplicates(subset=["as_of_date"], keep="last")
    data = data.set_index("as_of_date")["value"]
    return data


def add_range_selector(fig):
    fig.update_xaxes(
        rangeslider_visible=True,
        rangeselector=dict(
            buttons=list([
                dict(count=1, label="1m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(count=1, label="1y", step="year", stepmode="backward"),
                dict(step="all", label="All")
            ]),
            bgcolor="rgba(255, 255, 255, 0.8)",
            activecolor="rgba(100, 149, 237, 0.5)",
            x=0,
            y=1.02
        )
    )
