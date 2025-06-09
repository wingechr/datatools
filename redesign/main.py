import pandas as pd
from mockup import Process, Storage


def dfmult(df: pd.DataFrame, factor: float) -> pd.DataFrame:
    """Example"""
    return df * factor


st = Storage(".")


proc = Process(
    function=dfmult,
    inputs={
        # "df": "data://d1.csv",
        "df": "file://d1.csv",
        "factor": "10",
    },
    # outputs="data://d2.csv",
    default_storage=st,
    context={"project": "test"},
)


proc.run()
