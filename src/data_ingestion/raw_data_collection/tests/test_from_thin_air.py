import pandas as pd
import pytest
from ..from_thin_air import get_retaliatory_wine_tariff_months


def test_get_retaliatory_wine_tariff_months():
    df = get_retaliatory_wine_tariff_months()

    assert df.shape[1] == 2
    assert set(df.columns) == set(['date', 'retaliatory_wine_tariffs'])

    assert pd.api.types.is_datetime64_any_dtype(df['date'])
    assert pd.api.types.is_integer_dtype(df['retaliatory_wine_tariffs'])

    assert all(df.loc[df['date'] < '2019-10', 'retaliatory_wine_tariffs'] == 0)
    assert all(df.loc[(df['date'] >= '2019-10') & (df['date'] <= '2021-06'), 'retaliatory_wine_tariffs'] == 1)
   
