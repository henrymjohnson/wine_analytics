SELECT P.month
	, P.average_wine_price_us
	, P.producer_price_index_us
	, D.population_size
	, D.disposable_income_amount
	, P.retaliatory_wine_tariffs
FROM time_series_features.prices AS P
	JOIN time_series_features.demographics AS D
		ON D.month = P.month
ORDER BY P.month DESC