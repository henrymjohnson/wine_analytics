WITH imports_aggregated_programs AS (
	SELECT I.month
        , S.name
        , SUM(I.value) AS "value"
        , MAX(I.region_id) AS region_id
        , MAX(I.series_id) AS series_id
	FROM panel_data.imports_for_consumption AS I
		JOIN panel_data.regions AS R
			ON R.id = I.region_id
		JOIN panel_data.series AS S
			ON S.id = I.series_id
	WHERE R.name = 'Australia'
		AND R.type = 'country'
	GROUP BY I.month, S.name
), exports AS (
	SELECT X.month
        , S.name
        , X.value
        , X.series_id
        , X.region_id
	FROM panel_data.total_exports AS X
		JOIN panel_data.regions AS R
			ON R.id = X.region_id
		JOIN panel_data.series AS S
			ON S.id = X.series_id
	WHERE R.name = 'Australia'
		AND R.type = 'country'
)
SELECT I.month
	, SUM(DISTINCT CASE WHEN S.name = 'First Unit of Quantity' AND S.type = 'import' THEN I.value END) AS import_quantity
	, SUM(DISTINCT CASE WHEN S.name = 'First Unit of Quantity' AND S.type = 'export' THEN X.value END) AS export_quantity
	, SUM(DISTINCT CASE WHEN S.name = 'Customs Value' THEN I.value END) AS import_customs_value
	, SUM(DISTINCT CASE WHEN S.name = 'CIF Import Value' THEN I.value END) AS import_cif
	, SUM(DISTINCT CASE WHEN S.name = 'Landed Duty-Paid Value' THEN I.value END) AS import_landed_duty_paid
	, SUM(DISTINCT CASE WHEN S.name = 'Charges, Insurance, and Freight' THEN I.value END) AS import_charges_insurance_freight
	, SUM(DISTINCT CASE WHEN S.name = 'Dutiable Value' THEN I.value END) AS import_dutiable_value
	, SUM(DISTINCT CASE WHEN S.name = 'FAS Value' AND S.type = 'export' THEN X.value END) AS export_fas_value
FROM imports_aggregated_programs AS I
	FULL OUTER JOIN exports AS X
		ON X.month = I.month
			AND X.region_id = I.region_id
	JOIN panel_data.series AS S
		ON S.id = I.series_id OR S.id = X.series_id
WHERE TRUE
GROUP BY I.month
ORDER BY I.month DESC;