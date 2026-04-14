# Performance overview

## Timings

Indicative timings for matching 100,000 messy addresses are as follows.

Note that runtimes depend on whether the canonical data covers a local council region or the whole UK.

| Task | Local council region | Full country |
|------|---------------------|--------------|
| 1. Create data package and API key | 5 minutes | 5 minutes |
| 2. Install Python, uv, and `uk_address_matcher` | 5 minutes | 5 minutes |
| 3. Download and process OS data into a flat file | 5 seconds[^1] | 4 minutes[^2] |
| 4. Pre-process indexes and features | Not necessary | 4 min 50 sec |
| 5. Match 100,000 records | 18 seconds | 26 seconds |

[^1]: Plus ~15 seconds to download the data.
[^2]: Plus ~18 minutes to download the data.

These timings are measured on a MacBook Pro M4 Max.

Steps 1–3 are one-off; subsequent matching runs only require step 5 (or steps 4–5 for the full UK dataset).

## Benchmarking

There many different address matching solutions and it can be hard to compare performance between them.

Luckily, there are several open datasets of labelled address data that can be used to benchmark accuracy.

In this section, we set out `uk_address_matcher`'s accuracy against these labelled datasets.


### Hackney Council data

The Hackney Council dataset is available [here](https://www.datadaptive.com/addr/.)

The following script takes 26 seconds to run against 114,544 labelled records.

<details>
  <summary>Expand to see Hackney benchmarking script</summary>

```python
import pyarrow as pa
import duckdb
from uk_address_matcher import AddressMatcher, ExactMatchStage, SplinkStage
import uk_address_matcher


import time
import logging


start_time = time.time()
con = duckdb.connect()

path_to_all_canonical = "path_to_output_from_ukam-os-builder_tool"

sql = f"""
select *
from read_parquet('{path_to_all_canonical}')
where lowertierlocalauthoritygsscode = 'E09000012'
and substr(classificationcode, 1, 1) = 'R'
"""
df_canonical = con.sql(sql)


path = "path_to_HACKNEY_CTBANDS_ONSUD_202507.csv"
raw_labelled_data = con.read_csv(path)
raw_labelled_data.count("*").show()


sql = """
select propref as unique_id,
concat_ws(' ', addr1, addr2, addr3, addr4) as address_concat,
uprn as ukam_label,
postcode
from raw_labelled_data
where uprn is not null
and uprn in (select unique_id from df_canonical)

"""
df_messy = con.sql(sql)
df_messy.count("*").show()


matcher = AddressMatcher(
    canonical_addresses=df_canonical,
    addresses_to_match=df_messy,
    con=con,
    stages=[
        ExactMatchStage(),
        SplinkStage(
            final_distinguishability_threshold=1,
        ),
    ],
)

result = matcher.match()

end_time = time.time()
print(f"Execution time: {end_time - start_time} seconds")

chart = result.accuracy_analysis(output_type="precision_recall", add_metrics=["f1"], match_weight_round_to_nearest=1)
accuracy_table = result.accuracy_analysis(output_type="table", add_metrics=["f1"], match_weight_round_to_nearest= 1)

df = pa.Table.from_pylist(accuracy_table)
con.sql("select * from df").show(max_width=100000, max_rows=100000)
```

Note that we:

- Filter out any rows from the messy dataset where the label UPRN does not exist in our canonical dataset. We could not be expected to match these
- Filter the canonical dataset down to the Hackney council region, and to residential properties.

</details>



It achieves:

- 99.6% precision with recall of 71%
- 99.0% precision with recall of 94%

The full precision-recall curve is shown below:

```vegalite
{
    "schema-url": "assets/charts/hackney_precision_recall.json"
}
```

Manual review of the 'false positives' suggests many may in fact be true positives (that the "ground truth" labels contains errors).  So the true precision is likely higher than indicated in this chart.

### Suppressing the postcode from the Hackney data


The following chart shows how much performance is degraded if we suppress the postcode from the messy data, and re-match.

```vegalite
{
    "schema-url": "assets/charts/hackney_precision_recall_postcode_suppressed.json"
}
```

The region of recall between 0% and 25% is now populated because there are no longer any exact matches (which requires a match on postcode); all matches are now Splink matches.

### Mid Sussex District Council business rates data

This dataset is available [here](https://www.midsussex.gov.uk/housing-council-tax/council-tax-benefits-and-business-rates/business-rates/open-data-business-rates/)

The following script takes 2 seconds to run against 3,756 labelled records.

<details>
  <summary>Expand to see Mid Sussex benchmarking script</summary>

```python
import time

import altair as alt
import duckdb
import pyarrow as pa

from uk_address_matcher import (
    AddressMatcher,
    ExactMatchStage,
    SplinkStage,
    UniqueTrigramStage,
)

start_time = time.time()


con = duckdb.connect()
con.execute("INSTALL excel;")
con.execute("LOAD excel;")


path_to_all_canonical = "path_to_output_from_ukam-os-builder_tool"

sql = f"""
SELECT *
FROM read_parquet('{path_to_all_canonical}')
WHERE lowertierlocalauthoritygsscode = 'E07000228'
  AND SUBSTR(classificationcode, 1, 1) = 'C'
"""
df_canonical = con.sql(sql)


sql = """
SELECT *
FROM read_xlsx(
	'path_to_mid_sussex_business_rates_data.xlsx',
	all_varchar = true
)
"""
business_rates_data = con.sql(sql)


sql = """
WITH cleaned AS (
	SELECT
		NULLIF(NULLIF(TRIM("Property Reference"), ''), 'NULL') AS property_reference,
		NULLIF(NULLIF(TRIM("UPRN"), ''), 'NULL') AS uprn_raw,
		NULLIF(NULLIF(TRIM("Post Code"), ''), 'NULL') AS postcode,
		NULLIF(NULLIF(TRIM("Property Name 1"), ''), 'NULL') AS property_name_1,
		NULLIF(NULLIF(TRIM("Property Name 2"), ''), 'NULL') AS property_name_2,
		NULLIF(NULLIF(TRIM("Address 1"), ''), 'NULL') AS address_1,
		NULLIF(NULLIF(TRIM("Address 2"), ''), 'NULL') AS address_2,
		NULLIF(NULLIF(TRIM("Address 3"), ''), 'NULL') AS address_3,
		NULLIF(NULLIF(TRIM("Address 4"), ''), 'NULL') AS address_4
	FROM business_rates_data
),
uprn_normalised AS (
	SELECT
		property_reference,
		TRY_CAST(NULLIF(LTRIM(uprn_raw, '0'), '') AS BIGINT) AS uprn_bigint,
		postcode,
		property_name_1,
		property_name_2,
		address_1,
		address_2,
		address_3,
		address_4
	FROM cleaned
)
SELECT
	property_reference AS unique_id,
	CONCAT_WS(
		' ',
		property_name_1,
		property_name_2,
		address_1,
		address_2
	) AS address_concat,
	uprn_bigint AS ukam_label,
	UPPER(REPLACE(postcode, ' ', '')) AS postcode
FROM uprn_normalised
WHERE property_reference IS NOT NULL
  AND uprn_bigint IS NOT NULL
  AND uprn_bigint IN (SELECT unique_id FROM df_canonical)
  AND (
	  property_name_1 IS NOT NULL
	  OR property_name_2 IS NOT NULL
	  OR address_1 IS NOT NULL
	  OR address_2 IS NOT NULL
	  OR address_3 IS NOT NULL
	  OR address_4 IS NOT NULL
  )
"""
df_messy = con.sql(sql)
df_messy.count("*").show()


matcher = AddressMatcher(
    canonical_addresses=df_canonical,
    addresses_to_match=df_messy,
    con=con,
    stages=[
        ExactMatchStage(),
        UniqueTrigramStage(),
        SplinkStage(
            final_distinguishability_threshold=2,
        ),
    ],
)


result = matcher.match()

end_time = time.time()
print(f"Execution time: {end_time - start_time} seconds")


chart = result.accuracy_analysis(
    output_type="precision_recall",
    add_metrics=["f1"],
    match_weight_round_to_nearest=1,
)



accuracy_table = result.accuracy_analysis(
    output_type="table",
    add_metrics=["f1"],
    match_weight_round_to_nearest=1,
)

df = pa.Table.from_pylist(accuracy_table)

```

Note that we:

- Filter out any rows from the messy dataset where the label UPRN does not exist in our canonical dataset. We could not be expected to match these
- Filter the canonical dataset down to the Mid Sussex District Counil region, and to commercial properties.

</details>

The full precision-recall curve is shown below:

```vegalite
{
	"schema-url": "assets/charts/mid_sussex_precision_recall.json"
}
```