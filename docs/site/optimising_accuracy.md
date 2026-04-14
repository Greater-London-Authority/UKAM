# Optimising accuracy

`uk_address_matcher` has a variety of settings that can be tuned to optimise accuracy dependnig on your use case.

In 'biggest wins' we describe the most important settings that are likely to result in unambiguously better accuracy.

In 'optimising match stages' we describe settings which are harder to make recommendations about because the best settings depend on the input data.

## Biggest wins

### If matching to Ordnance Survey data, use `ukam-os-builder` to prepare it for matching

In its [typical form](https://docs.os.uk/osngd/data-structure/address/gb-address/built-address), Ordnance Survey address data contains one address [UPRN](https://www.ordnancesurvey.co.uk/public/unique-property-reference-numbers) per row.

However, if processed in a specific way, Ordnance Survey contains variants on an address that provides a greater 'target area' to match to.  Our `ukam-os-builder` tool does this processing for you to increase the chance of a match. Use of this tool is documented in [this guide](ordnance_survey.md).

To illustrate why this is important, an single address could have two variants:

1. `Basement Flat, 10 Demo Road, Townton`
2. `Flat A, Example Court, 10 Demo Road, Townton`

By providing `uk_address_matcher` with a canonical address dataset built by `ukam-os-builder`, you will match against all these variants, giving more options for a high scoring match.

### Filter your input data to the smallest possible dataset

The smaller the number of addresses to match to, the more accurate your results are likely to be, because there is less chance of multiple similar candidates (such as two different '1 High Street' addresses in two different geographical locations).

Matching will also be faster, because there are fewer candidates to compare against.

There are two primary ways to filter your input data:

- **Geographically**.  If your input data comes from a known geographical area such as a local authority, use an extract of Ordnance Survey data for that area only.
- **By classification or other metadata**.  Ordnance Survey data contains rich metadata about each address, such as its [classification](https://docs.os.uk/osngd/code-lists/code-lists-overview/addressclassificationcodevalue).  For example, if you know your messy data is residential only, filter out non-residential addresses from the canonical dataset.

#### How to filter

The mechanism for filtering depends on whether you are [pre-processing your canonical dataset or processing it on the fly](https://moj-analytical-services.github.io/uk_address_matcher/get_started/#choose-whether-to-pre-process-your-canonical-dataset).

If you are processing data on-the-fly, then you can simply filter your data before passing it to `AddressMatcher`:


#### Filtering for on the fly processing

```python
import duckdb
from uk_address_matcher import AddressMatcher, prepare_canonical_folder

con = duckdb.connect()

df_canonical = con.read_csv("path/to/canonical.csv")

# Filter to residential addresses only
df_canonical = df_canonical.filter("substr(classificationcode, 1, 1) = 'R'")
df_messy = con.read_csv("path/to/messy.csv")

matcher = AddressMatcher(
    canonical_addresses=output_folder,
    addresses_to_match=df_messy,
    con=con,
)
result = matcher.match()

```

#### Filtering a pre-prepared datasets

If your are pre-processing your canonical dataset, consider whether users of the preprocessed file will always want a filter applied.  If so, apply this filter using prior to passing the data to the `prepare_canonical_folder` folder.

```python
import duckdb
import os
import tempfile
from uk_address_matcher import AddressMatcher, prepare_canonical_folder

con = duckdb.connect()
df_canonical = con.read_csv("path/to/canonical.csv")
df_canonical = df_canonical.filter("substr(classificationcode, 1, 1) = 'R'")

output_folder = tempfile.mkdtemp()
prepare_canonical_folder(
    df_canonical,
    output_folder=output_folder,
    con=con
)
```

However, if different users will need different filters, you can also apply a filter _after_ pre-processing the whole dataset.  This will result in a small degradation in accuracy because indices and term frequencies will be computed globally, making them less discriminative.

```python

output_folder = "path_to_prepared_canonical_folder"
df_canonical = con.read_csv("path/to/canonical.csv")

prepare_canonical_folder(
    df_canonical,
    output_folder=output_folder,
    con=con
)

matcher = AddressMatcher(
    canonical_addresses=output_folder,
    addresses_to_match=df_messy,
    canonical_address_filter="substr(classificationcode, 1, 1) = 'R'",
    con=con,
)
result = matcher.match()
```



## Optimising match stages

`uk_address_matcher` uses an 'ensemble' methodology, whereby it sequentially applies a number of  matching strategies called 'stages'.

Stages run in order. Once a record is matched by one stage, later stages do not revisit it.  The results contain a column indicating which stage produced the match.

As a result they are intended to be ordered from highest precision to lowest precision.  For example, the first stage is usually the `ExactMatchStage`, since it makes sense to find all exact matches (full address string and postcode) prior to applying any more sophisticated/fuzzy matching strategies.


### Matching stages

The available stages are as follows:

| Stage | Type | What it is good at | Accuracy implication |
|---|---|---|---|
| `ExactMatchStage` | Deterministic | Cleaned address text is already the same on both sides | Very high precision, should usually run first |
| `PeeledAddressStage` | Deterministic | One side has extra trailing locality words such as `LONDON` or `HACKNEY` | High precision, useful before probabilistic matching |
| `UniqueTrigramStage` | Deterministic | A distinctive phrase identifies one canonical address within the postcode | High precision, removes clear fuzzy cases before Splink |
| `SplinkStage` | Scored | Typos, abbreviations, partial matches, and other fuzzy cases | Precision and recall depend on threshold choice |


##### Summary recommendation

You almost always want to use the `ExactMatchStage`.  The `PeeledAddressStage` and `UniqueTrigramStage` produce high, but not perfect precision (i.e. there's a chance of a small number of false positives).

You almost always want to use the `SplinkStage` last, to attempt to find any matches missed by the previous stages.  In some cases, it may produce higher accuracy than the `PeeledAddressStage` and `UniqueTrigramStage`, which is why you do not always want to use these stages.




```python
from uk_address_matcher import (
    AddressMatcher,
    ExactMatchStage,
    PeeledAddressStage,
    UniqueTrigramStage,
    SplinkStage,
)

matcher = AddressMatcher(
    canonical_addresses=df_canonical,
    addresses_to_match=df_messy,
    con=con,
    stages=[
        ExactMatchStage(),
        PeeledAddressStage(),
        UniqueTrigramStage(),
        SplinkStage(
            final_match_weight_threshold=2.0,
            final_distinguishability_threshold=1.0,
        ),
    ],
)
```

## Tuning the Splink stage

For guidance on choosing Splink thresholds, see [here](choosing_a_matching_threshold.md).


## Stage API docs

### ExactMatchStage

::: uk_address_matcher.ExactMatchStage

### PeeledAddressStage

::: uk_address_matcher.PeeledAddressStage

### UniqueTrigramStage

::: uk_address_matcher.UniqueTrigramStage

### SplinkStage

::: uk_address_matcher.SplinkStage
