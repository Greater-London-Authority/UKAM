# Choosing a matching threshold

Fuzzy matches found by `uk_address_matcher` in the `matcher.match().matches()` table have two scores associated with them: `match_weight` and `distinguishability`.[^splink-only]

By default, all matches are returned, no matter how low these scores.  The user must therefore decide what acceptance threshold to use for these scores.

Once you have decided on a threshold, you can set it in the `SplinkStage` parameters:

```python
matcher = AddressMatcher(
    canonical_addresses=df_canonical,
    addresses_to_match=df_messy,
    con=con,
    stages=[
        ExactMatchStage(),
        PeeledAddressStage(),
        UniqueTrigramStage(),
        SplinkStage(
            final_match_weight_threshold=10.0,
            final_distinguishability_threshold=1.0,
        ),
    ],
)
```

## What do these scores mean?

### Match weight

`match_weight` is Splink's score for the selected candidate.

Higher values mean stronger evidence that the messy address and canonical address are the same place. Lower values are weaker matches. Negative values are often a sign that the candidate was retained only because the stage is in a very permissive mode. Raising `final_match_weight_threshold` filters out weak matches.

### Distinguishability

`distinguishability` is the gap in `match_weight` between the best candidate and the next best candidate for the same messy address. Higher values mean the winner is clearly ahead. Low values mean the top two candidates look similar, which is often where false positives come from. Raising `final_distinguishability_threshold` filters out ambiguous matches.

[^splink-only]: These scores are only calculated for `SplinkStage` matches and do not apply, for example, to `ExactMatchStage` matches.


To illustrate why we need both scores, consider the following examples.


#### Example 1:

##### Messy address:
```
Flat 165 Block 3 Rose Square, Hammersmith And Fulham, EG1 2EG
```

##### Candidates
```
Flat 165 A Block 3 Rose Square, Hammersmith And Fulham, EG1 2EG
Flat 165 B Block 3 Rose Square, Hammersmith And Fulham, EG1 2EG
```

These two candidates will both get a high match score due to the large amount of matching information, but will have zero distinguishability.

#### Example 2:

##### Messy address:
```
1 High St, AB1 2CD
```

##### Candidates
```
1 High Street, AB1 2CD
1 High Street, WX1 2YZ
```

Neither of these candidate will have a high match score because they are very short, but they will have high distinguishability due to the different postcodes.

## Choosing thresholds

Unfortunately, the best thresholds to use depend on various factors such as the quality and type of your input data (e.g. businesses vs residential), and whether you are matching to a small geographical area or the whole country.  There's therefore no one-size-fits-all answer to what thresholds to use.

As a starting point we recommend trying the following settings:

```python
SplinkStage(
    final_match_weight_threshold=10.0,
    final_distinguishability_threshold=1.0,
)
```

But to pick optimal thresholds for your use case, it's best to used labelled data, and use a precision recall curve to decide on your threshold.  See the [performance and benchmarking](./performance.md) for more details on how to do this.