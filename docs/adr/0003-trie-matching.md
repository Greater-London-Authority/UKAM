# 3. trie-matching

Date: 2025-11-25

## Status

Rejected

## Context

### Summary

The original ambition of trie based matching was that it would provide a very high precision way of matching addresses, almost as good as exact matching.

The main benefit to the user would be these matches could be categorised as highly trustworthy - significantly more so than fuzzy (Splink-based) matching.

Unfortunately, we have found that it is not possible to define a trie-based algorithm which both:

- is significantly more accurate than Splink-based matching; and
- matches many addresses.

In a nutshell, you can either:

- allow a considerable amount of fault tolerance, enabling many addresses to be matched using the trie, but reducing accuracy below Splink-based matching
- be very strict about fault tolerance, leading to high precision, but meaning very few addresses are matched over and above exact sting matching,

As a result, trie based matching cannot filfil its original purpose, and therefore we have decided not to pursue it further.

Finally, to note, we did find trie based matching was considerably faster than Splink-based fuzzy matching, but Splink based matching is already extremely fast, so there is little perceived benefit to the end user.

### Methodology

The following results are based on:

- `duckdb==1.4.2`
- `splink_udfs` version `0.0.11` as released at commit `cf00056` of the `splink_udfs` repo.

### Results

#### Lambeth Council Tax dataset

In summary, trie based matching only "works" for a small percentage (3.46%) of rows, and, amongst these rows, isn't very accurate (95% accuracy).

##### Post hardening

**Overall match distribution**

| match_reason                                  | match_count | match_percentage |
| --------------------------------------------- | ----------: | ---------------: |
| exact: full match                             |      89,149 |           69.36% |
| unmatched                                     |      34,922 |           27.17% |
| trie: exact match with skips and fuzziness    |       4,451 |            3.46% |

**Trie match correctness**

| trie_match_correct |   cnt | match_percentage |
| ------------------ | ----: | ---------------: |
| true               | 4,232 |           95.08% |
| false              |   219 |            4.92% |

The accuracy here is significantly worse than Splink-based fuzzy matching accuracy.

An example of an incorrect match (to illustrate the difficulty in changing the rules of fault tolerance):

- Messy address: `WOOD HOUSE 3 GASKELL STREET LONDON`
  matches to: `3 GASKELL STREET LONDON`

The problem is that `3 GASKELL STREET LONDON` exists in the trie, has no children and is a terminal node.

The true address is: `FLAT 3 WOOD HOUSE GASKELL STREET LONDON`.

##### Pre-hardening

Pre hardening we got more matches, but they were even less accurate. But even then, only 5.28% of addresses were matched using the trie.

**Overall match distribution**

| match_reason                                  | match_count | match_percentage |
| --------------------------------------------- | ----------: | ---------------: |
| exact: full match                             |      89,149 |           69.36% |
| unmatched                                     |      32,591 |           25.36% |
| trie: exact match with skips and fuzziness    |       6,782 |            5.28% |

**Trie match correctness**

| trie_match_correct |   cnt | match_percentage |
| ------------------ | ----: | ---------------: |
| true               | 6,272 |           92.48% |
| false              |   510 |            7.52% |


#### Hackney Council Tax dataset

A "naive" run finds that 27% of all addresses are matched using a trie, but further inspection shows that this is simply due to the inclusion/ommission of the token `HACKNEY` (some addresses say `HACKNEY LONDON`, others just `LONDON`).

The following results are after removing the token `HACKNEY`.

##### Post hardening

We match 6% of addresses, but with an error rate of 4.59%. This compares to an error rate of 97.67% when just using exact matching and Splink based fuzzy matching.

**Overall match distribution**

| match_reason                               | match_count | match_percentage |
| ------------------------------------------ | ----------: | ---------------: |
| unmatched                                  |      59,047 |           51.26% |
| exact: full match                          |      48,701 |           42.28% |
| trie: exact match with skips and fuzziness |       7,449 |            6.47% |

**Trie match correctness**

| trie_match_correct |   cnt | match_percentage |
| ------------------ | ----: | ---------------: |
| true               | 7,107 |           95.41% |
| false              |   342 |            4.59% |

An example error was:

- `FLAT BASEMENT 3 BLETCHLEY STREET LONDON`
  matching to: `HOUSE EXCLUDING BASEMENT 3 BLETCHLEY STREET LONDON`

instead of:

- `BASEMENT FLAT 3 BLETCHLEY STREET LONDON`.


#### Business dataset

##### Post hardening

Note the data quality here is poor. The trie performs terribly – getting it wrong 94.6% of the time.

**Overall match distribution**

| match_reason                               | match_count | match_percentage |
| ------------------------------------------ | ----------: | ---------------: |
| unmatched                                  |      44,415 |           88.83% |
| trie: exact match with skips and fuzziness |       5,383 |           10.77% |
| exact: full match                          |         202 |            0.40% |

**Trie match correctness**

| trie_match_correct |   cnt | match_percentage |
| ------------------ | ----: | ---------------: |
| true               |   292 |            5.42% |
| false              | 5,091 |           94.58% |


### Other things we learned

- Debugging complex UDFs like this is relatively hard, so there's an important benefit in maintainability and simplicity.

### Annex

The code for this analysis uses functions from
<https://github.com/moj-analytical-services/address_matching_performance>

and can be found in this gist:
<https://gist.github.com/RobinL/bfc9b5dbda3473238fc52dafb03aa10e>


## Decision

We will **not** pursue trie based address matching further as a core matching strategy.

Instead, we will continue to rely on existing Splink-based fuzzy matching and exact matching approaches, because:

- we could not define a trie-based algorithm that was both:
  - significantly more accurate than Splink-based matching; **and**
  - able to match a large proportion of addresses across multiple datasets.
- even with hardened rules:
  - only a small percentage of addresses are matched (e.g. 3.46% in Lambeth, 6.47% in Hackney, 10.77% in the business dataset); and
  - error rates remain materially higher than desired and worse than Splink on realistic data.
- although trie-based matching is faster, Splink-based matching is already extremely fast, so the performance benefit is not compelling for end users.

## Consequences

- **We avoid shipping a low-value "high-trust" signal.**
  Trie matches cannot reliably be treated as "highly trustworthy" relative to Splink-based fuzzy matches, so we avoid exposing a misleading trust category to users.

- **We reduce complexity and maintenance cost.**
  Complex trie-based UDFs are harder to implement, debug and maintain. Dropping this approach simplifies the codebase and ongoing support.

- **We continue to invest in Splink-based approaches.**
  Effort focused on improving Splink configurations, data quality, and evaluation will likely yield better returns than further experimentation with tries.

- **We accept a missed opportunity for marginal performance gains.**
  Tries are faster, but since Splink is already extremely fast in practice, we accept that we are not exploiting that extra performance in exchange for simpler, more reliable matching behaviour.

- **We retain the analysis as reference.**
  The experimental code and analysis (in the `address_matching_performance` repo and associated gist) remain as a reference for any future exploration, but not as a path to production.