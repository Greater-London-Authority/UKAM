# Ordnance Survey data

This guide walks through the end-to-end process of matching messy data to Ordnance Survey using `uk_address_matcher`

The steps are:

| Step | Indicative timing[^timings-m4] |
|------|-------------------|
| Create a data package in the Ordnance Survey Data Hub and obtain an API key | about 5 mins |
| Install tooling | about 5 mins |
| Build the canonical dataset | 5 mins for full UK |
| Pre-process for matching (national-scale only) | 5 mins |
| Match | Less than 1 minute |

[^timings-m4]: Timings on a MacBook M4 Max.


## What is Ordnance Survey data?

Ordnance Survey is the UK's authoritative provider of address data. Many public sector organisations can use this data for free under the [Public Sector Geospatial Agreement](https://www.ordnancesurvey.co.uk/customers/public-sector/public-sector-geospatial-agreement)
(PSGA).


## Why is there a special process for Ordnance Survey data?

`uk_address_matcher` makes no assumptions about the canonical dataset provided by the user.

However, in recognition that many users will be matching to Ordnance Survey data, we provide a streamlined process for downloading and preparing it for matching.  There are two main reasons:

1. We provide a tool to easily download and extract the data into the required format
2. With careful processing of Ordnance Survey files, it's possible to achieve higher accuracy than simply using the raw [NGD Builtaddress](https://docs.os.uk/osngd/data-structure/address/gb-address/built-address).  Our tool does this processing for you.


## Step 1: Create a data package and obtain an API key

Create a 'data package' in the [OS Data Hub](https://osdatahub.os.uk/) containing the Ordnance Survey data you want to match to.

Choose NGD or AddressBase. Default to NGD if you have no preference.

!!! tip "Optimising accuracy when linking to a local area"
    If you are matching to a local area, restrict your data package to only that area to make matching faster and more accurate.

1. Log in and create a
   [new recipe](https://osdatahub.os.uk/data/downloads/recipe-library) for
   your geographical area.  Follow the process and create a new data package.
2. Navigate to [data packages](https://osdatahub.os.uk/data/downloads/data-packages/)
   and and crete a new data package corresponding to the area of interest.  Use 'load polygon' to load a pre-defined polygons, which exist for geographic areas such as local authorities.
4. Obtain your `package_id` and `version_id`.

    The `package_id` is in the URL for the data package - for instance it is `18296` in  `https://osdatahub.os.uk/data/downloads/data-packages/18296`

    Within the data package, The version ID can be retrieved by hovering over the download link for your data package, which is in the format `https://osdatahub.os.uk/api/dataPackages/{package_id}/{version_id}/file`.

4. Obtain your API key and secret from the
   [API Projects](https://osdatahub.os.uk/data/apis/projects) page.


You should now have the following values:

- Data package `package_id`
- THe data package `version_id`
- Your API key and secret
    - `OS_PROJECT_API_KEY`
    - `OS_PROJECT_API_SECRET`


## Step 2: Create a folder for your project and install tooling

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then
create a project and add `uk_address_matcher`:

```bash
mkdir address_project && cd address_project
uv init --bare
uv add uk_address_matcher
```

## Step 3: Build the canonical dataset

Our [`ukam-os-builder`](https://github.com/moj-analytical-services/ukam_os_builder) tool
downloads and processes Ordnance Survey data into a Parquet file optimised for
matching.

You can use it as follows.  Make sure you have your `package_id`, `version_id`, and API key/secret to hand from step 1.

```bash
# Config wizard — point the tool to your data package
uvx --from ukam-os-builder ukam-os-setup
```

```bash
# Download and build the flat file
uvx --from ukam-os-builder ukam-os-build
```

By default the output lands in `data/output/`. Unless you set `num_chunks=1`,
the output is a folder of Parquet files representing a single table. DuckDB
reads them as one table via `con.read_parquet('data/output/*.parquet')`.

## Step 4: Pre-process for matching (national-scale only)

If you're linking to a small canonical dataset (of say, less than 500,000 rows), then it's simplest to process the data on-the-fly.

If you're linking to a large canonical dataset (for example, national-scale NGD), then we recommend a one-time pre-processing step. It computes reusable datasets (indices and feature tables) once, so subsequent matching runs are fast.

For a local council region, skip this step — everything runs on the fly.



```python
import duckdb
from uk_address_matcher import prepare_canonical_folder

con = duckdb.connect()
df_canonical = con.read_parquet("data/output/*.parquet")

prepare_canonical_folder(
    df_canonical,
    output_folder="./ukam_prepared_canonical",
    con=con,
    overwrite=True,
)
```

The prepared folder can be used for all subsequent matching runs.

## Step 5: Match

Create a script called `match.py` with the following content.

=== "Local / regional"

    ```python
    import duckdb
    from uk_address_matcher import AddressMatcher

    con = duckdb.connect()

    df_messy = con.read_parquet("messy_addresses.parquet")
    df_canonical = con.read_parquet("data/output/*.parquet")

    matcher = AddressMatcher(
        canonical_addresses=df_canonical,
        addresses_to_match=df_messy,
        con=con,
    )

    result = matcher.match()

    result.matches().show(max_width=10000)

    ```



=== "National-scale (prepared folder)"

    ```python
    import duckdb
    from uk_address_matcher import AddressMatcher

    con = duckdb.connect()
    df_messy = con.read_parquet("messy_addresses.parquet")

    matcher = AddressMatcher(
        canonical_addresses="./ukam_prepared_canonical",
        addresses_to_match=df_messy,
        con=con,
    )

    result = matcher.match()
    result.matches().show(max_width=10000)
    ```

Run with:

```bash
uv run match.py
```

## Video

The following video shows the end-to-end process of matching council tax data from a local council to the full Ordnance Survey dataset for that council.

<video controls preload="metadata" width="100%">
    <source src="../assets/videos/end_to_end_hackney.mp4" type="video/mp4">
    Your browser does not support the video tag.
</video>



