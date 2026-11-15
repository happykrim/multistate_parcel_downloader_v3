# Multi-State Parcel Downloader v3.1

**States:** Wyoming (WY), Oklahoma (OK), Colorado (CO), New Mexico (NM). This is an updated version of our v3.0 script, with county-specific assessor enrichment and a generalized data-quality fallback. It does **not** claim that every county in Oklahoma or New Mexico has a configured public source.

**Important:** The supplied 24 automated tests are **offline tests** (including synthetic assessor archives and mocked ArcGIS results). Live bulk enrichment against the official county sources has **not** been verified from the development environment. Every real run writes matching statistics and recovery warnings so its success can be measured.

## Install on Windows / VS Code

Open a PowerShell terminal in the extracted folder:

```powershell
python -m pip install -r requirements.txt
```

For local GIS shapefiles, GeoPackages or GDB files:

```powershell
python -m pip install -r requirements_local_gis.txt
```

### Recommended initial test — random 500-parcel samples

```powershell
python multistate_parcel_downloader_v3.py --state CO --counties "El Paso" Weld --max-parcels 500 --sample-mode random --seed 42 --workers 1 --assessor-enrich auto
python multistate_parcel_downloader_v3.py --state NM --counties Bernalillo --max-parcels 500 --sample-mode random --seed 42 --workers 1 --assessor-enrich auto
python multistate_parcel_downloader_v3.py --state OK --counties Tulsa Canadian --max-parcels 500 --sample-mode random --seed 42 --workers 1
python multistate_parcel_downloader_v3.py --state WY --counties Natrona Laramie --max-parcels 500 --sample-mode random --seed 42 --workers 1
```

`--sample-mode random` gives a reproducible selection rather than relying on the first sorted ArcGIS object IDs. **This is still only a preview, not proof of countywide completeness.** Use a full run after inspecting the quality reports. To disable supplemental data fetching, use `--assessor-enrich off`.

### Full county and acreage filtering

```powershell
python multistate_parcel_downloader_v3.py --state CO --counties "El Paso" --min-acres 50 --workers 1 --assessor-enrich auto
```

The raw and all-normalized files retain **all** downloaded rows; only `_production.csv` is filtered by `--min-acres`.

## Improvements since v3.0

- **El Paso County, Colorado:** Automatically discovers the latest `EPC-Parcel-Data-YYYYMMDD.zip` link on the official assessor data page and downloads it once into a county cache. Streams county data to a disk-backed SQLite index, joins `PARCEL` to your existing `parcel_id`, and fills missing official values (`OWNER1`, `MAILADR`, `MAILCITY`, `MAILSTATE`, `MAILZIPCODE`, `LOCATION`, `LOCATIONZIP`, `Acreage`, `MARKETVALUE`, `ASSESSEDVALUE`, `PARTIALLEGAL`). GIS-estimated acreage is replaced with assessor acreage while the geometry estimate remains separately available for audit.
- **Weld County, Colorado:** Supports official assessor `OWNERSHIPGIS` and `ACCTCURRENTINVNTRY` CSVs contained in the county all-data ZIP. `OWNERSHIPGIS` is joined by `PARCELNB` (account disambiguation if needed). `ACCTCURRENTINVNTRY` uses `ACCOUNTNO` to supplement property ZIP. Some website downloads block automated connections; use `--assessor-file` with the county ZIP if auto download fails.
- **Bernalillo County, New Mexico:** Parses property city/ZIP from `SITUSADD2` and mailing city/state/ZIP from `OWNADD2`, never copies mailing ZIP into situs ZIP. Handles rare site lines without a state abbreviation by using the **known situs county state** (not applicable to mailing lines). Optionally enriches only requested UPCs from the official City of Albuquerque-hosted county-assessor MapServer layer. Preserve `TOTVALUE` as `total_value` and `NETTAXABLE` as `taxable_value`; neither is automatically a market value.
- **Canadian County, Oklahoma:** Treats `UNINCORPORATED` as a location category, **not** a city. Keeps it in `property_city_raw` for traceability. No invented postal city is substituted.
- **Wyoming (Natrona / Laramie):** Counts only **positive** market/assessor actual values as useful coverage. Invalid/zero actual values are flagged. Situs ZIP/city are not inferred from owner mailing details.
- **Every county:** One missing-data queue, pre/post enrichment coverage, field-level provenance, strict duplicate-match handling, non-authoritative-ID flags, geometry acreage flags and low-coverage recommendations. Newly configured counties get the same QA behavior automatically.
- **Resume:** The ArcGIS downloader can reuse completed raw batches from v3.0, while **renormalizing them with the v3.1 mapping**; previously cached normalized mapping errors are not reused. If source IDs or schema changed, existing validation may require restarting with `--force`.

## Official county sources

- Colorado / El Paso: https://assessor.elpasoco.com/assessordata/ and https://assessor.elpasoco.com/wp-content/uploads/Data/EPC-Parcel-Layout.pdf
- Colorado / Weld: https://www.weld.gov/Government/Departments/Assessor/Data-Download
- New Mexico / Bernalillo assessor GIS: https://coageo.cabq.gov/cabqgeo/rest/services/agis/AddressReport/MapServer/4
- Oklahoma / Canadian GIS catalog: https://canadiancounty.org/868/Mapping-and-GIS-Data

Other county-level recovery sources must be configured/verified case by case. Missing owner names are **not** invented, and rural parcels may legitimately lack a street address.

## When El Paso or Weld's automatic ZIP download fails

Download the official county ZIP manually from the assessor page, then:

```powershell
python multistate_parcel_downloader_v3.py --state CO --counties "El Paso" --assessor-file "CO:El Paso=C:\Data\EPC-Parcel-Data-20261110.zip" --assessor-enrich require --max-parcels 500 --sample-mode random
```

```powershell
python multistate_parcel_downloader_v3.py --state CO --counties Weld --assessor-file "CO:Weld=C:\Data\All-Data-CSV.zip" --assessor-enrich auto --max-parcels 500 --sample-mode random
```

The `--assessor-file` path works with CSV, TXT, DBF, or ZIP depending on the county adapter. The **El Paso** adapter expects the official parcel dataset (containing `PARCEL`), and the **Weld** adapter looks for `OWNERSHIPGIS` and `ACCTCURRENTINVNTRY` files inside the ZIP. The second Weld table may not always be available; in `auto` mode this is reported as a warning and the working first stage is retained. `require` mode stops if a required stage is unavailable rather than silently declaring complete enrichment.

ZIPs may be larger than a preview needs. Allow enough disk space and start with `--workers 1`.

## How to add another county (or another assessor source)

The primary parcel GIS registry remains customizable with `--sources-json` as in v3.0. The new **independent supplemental registry** is `--assessor-rules-json` and supports:

1. A local official CSV/TXT/ZIP/DBF joined by a strictly specified parcel ID and optional account ID; or
2. A public ArcGIS layer with a named parcel key queried only for the downloaded parcel IDs.

See `assessor_rules.example.json`. That file is a **template with fictional paths**, not a verified live source. Example using local assessor CSV for an extra county:

```powershell
python multistate_parcel_downloader_v3.py --state NM --counties "San Juan" --local-file "C:\Data\san_juan_parcels.csv" --assessor-rules-json assessor_rules.json --assessor-enrich require
```

**Join safety:** parcel IDs are treated as text to retain leading zeros. The state and county are implicit in each configured source. Account IDs resolve multiple same-parcel candidates where possible; records with conflicting supplemental owner/value/address attributes are flagged **ambiguous** and left unchanged. Non-missing original fields are never silently replaced. When the original acreage was geometry-derived, official assessor acreage may replace it. A large discrepancy is flagged for review.

## Output files per county

In `multi_state_parcels_output_v3/<STATE>/<county>/.../output/`:

- `<county>_raw_all_fields.csv` — original extracted parcel fields, unmodified
- `<county>_normalized_pre_repair.csv` — normalized from original data, *before* assessor enrichment
- `<county>_all_normalized.csv` — corrected + assessor-enriched (if available) + owner-name repair
- `<county>_production.csv` — same normalized rows, optionally filtered by acreage
- `<county>_missing_data_queue.csv` — each parcel needing recovery, with priority and suggested method
- `<county>_missing_arcgis_ids.csv` — any ID that ArcGIS failed to return
- `<county>_report.json` — source metadata, match/ambiguity counts, recovered fields, before/after coverage, QA recommendations and errors
- `<county>_owner_name_audit.csv` — created if a compressed-name repair is triggered

Important fields: `full_name_source`, `property_zip_source`, `mailing_zip_source`, `acres_source`, `market_value_source`, `enrichment_fields_filled`, `enrichment_review_flags`. The `property_city_raw` column retains placeholders such as `UNINCORPORATED` even when the clean `city` is blank. `legal_address` in your existing schema continues to mean **physical/situs street address**, not a deed legal description (`legal_description`).

**Coverage definition:** `market_value` coverage requires a value greater than zero; `acres` requires positive acreage; property city excludes placeholders and ZIP must be a valid 5-digit or ZIP+4 value. A source can contain 100% geometry acreage while having 0% official acreage.

## Recheck the original 7-county sample ZIP without new downloads

```powershell
python recheck_preview_zip_v31.py "multi_state_parcels_output_v3.zip" --output "preview_v31_reaudit"
```

The script uses the original raw CSVs and saved geometry-acre estimates to isolate **mapping/quality changes**. It does **not** falsely claim successful county-assessor enrichment. Compare `preview_v30_vs_v31_quality.csv`, then review the generated corrected sample CSVs and missing-data queues.

## Tests

```powershell
python -m unittest test_downloader_v3.py test_enrichment_v31.py -v
```

**24 offline tests passed** on the development environment. These include source normalization for all four states, parcel ID preservation, geometry acreage, ArcGIS truncation handling and resume, ZIP-based assessor joins, ambiguous join rejection, different mailing vs situs ZIP, a local El Paso full CLI test, and custom future-county assessor mapping. **Live API download throughput, county file layouts, source availability, completeness and join rates still need real-world validation** with your next test files.

### If a county still has missing fields

Upload its `<county>_report.json`, `<county>_raw_all_fields.csv`, `<county>_missing_data_queue.csv` and optionally the full download ZIP. With these we can determine whether the issue is a missing field alias, different county assessor source, API limitations, inaccessible postal address, or a defective join.
