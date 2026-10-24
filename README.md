# DSE 511 — Web Data Acquisition: NGSIM US-101 Vehicle Trajectories

**Option A — documented API.** A small, cached, reproducible pipeline that downloads 23,793 one-second trajectory points for **396 vehicles** on southbound US-101 (Los Angeles, 2005-06-15 morning peak) from the U.S. DOT open-data portal. It preserves the raw API responses, cleans them into a tidy one-row-per-vehicle table, validates the table, and compares speed and headway across the morning and across vehicle classes.

## Files

```text
dse511-web-data-acquisition/
├── README.md                 # this file: plan, setup, run order, provenance, limitations
├── ETHICS_REFLECTION.md      # ethics reflection (also displayed in notebook section 5)
├── acquisition.ipynb         # run this: config → acquisition → cleaning → validation → analysis → ethics
├── ngsim_pipeline.py         # all functions used by the notebook
├── config.json               # every query and cleaning parameter
├── requirements.txt
├── tests/test_ngsim_pipeline.py   # unit tests for parsing/cleaning functions (no network)
├── data/
│   ├── raw/run_20261024T194613Z/  # raw data exactly as received + provenance
│   │   ├── page_000.json … page_002.json   # API responses, unchanged response text
│   │   ├── dataset_metadata.json           # source metadata (license, column definitions)
│   │   ├── row_count.json                  # response of the count query
│   │   ├── requests_log.jsonl              # every request: time, URL, parameters, HTTP status
│   │   └── manifest.json                   # provenance summary written by the pipeline
│   └── clean/
│       ├── ngsim_us101_vehicles.csv        # MAIN clean table: 396 rows, one per vehicle trajectory
│       ├── ngsim_us101_frames_1hz.csv      # clean 1 Hz frame-level table (23,793 rows)
│       ├── data_dictionary.csv             # column, type, unit, description
│       ├── cleaning_log.csv                # what each cleaning step changed
│       └── validation_report.csv           # named checks and results
├── figures/                  # speed_by_period.png, headway_by_class.png
└── logs/pipeline.log         # run log, created when you run the pipeline (not submitted: contains local paths)
```

## Data-access plan

| Item | Plan / decision |
|---|---|
| Source | *Next Generation Simulation (NGSIM) Vehicle Trajectories and Supporting Data*, U.S. DOT ITS Joint Program Office / FHWA, on data.transportation.gov |
| Method | Socrata SODA API (documented JSON API). No scraping, no bulk "Export" (≈ 11.8 million rows) |
| Observation | One **vehicle trajectory**: one vehicle's pass through the US-101 study section |
| Fields | vehicle ID, frame ID, total frames, time, vehicle class, length, lane, speed, acceleration, leader ID, space headway, time headway (12 of 25 columns) |
| Expected requests | 1 metadata + 1 row count + 3 pages of 10,000 rows = **5 requests**, ≥ 1 s apart |
| Concerns | Table size (filter server-side); the public video could let someone match trajectories to vehicles; 2005 data; CC BY-SA 3.0 requires attribution and share-alike |
| Why appropriate | Public, no login, explicit open license, documented API, and directly relevant to car-following and mixed-traffic research |

### Responsible-access assessment

- **Terms and license:** the dataset metadata lists *Creative Commons Attribution–ShareAlike 3.0 Unported* and `Rights: public`. FHWA provides a recommended citation (see below). The derived data in `data/clean/` are shared under the same license.
- **Authentication:** none required. The data are publicly accessible without login.
- **Rate limits:** I did not find a numeric limit for anonymous requests. Socrata supports an optional app token for identified traffic. The pipeline therefore sends very few requests, waits 1 s between them, never retries automatically, and stops on HTTP 401/403/429.
- **Data minimization:** the query is filtered on the server to one freeway (`us-101`), the first 150 vehicle IDs, and one frame per second (`frame_id % 10 = 0`). It requests 12 columns, leaving out global coordinates and lateral position. Result: 23,793 rows, about 0.2 % of the table.
- **Sensitive data:** no names, plates, or personal attributes. The re-identification risk created by the linked video is discussed in `ETHICS_REFLECTION.md`.

## Setup

Tested with Python 3.12.3 on Windows.

```bash
python -m pip install -r requirements.txt
```

### Credentials

**No API key is required.** Optionally, you can create a free Socrata app token (sign in on data.transportation.gov and use the portal's *Developer* settings) and provide it as an environment variable. The pipeline sends it as the `X-App-Token` header; it is never written in code, config, or output files.

```powershell
$env:SOCRATA_APP_TOKEN = "your-token"     # PowerShell
```

```bash
export SOCRATA_APP_TOKEN="your-token"     # macOS / Linux
```

## How to run

1. Open `acquisition.ipynb` and choose **Kernel → Restart & Run All**. Alternatively, run it headless from this folder:

   ```bash
   python -m nbconvert --to notebook --execute --inplace acquisition.ipynb
   ```

2. Execution order inside the notebook is fixed: **0 Config → 1 Acquisition → 2 Cleaning → 3 Validation → 4 Analysis → 5 Ethics**. Only section 1 uses the network.
3. **Caching:** if a complete raw run already exists in `data/raw/`, section 1 reuses it and sends no requests. To collect fresh data, set `FORCE_REFRESH = True` in section 0. This writes a **new** `run_<UTC time>` folder and never overwrites earlier raw data. An interrupted run is resumed automatically: pages already on disk are skipped.
4. Optional tests (no network): `python -m pytest -q`
5. Query and cleaning parameters (location, vehicle-ID limit, sampling step, page size, delay, plausibility limits) are changed in `config.json`, not in the code.

**Expected runtime:** first collection about 10 s (5 requests). The whole notebook takes about 15 s with cached data.

## Data provenance

| Item | Value |
|---|---|
| Source name | Next Generation Simulation (NGSIM) Vehicle Trajectories and Supporting Data |
| Publisher | U.S. DOT Federal Highway Administration; ITS Joint Program Office (ITS DataHub) |
| Source URL | https://data.transportation.gov/d/8ect-6jqj (DOI: http://doi.org/10.21949/1504477) |
| API endpoint | `https://data.transportation.gov/resource/8ect-6jqj.json` (data); `https://data.transportation.gov/api/views/8ect-6jqj.json` (metadata) |
| Query parameters | `$select` = the 12 columns in `config.json`; `$where` = `location='us-101' AND vehicle_id <= 150 AND frame_id % 10 = 0`; `$order` = `:id`; `$limit` = 10000; `$offset` = 0, 10000, 20000 |
| Collection time | 2026-10-24, 19:46:13–19:46:20 UTC (15:46 EDT) |
| Data version | Data rows last updated 2018-08-27 (`rowsUpdatedAt`); the dataset page shows "Last Updated October 6, 2026" (metadata); original collection 2005-06-15 |
| Observations | 23,793 frame rows → **396 vehicle trajectories** (150 vehicle IDs reused across 3 collection periods) |
| Requests | 5 for the final collection (see `requests_log.jsonl`). During planning I also sent six small read-only queries (row counts, a 2-row sample, metadata, a headway check); two further attempts through a web-fetch tool timed out |
| License | CC BY-SA 3.0 Unported — http://creativecommons.org/licenses/by-sa/3.0/legalcode |
| Contact | ITS JPO, data.itsjpo@dot.gov |

**Recommended citation:** U.S. Department of Transportation Federal Highway Administration. (2016). *Next Generation Simulation (NGSIM) Vehicle Trajectories and Supporting Data* [Dataset]. Provided by ITS DataHub through Data.transportation.gov. Accessed 2026-10-24 from http://doi.org/10.21949/1504477

**Reproducibility:** web data can change. The rows have not changed since 2018, but the portal still edits the metadata, and the dataset could be modified, moved, or withdrawn. A rerun with `FORCE_REFRESH = True` may therefore not return identical records. The raw pages from this collection are preserved so that the cleaning and analysis can always be reproduced exactly.

## Cleaning and validation (summary)

Details are in notebook section 2 and in `data/clean/cleaning_log.csv`.

1. **Numeric conversion.** All API values arrive as strings. Values that fail to parse are counted (0).
2. **Duplicate removal** on the true key `(vehicle_id, global_time)`. `(vehicle_id, frame_id)` is not a key because frame numbers restart in each period; it would show 9,505 false duplicates.
3. **Date parsing.** `global_time` (ms since 1970, UTC) is converted to UTC and America/Los_Angeles datetimes.
4. **Splitting reused IDs into trajectories** at gaps > 5 s. The source states that repeated vehicle IDs are not associated: 150 IDs correspond to 396 vehicles.
5. **Unit conversion** from ft, ft/s, ft/s² to m, m/s, m/s².
6. **Standardized category labels** for vehicle class and lane type.
7. **Coded missing values.** Headway 0 with no leader (1,991 rows), 9999.99 for stopped vehicles (106 rows, all with speed 0), and 2 zero headways with a leader are set to NaN.
8. **Plausible-range flags.** No speed or acceleration is out of range. 27 time headways above 30 s are flagged but kept.
9. **Aggregation** to one row per trajectory, followed by **21 validation checks**: sample size, key uniqueness, one class, one length and one source trajectory per vehicle, completeness against the source `total_frames` (0.97–1.02), the collection date, and a dtype schema. All checks pass, and the notebook stops if any check fails.

The column definitions are in `data/clean/data_dictionary.csv`.

## Results in one paragraph

Mean vehicle speed falls from 14.3 m/s (07:49–07:51) to 11.4 m/s (08:04–08:06) and 8.7 m/s (08:19–08:22) as congestion builds. Median spacing shrinks from 24 m to 18 m, and median time headway rises from 1.7 s to 2.2 s. Trucks show a longer median time headway (3.0 s) than autos (2.0 s), and motorcycles a shorter one (0.9 s), but these class samples are very small (7 trucks and 5 motorcycles). Figures are in `figures/`.

## Limitations and known problems

- **Not a representative time sample.** Vehicle IDs restart in each collection period, so `vehicle_id <= 150` returns the earliest 2–3 minutes of each period, not the full 15 minutes.
- **Small class samples.** There are 7 trucks and 5 motorcycles; class comparisons are only suggestive.
- **Narrow scope.** One site, one morning, human drivers in 2005. The results cannot be generalized to other freeways or to today's mixed traffic.
- **Processed measurements.** Acceleration is clipped at ±11.2 ft/s² (±3.41 m/s²) in the source. One-second sampling hides short manoeuvres.
- **Incomplete documentation.** The source description of `time_headway` is cut off ("A headway value of 99…"). The meaning of 9999.99 (stopped vehicle) is inferred from the data: all 106 such rows have speed 0.
- **Collection periods are derived from the data.** The pipeline groups trajectories by gaps of 5 minutes or more between start times. It does not use official period boundaries.

If the source becomes unavailable or starts refusing requests, the pipeline stops without retrying. The raw data already collected remain usable, and a change of source would be documented here.

## Ethics reflection

See [`ETHICS_REFLECTION.md`](ETHICS_REFLECTION.md); it is also shown in notebook section 5.
