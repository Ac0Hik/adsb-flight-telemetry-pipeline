# Spark Pipeline — Notes & Known Limitations

This document captures design decisions, known limitations, and future improvements for the Spark jobs and notebooks in this pipeline. It is intended for engineers who want to understand the reasoning behind implementation choices.

---

## Manual Steps (Not Yet Automated)

**geo_utils upload**
`spark/utils/geo_utils.py` must be manually uploaded to Databricks Volumes before running the silver notebook:
```bash
databricks fs mkdirs "dbfs:/Volumes/workspace/default/adsb_data/utils"
databricks fs cp "spark/utils/geo_utils.py" "dbfs:/Volumes/workspace/default/adsb_data/utils/geo_utils.py"
```
This will be automated as part of the Airflow pipeline in a future iteration. The end goal is a single command that handles all uploads, notebook runs, and validation.

**Bronze upload**
After the local streaming job runs, the Delta table must be uploaded to Volumes:
```bash
databricks fs cp -r "data/delta/bronze/live_states" "dbfs:/Volumes/workspace/default/adsb_data/bronze/live_states" --overwrite
```
This will also be automated via Airflow.

---

## Segmentation Logic Limitations

**Gap threshold too aggressive**
The current gap threshold is 600 seconds (10 minutes). For commercial aviation this is too short — long-haul flights over oceans can have ADS-B coverage gaps of several hours without the aircraft landing. This causes legitimate long flights to be split into multiple segments.
- Current: 600 seconds
- Recommended for production: 3+ hours (10,800 seconds)

**Ground stay segments not filtered**
When an aircraft is on the ground (`on_ground = True`), it still generates state vectors and gets assigned its own segment. These ground dwell segments appear in silver as noise — they are not actual flights.
- Fix: filter out segments where the majority of states have `on_ground = True` after aggregation.

**No explicit landing detection**
Flights currently end implicitly when tracking stops or a gap is detected. There is no explicit detection of the `on_ground = True` transition at landing. This means arrival coordinates may not accurately reflect the actual landing position.

---

## Data Quality Notes

**Limited real data during development**
Only a small amount of real ADS-B data was collected during development (a few batches of ~11,000 records each). This data did not contain enough variety to test all segmentation conditions (takeoffs, landings, long gaps). Mock data was created to validate the downstream logic — see the silver notebook for the mock data cells.

**Mock data scope**
The mock data covers:
- Aircraft starting on ground then taking off (`abc123`)
- Signal gap > 600 seconds causing a new segment (`abc123`)
- Aircraft always airborne with no state transitions (`xyz999`)
- Long flight with 10+ states and 5+ minutes duration to pass the filter (`flx001`)

---

## Future Improvements

- **Automate all uploads** — integrate bronze upload and geo_utils upload into the Airflow DAG so the pipeline runs end to end with a single trigger
- **Increase gap threshold** — raise to 3 hours for production use
- **Filter ground segments** — exclude segments where `on_ground = True` for the majority of states
- **Landing detection** — explicitly detect `on_ground` transition at the end of a flight to improve destination accuracy
- **Scale geo operations** — Apache Sedona for spatial operations for more airport support could be a good idea
- **Notebook → job promotion** — convert tested notebooks to scheduled Databricks jobs triggered automatically by Airflow without manual intervention (this will be solved in future iterations)