# CMS Hospitals Data Loader

A daily Python job that downloads every **Hospitals** dataset from the CMS Provider Data catalog, standardizes the column names to `snake_case`, and only re-downloads files that have changed since the last run.


## Important Features of Script



**Automatic discovery**  Reads the CMS metastore and finds every dataset tagged "Hospitals". No hard-coded file lists. 
**Clean column names**  Converts headers like `Patients’ rating of the facility linear mean score` to `patients_rating_of_the_facility_linear_mean_score`. 
**Parallel processing**  Downloads and processes multiple files at the same time. 
**Incremental loads**  Remembers each dataset's last-modified date and skips anything unchanged. Re-running the same day downloads nothing. 
**Automatic retry**  If a file fails, it is retried on the next run; other files are not affected. 
**Run tracking**  Every run and every file is logged in a DuckDB database: what was loaded, row counts, timing, and errors. 
**Data preserved exactly**  Only the header row changes. Values such as facility IDs and ZIP codes keep their leading zeros. 
**Safe for daily scheduling**  Files are replaced only when fully written, and overlapping runs are blocked. 
**Portable**  Plain Python on Windows or Linux. No cloud services required. 

## How to Run

```bash
pip install -r requirements.txt
python cms_hospitals_job.py
```

Optional flags: 

 Flag  Purpose 
------
 `--output-dir <path>`  Where files are written (default `cms_output`) 
 `--workers <n>`  Number of parallel downloads (default 8) 
 `--dry-run`  Show what would be downloaded without downloading 
 `--force`  Re-download everything 
