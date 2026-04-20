"""
Stream CMS Monthly Enrollment by Plan ZIPs straight into Neon Postgres.
No files are written to disk — ZIPs are pulled into memory, unzipped in memory,
and each CSV is streamed into Postgres via COPY FROM STDIN.

Env:
  NEON_STRING   Full Postgres connection string (postgres://user:pass@host/db?sslmode=require)
    START_YEAR    (optional) default 2008
      END_YEAR      (optional) default current year
        ONLY_MONTH    (optional) e.g. "2026-04" to load one file only
        """
import csv
import io
import os
import sys
import zipfile
from datetime import date
from urllib.request import Request, urlopen

import psycopg2
from psycopg2 import sql

MONTHS = ["january","february","march","april","may","june",
                    "july","august","september","october","november","december"]

BASE = "https://www.cms.gov/files/zip/monthly-enrollment-plan-{month}-{year}.zip"
UA   = {"User-Agent": "cmsdata-ingest/1.0 (+github.com/copperlang2007/cmsdata)"}

DDL = """
CREATE SCHEMA IF NOT EXISTS cms;

CREATE TABLE IF NOT EXISTS cms.load_log (
    report_period date PRIMARY KEY,
        source_url    text NOT NULL,
            rows_loaded   bigint NOT NULL,
                loaded_at     timestamptz NOT NULL DEFAULT now()
                );

                CREATE TABLE IF NOT EXISTS cms.monthly_enrollment_by_plan (
                    report_period      date        NOT NULL,
                        contract_number    text,
                            plan_id            text,
                                ssa_state_county_code text,
                                    fips_state_county_code text,
                                        state              text,
                                            county             text,
                                                enrollment         text,
                                                    PRIMARY KEY (report_period, contract_number, plan_id, fips_state_county_code)
                                                    );

                                                    CREATE INDEX IF NOT EXISTS idx_mebp_period ON cms.monthly_enrollment_by_plan(report_period);
                                                    """

def period_iter(start_year: int, end_year: int):
      today = date.today()
      for y in range(start_year, end_year + 1):
                for m_idx, m_name in enumerate(MONTHS, start=1):
                              if date(y, m_idx, 1) > today:
                                                return
                                            yield y, m_idx, m_name

        def fetch_zip(url: str) -> bytes | None:
              req = Request(url, headers=UA)
              try:
                        with urlopen(req, timeout=120) as r:
                                      if r.status != 200:
                                                        return None
                                                    return r.read()
              except Exception as e:
                        print(f"  fetch fail: {e}", file=sys.stderr)
                        return None

          def pick_csv(zf: zipfile.ZipFile) -> str | None:
                # Prefer a CSV at the top level; fall back to any .csv
                names = zf.namelist()
                csvs = [n for n in names if n.lower().endswith(".csv")]
                if not csvs:
                          return None
                      csvs.sort(key=lambda n: (n.count("/"), len(n)))
    return csvs[0]

def normalize_rows(reader):
      """
          CMS monthly-enrollment-by-plan CSVs use '*' to mark suppressed small counts
              and sometimes have a header with varying column names. We emit 8 columns
                  in a fixed order; missing columns become empty strings.
                      """
    header = next(reader, None)
    if header is None:
              return
    # lower-case header lookup
    h = [c.strip().lower() for c in header]
    def col(row, *names):
              for n in names:
                            if n in h:
                                              i = h.index(n)
                                              if i < len(row):
                                                                    return row[i].strip()
                                                        return ""
                                  for row in reader:
                                            if not row or all((not c.strip()) for c in row):
                                                          continue
                                                      yield [
                                                                    col(row, "contract number"),
                                                                    col(row, "plan id"),
                                                                    col(row, "ssa state county code", "ssa state/county code"),
                                                                    col(row, "fips state county code", "fips state/county code"),
                                                                    col(row, "state"),
                                                                    col(row, "county"),
                                                                    col(row, "enrollment", "plan enrollment"),
                                                      ]

def load_month(conn, y: int, m: int, m_name: str) -> bool:
      url = BASE.format(month=m_name, year=y)
    period = date(y, m, 1)

    with conn.cursor() as cur:
              cur.execute("SELECT 1 FROM cms.load_log WHERE report_period = %s", (period,))
        if cur.fetchone():
                      print(f"  skip {period} (already loaded)")
            return False

    print(f"  GET {url}")
    data = fetch_zip(url)
    if data is None:
              print(f"  no file for {period}")
        return False

    try:
              zf = zipfile.ZipFile(io.BytesIO(data))
except zipfile.BadZipFile:
        print(f"  bad zip for {period}")
        return False

    name = pick_csv(zf)
    if not name:
              print(f"  no csv inside zip for {period}")
        return False

    # Read CSV from the zip as text
    with zf.open(name) as fp:
              text = io.TextIOWrapper(fp, encoding="latin-1", newline="")
        reader = csv.reader(text)

        # Build an in-memory TSV buffer prefixed with report_period
        buf = io.StringIO()
        writer = csv.writer(buf, delimiter="\t", lineterminator="\n",
                                                        quoting=csv.QUOTE_MINIMAL)
        rows = 0
        for norm in normalize_rows(reader):
                      writer.writerow([period.isoformat(), *norm])
            rows += 1

    buf.seek(0)

    with conn.cursor() as cur:
              cur.execute("""
                          CREATE TEMP TABLE _stage (
                                          report_period date,
                                                          contract_number text,
                                                                          plan_id text,
                                                                                          ssa_state_county_code text,
                                                                                                          fips_state_county_code text,
                                                                                                                          state text,
                                                                                                                                          county text,
                                                                                                                                                          enrollment text
                                                                                                                                                                      ) ON COMMIT DROP
                                                                                                                                                                              """)
        cur.copy_expert(
                      "COPY _stage FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')",
                      buf,
        )
        cur.execute("""
                    INSERT INTO cms.monthly_enrollment_by_plan
                                    (report_period, contract_number, plan_id,
                                                     ssa_state_county_code, fips_state_county_code,
                                                                      state, county, enrollment)
                                                                                  SELECT report_period, contract_number, plan_id,
                                                                                                     ssa_state_county_code, fips_state_county_code,
                                                                                                                        state, county, enrollment
                                                                                                                                    FROM _stage
                                                                                                                                                ON CONFLICT DO NOTHING
                                                                                                                                                        """)
        cur.execute(
                      "INSERT INTO cms.load_log(report_period, source_url, rows_loaded) "
                      "VALUES (%s, %s, %s) ON CONFLICT (report_period) DO NOTHING",
                      (period, url, rows),
        )
    conn.commit()
    print(f"  loaded {period}: {rows} rows")
    return True

def main():
      dsn = os.environ.get("NEON_STRING")
    if not dsn:
              print("NEON_STRING not set", file=sys.stderr)
        sys.exit(1)

    start_year = int(os.environ.get("START_YEAR", "2008"))
    end_year   = int(os.environ.get("END_YEAR", str(date.today().year)))
    only_month = os.environ.get("ONLY_MONTH")  # e.g. 2026-04

    conn = psycopg2.connect(dsn)
    try:
              with conn.cursor() as cur:
                            cur.execute(DDL)
        conn.commit()

        if only_month:
                      y, m = map(int, only_month.split("-"))
            load_month(conn, y, m, MONTHS[m - 1])
else:
            for y, m, m_name in period_iter(start_year, end_year):
                              load_month(conn, y, m, m_name)
finally:
        conn.close()

if __name__ == "__main__":
      main()
