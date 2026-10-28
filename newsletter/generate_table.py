# Run this after pipeline.py to generate your newsletter table
# save as: newsletter/generate_table.py

import io
import os
import boto3
import pandas as pd


def get_latest_parquet(bucket, prefix, region):
    s3 = boto3.client("s3", region_name=region)
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    keys = []
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append((obj["LastModified"], obj["Key"]))
    keys.sort(reverse=True)
    obj = s3.get_object(Bucket=bucket, Key=keys[0][1])
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def format_currency(val):
    try:
        if val >= 1_000_000:
            return f"${val/1_000_000:.1f}M"
        if val >= 1_000:
            return f"${val/1_000:.0f}K"
        return f"${val:.0f}"
    except:
        return "N/A"


def generate_tables(bucket, prefix="seattle-housing/raw", region="us-west-2"):
    df = get_latest_parquet(bucket, prefix, region)

    # Clean and select display columns
    display_cols = [
        "permitnum",
        "originaladdress1",
        "originalzip",
        "permittypedesc",
        "estprojectcost",
        "statuscurrent",
        "contractorcompanyname",
    ]
    df = df[display_cols].copy()
    df["estprojectcost"] = df["estprojectcost"].apply(
        lambda x: format_currency(x) if pd.notna(x) else "N/A"
    )
    df["contractorcompanyname"] = df["contractorcompanyname"].fillna("—")
    df.columns = [
        "Permit #",
        "Address",
        "Zip",
        "Type",
        "Est. Value",
        "Status",
        "Contractor",
    ]

    # ── FREE TABLE (top 10 only) ───────────────────────────────────────────────
    free_html = """
<table style="width:100%;border-collapse:collapse;font-family:-apple-system,
sans-serif;font-size:13px;">
  <thead>
    <tr style="background:#1a1a1a;color:#fff;">
      <th style="padding:10px 8px;text-align:left;">#</th>
      <th style="padding:10px 8px;text-align:left;">Address</th>
      <th style="padding:10px 8px;text-align:left;">Zip</th>
      <th style="padding:10px 8px;text-align:left;">Type</th>
      <th style="padding:10px 8px;text-align:left;">Est. Value</th>
      <th style="padding:10px 8px;text-align:left;">Status</th>
    </tr>
  </thead>
  <tbody>"""

    for i, row in enumerate(df.head(10).itertuples(), 1):
        bg = "#fff" if i % 2 == 0 else "#f9f9f9"
        free_html += f"""
    <tr style="background:{bg};border-bottom:1px solid #eee;">
      <td style="padding:9px 8px;color:#999;">{i}</td>
      <td style="padding:9px 8px;font-weight:600;">{row.Address}</td>
      <td style="padding:9px 8px;">{row.Zip}</td>
      <td style="padding:9px 8px;">{row.Type if row.Type and row.Type != 'nan' else '—'}</td>
      <td style="padding:9px 8px;font-weight:700;color:#d7301f;">{row._5}</td>
      <td style="padding:9px 8px;">{row.Status}</td>
    </tr>"""

    free_html += """
  </tbody>
</table>
<p style="font-size:13px;color:#888;margin-top:8px;">
  Showing 10 of 50 listings.
  <strong>Upgrade to Pro to unlock all 50 →</strong>
</p>"""

    # ── PAID TABLE (all 50) ───────────────────────────────────────────────────
    paid_html = """
<table style="width:100%;border-collapse:collapse;font-family:-apple-system,
sans-serif;font-size:13px;">
  <thead>
    <tr style="background:#1a1a1a;color:#fff;">
      <th style="padding:10px 8px;text-align:left;">#</th>
      <th style="padding:10px 8px;text-align:left;">Address</th>
      <th style="padding:10px 8px;text-align:left;">Zip</th>
      <th style="padding:10px 8px;text-align:left;">Type</th>
      <th style="padding:10px 8px;text-align:left;">Est. Value</th>
      <th style="padding:10px 8px;text-align:left;">Status</th>
      <th style="padding:10px 8px;text-align:left;">Contractor</th>
    </tr>
  </thead>
  <tbody>"""

    for i, row in enumerate(df.itertuples(), 1):
        bg = "#fff" if i % 2 == 0 else "#f9f9f9"
        paid_html += f"""
    <tr style="background:{bg};border-bottom:1px solid #eee;">
      <td style="padding:9px 8px;color:#999;">{i}</td>
      <td style="padding:9px 8px;font-weight:600;">{row.Address}</td>
      <td style="padding:9px 8px;">{row.Zip}</td>
      <td style="padding:9px 8px;">{row.Type if row.Type and row.Type != 'nan' else '—'}</td>
      <td style="padding:9px 8px;font-weight:700;color:#d7301f;">{row._5}</td>
      <td style="padding:9px 8px;">{row.Status}</td>
      <td style="padding:9px 8px;color:#555;">{row.Contractor}</td>
    </tr>"""

    paid_html += "</tbody></table>"

    # Write both files
    os.makedirs("newsletter_output", exist_ok=True)
    with open("newsletter_output/free_table.html", "w") as f:
        f.write(free_html)
    with open("newsletter_output/paid_table.html", "w") as f:
        f.write(paid_html)

    print("✅ Tables generated!")
    print("   Free table (10 rows):  newsletter_output/free_table.html")
    print("   Paid table (50 rows): newsletter_output/paid_table.html")

    # Also save as CSV for Google Sheets
    df.to_csv("newsletter_output/listings.csv", index=False)
    print("   CSV for Google Sheets: newsletter_output/listings.csv")


if __name__ == "__main__":
    generate_tables(
        bucket=os.environ.get("S3_BUCKET", "claude-seattle-housing-data"),
        prefix=os.environ.get("S3_PREFIX", "seattle-housing/raw"),
        region=os.environ.get("AWS_REGION", "us-west-2"),
    )
