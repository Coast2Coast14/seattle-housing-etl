"""
Newsletter: Neighborhood Heat Map Generator

Reads the latest weekly Parquet file from S3, computes permit activity
by zip code, and produces an HTML heat map section ready to embed in
a newsletter (Beehiiv, Substack, or raw email).

Usage:
    python -m newsletter.heatmap

Output:
    - heatmap_output/heatmap.html  (standalone preview)
    - heatmap_output/heatmap_snippet.html  (embeddable newsletter block)
"""

from __future__ import annotations

import io
import os
import logging
from datetime import datetime, timezone

import boto3
import pandas as pd

logger = logging.getLogger("seattle_housing_etl.newsletter.heatmap")

# ── Colour scale (light → dark) mapped to activity intensity ─────────────────
HEAT_COLOURS = [
    "#fff7ec",
    "#fee8c8",
    "#fdd49e",
    "#fdbb84",
    "#fc8d59",
    "#ef6548",
    "#d7301f",
    "#990000",
]


def get_latest_s3_parquet(bucket: str, prefix: str, region: str) -> pd.DataFrame:
    """
    Find and load the most recent Parquet file from S3.

    Args:
        bucket: S3 bucket name
        prefix: Key prefix, e.g. 'seattle-housing/raw'
        region: AWS region string

    Returns:
        DataFrame of the latest weekly run
    """
    s3 = boto3.client("s3", region_name=region)

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    all_keys = []
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                all_keys.append((obj["LastModified"], obj["Key"]))

    if not all_keys:
        raise FileNotFoundError(f"No Parquet files found at s3://{bucket}/{prefix}")

    # Sort by last modified, take the newest
    all_keys.sort(key=lambda x: x[0], reverse=True)
    latest_key = all_keys[0][1]
    logger.info(f"Loading latest file: s3://{bucket}/{latest_key}")

    obj = s3.get_object(Bucket=bucket, Key=latest_key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    logger.info(f"Loaded {len(df)} records from S3")
    return df


def compute_zip_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate permit activity by zip code.

    Returns DataFrame with columns:
        zip, permit_count, total_value, avg_value, top_type
    """
    df = df.dropna(subset=["originalzip"])
    df["originalzip"] = df["originalzip"].astype(str).str.strip().str[:5]

    stats = (
        df.groupby("originalzip")
        .agg(
            permit_count=("permitnum", "count"),
            total_value=("estprojectcost", "sum"),
            avg_value=("estprojectcost", "mean"),
        )
        .reset_index()
        .sort_values("permit_count", ascending=False)
    )

    # Most common permit type per zip
    top_type = (
        df.groupby("originalzip")["permittypedesc"]
        .agg(lambda x: x.value_counts().index[0] if len(x) > 0 else "N/A")
        .reset_index()
        .rename(columns={"permittypedesc": "top_type"})
    )

    stats = stats.merge(top_type, on="originalzip", how="left")

    # Assign heat intensity 0–7
    max_count = stats["permit_count"].max()
    stats["intensity"] = (
        (stats["permit_count"] / max_count * 7).round().astype(int).clip(0, 7)
    )
    stats["heat_colour"] = stats["intensity"].map(lambda i: HEAT_COLOURS[i])

    return stats


def format_currency(value: float) -> str:
    """Format a number as a compact currency string."""
    if value >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"${value / 1_000:.0f}K"
    return f"${value:.0f}"


def build_heatmap_html(stats: pd.DataFrame, week_label: str) -> tuple[str, str]:
    """
    Build a full standalone HTML page and an embeddable snippet.

    Args:
        stats:      Aggregated zip stats DataFrame
        week_label: Human-readable week string, e.g. 'May 19 – May 25, 2026'

    Returns:
        (full_html, snippet_html) tuple
    """
    total_permits = stats["permit_count"].sum()
    total_value = stats["total_value"].sum()
    top_zip = stats.iloc[0]["originalzip"]
    top_zip_count = stats.iloc[0]["permit_count"]

    # ── Build zip rows ────────────────────────────────────────────────────────
    rows_html = ""
    for rank, row in enumerate(stats.head(15).itertuples(), start=1):
        bar_pct = int((row.permit_count / stats["permit_count"].max()) * 100)
        rows_html += f"""
        <tr>
            <td style="padding:10px 8px;font-weight:600;color:#555;width:28px;">
                #{rank}
            </td>
            <td style="padding:10px 8px;">
                <span style="
                    display:inline-block;
                    background:{row.heat_colour};
                    color:{'#fff' if row.intensity >= 4 else '#333'};
                    border-radius:4px;
                    padding:3px 10px;
                    font-weight:700;
                    font-size:15px;
                    min-width:60px;
                    text-align:center;
                ">
                    {row.originalzip}
                </span>
            </td>
            <td style="padding:10px 8px;">
                <div style="display:flex;align-items:center;gap:8px;">
                    <div style="
                        background:{row.heat_colour};
                        width:{bar_pct}%;
                        max-width:160px;
                        min-width:8px;
                        height:16px;
                        border-radius:3px;
                        border:1px solid #ddd;
                    "></div>
                    <span style="font-weight:700;color:#222;">
                        {row.permit_count} permit{'s' if row.permit_count != 1 else ''}
                    </span>
                </div>
            </td>
            <td style="padding:10px 8px;color:#555;">
                {format_currency(row.total_value)}
            </td>
            <td style="padding:10px 8px;color:#888;font-size:13px;">
                {row.top_type if row.top_type and row.top_type != 'nan' else '—'}
            </td>
        </tr>"""

    # ── Legend ────────────────────────────────────────────────────────────────
    legend_cells = "".join(
        f'<td style="background:{c};width:24px;height:14px;border-radius:2px;"></td>'
        for c in HEAT_COLOURS
    )

    # ── Embeddable snippet ────────────────────────────────────────────────────
    snippet = f"""
<div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
            max-width:620px;margin:0 auto;padding:24px 0;">

  <!-- Header -->
  <h2 style="font-size:22px;font-weight:800;color:#1a1a1a;margin:0 0 4px;">
    🗺️ Seattle Permit Activity by Neighborhood
  </h2>
  <p style="color:#666;font-size:14px;margin:0 0 20px;">
    Week of {week_label} &nbsp;·&nbsp;
    <strong>{total_permits} permits</strong> &nbsp;·&nbsp;
    <strong>{format_currency(total_value)}</strong> total project value
  </p>

  <!-- Summary callout -->
  <div style="background:#fff7ec;border-left:4px solid #fc8d59;
              border-radius:4px;padding:14px 18px;margin-bottom:20px;">
    <strong>📍 Hottest zip this week:</strong>
    <span style="font-size:16px;font-weight:700;color:#d7301f;">
      &nbsp;{top_zip}
    </span>
    &nbsp;with <strong>{top_zip_count} permits</strong>
  </div>

  <!-- Heat map table -->
  <table style="width:100%;border-collapse:collapse;font-size:14px;">
    <thead>
      <tr style="border-bottom:2px solid #eee;">
        <th style="padding:8px;text-align:left;color:#999;font-weight:600;
                   font-size:12px;text-transform:uppercase;">Rank</th>
        <th style="padding:8px;text-align:left;color:#999;font-weight:600;
                   font-size:12px;text-transform:uppercase;">Zip</th>
        <th style="padding:8px;text-align:left;color:#999;font-weight:600;
                   font-size:12px;text-transform:uppercase;">Activity</th>
        <th style="padding:8px;text-align:left;color:#999;font-weight:600;
                   font-size:12px;text-transform:uppercase;">Total Value</th>
        <th style="padding:8px;text-align:left;color:#999;font-weight:600;
                   font-size:12px;text-transform:uppercase;">Top Type</th>
      </tr>
    </thead>
    <tbody>{rows_html}
    </tbody>
  </table>

  <!-- Colour legend -->
  <div style="margin-top:16px;display:flex;align-items:center;gap:8px;
              font-size:12px;color:#999;">
    <span>Less active</span>
    <table style="border-collapse:collapse;">
      <tr>{legend_cells}</tr>
    </table>
    <span>More active</span>
  </div>

</div>
"""

    # ── Full standalone page ──────────────────────────────────────────────────
    full_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Seattle Housing Heat Map — {week_label}</title>
  <style>
    body {{
      background: #f5f5f5;
      display: flex;
      justify-content: center;
      padding: 40px 16px;
      margin: 0;
    }}
    .card {{
      background: #fff;
      border-radius: 12px;
      box-shadow: 0 2px 16px rgba(0,0,0,0.08);
      padding: 32px;
      max-width: 680px;
      width: 100%;
    }}
  </style>
</head>
<body>
  <div class="card">
    {snippet}
    <p style="font-size:12px;color:#bbb;margin-top:24px;text-align:center;">
      Data source: Seattle Open Data — Building Permits &nbsp;·&nbsp;
      Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
    </p>
  </div>
</body>
</html>"""

    return full_html, snippet


def generate(
    bucket: str,
    prefix: str = "seattle-housing/raw",
    region: str = "us-west-2",
    output_dir: str = "heatmap_output",
) -> dict:
    """
    Full heat map generation pipeline.

    Args:
        bucket:     S3 bucket name
        prefix:     S3 key prefix
        region:     AWS region
        output_dir: Local directory to write HTML files

    Returns:
        Dict with output file paths and summary stats
    """
    os.makedirs(output_dir, exist_ok=True)

    # Load data
    df = get_latest_s3_parquet(bucket, prefix, region)

    # Compute stats
    stats = compute_zip_stats(df)

    # Build week label from the data
    if "issueddate" in df.columns:
        try:
            dates = pd.to_datetime(df["issueddate"], errors="coerce").dropna()
            week_label = (
                f"{dates.min().strftime('%b %-d')} – "
                f"{dates.max().strftime('%b %-d, %Y')}"
            )
        except Exception:
            week_label = datetime.now(timezone.utc).strftime("Week of %b %-d, %Y")
    else:
        week_label = datetime.now(timezone.utc).strftime("Week of %b %-d, %Y")

    # Build HTML
    full_html, snippet = build_heatmap_html(stats, week_label)

    # Write files
    full_path = os.path.join(output_dir, "heatmap.html")
    snippet_path = os.path.join(output_dir, "heatmap_snippet.html")

    with open(full_path, "w") as f:
        f.write(full_html)

    with open(snippet_path, "w") as f:
        f.write(snippet)

    logger.info(f"Heat map written to {full_path}")
    logger.info(f"Snippet written to {snippet_path}")

    return {
        "full_html_path": full_path,
        "snippet_path": snippet_path,
        "top_zip": stats.iloc[0]["originalzip"],
        "total_permits": int(stats["permit_count"].sum()),
        "zips_covered": len(stats),
        "week_label": week_label,
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )

    result = generate(
        bucket=os.environ.get("S3_BUCKET", "your-bucket-name"),
        prefix=os.environ.get("S3_PREFIX", "seattle-housing/raw"),
        region=os.environ.get("AWS_REGION", "us-west-2"),
    )

    print("\n✅ Heat map generated successfully!")
    print(f"   Week:          {result['week_label']}")
    print(f"   Top zip:       {result['top_zip']}")
    print(f"   Total permits: {result['total_permits']}")
    print(f"   Zips covered:  {result['zips_covered']}")
    print(f"\n   Preview:  open {result['full_html_path']}")
    print(f"   Snippet:  {result['snippet_path']}")
