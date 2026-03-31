#!/usr/bin/env python3
"""Generate a station-based coverage map for the documentation landing page."""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
from pyproj import Transformer

os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "rivretrieve-mplconfig"))

import matplotlib
from matplotlib import pyplot as plt

matplotlib.use("Agg")


REPO_ROOT = Path(__file__).resolve().parents[1]

WORLD_SHP_GLOB = "python*/site-packages/pyogrio/tests/fixtures/naturalearth_lowres/naturalearth_lowres.shp"
BASE_FETCHER_MODULES = {"__init__", "__about__", "base", "constants", "utils"}

STATION_SOURCES = [
    {"csv": "australia_sites.csv", "label": "Australia", "country": "Australia"},
    {"csv": "brazil_sites.csv", "label": "Brazil", "country": "Brazil"},
    {"csv": "canada_sites.csv", "label": "Canada", "country": "Canada"},
    {"csv": "chile_sites.csv", "label": "Chile", "country": "Chile"},
    {"csv": "czech_sites.csv", "label": "Czechia", "country": "Czechia"},
    {"csv": "french_sites.csv", "label": "France", "country": "France"},
    {"csv": "germany_berlin_sites.csv", "label": "Germany (Berlin only)", "country": "Germany"},
    {"csv": "japan_sites.csv", "label": "Japan", "country": "Japan"},
    {"csv": "lithuania_sites.csv", "label": "Lithuania", "country": "Lithuania"},
    {"csv": "norway_sites.csv", "label": "Norway", "country": "Norway"},
    {"csv": "poland_sites.csv", "label": "Poland", "country": "Poland"},
    {"csv": "portugal_sites.csv", "label": "Portugal", "country": "Portugal"},
    {"csv": "slovenia_sites.csv", "label": "Slovenia", "country": "Slovenia"},
    {"csv": "southAfrican_sites.csv", "label": "South Africa", "country": "South Africa"},
    {"csv": "spain_sites.csv", "label": "Spain", "country": "Spain", "converter": "spain_utm30"},
    {"csv": "uk_ea_sites.csv", "label": "United Kingdom (EA)", "country": "United Kingdom"},
    {"csv": "uk_nrfa_sites.csv", "label": "United Kingdom (NRFA)", "country": "United Kingdom"},
    {"csv": "usa_sites.csv", "label": "United States", "country": "United States of America"},
]

FETCHER_LABELS = {
    "argentina": "Argentina",
    "austria": "Austria",
    "belgium_flanders": "Belgium (Flanders)",
    "belgium_wallonia": "Belgium (Wallonia)",
    "bosnia_herzegovina": "Bosnia and Herzegovina",
    "denmark": "Denmark",
    "estonia": "Estonia",
    "finland": "Finland",
    "greece": "Greece",
    "ireland_opw": "Ireland (OPW)",
    "italy_toscany": "Italy (Tuscany)",
    "netherlands": "Netherlands",
    "southkorea": "South Korea",
    "sweden": "Sweden",
    "switzerland": "Switzerland",
    "taiwan": "Taiwan",
    "thailand": "Thailand",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "docs" / "_static" / "station-coverage-map.png",
        help="Path to the generated map image.",
    )
    parser.add_argument(
        "--world-shapefile",
        type=Path,
        default=None,
        help="Optional path to a world shapefile. If omitted, the script looks inside .venv.",
    )
    parser.add_argument(
        "--readme-output",
        type=Path,
        default=REPO_ROOT / "_static" / "station-coverage-map.png",
        help="Optional duplicate output for the repo-root README image path.",
    )
    parser.add_argument(
        "--update-readme",
        action="store_true",
        help="Also refresh the landing-page map block in README.md.",
    )
    parser.add_argument(
        "--readme-path",
        type=Path,
        default=REPO_ROOT / "README.md",
        help="README file to update when --update-readme is used.",
    )
    return parser.parse_args()


def find_world_shapefile(explicit_path: Path | None) -> Path:
    if explicit_path is not None:
        if explicit_path.exists():
            return explicit_path
        raise FileNotFoundError(f"World shapefile not found: {explicit_path}")

    lib_dir = REPO_ROOT / ".venv" / "lib"
    matches = sorted(lib_dir.glob(WORLD_SHP_GLOB))
    if matches:
        return matches[0]

    raise FileNotFoundError(
        "Could not find a local Natural Earth shapefile. Pass --world-shapefile to specify one explicitly."
    )


def load_station_table(source: dict[str, str]) -> pd.DataFrame:
    csv_path = REPO_ROOT / "rivretrieve" / "cached_site_data" / source["csv"]
    df = pd.read_csv(csv_path)

    if source.get("converter") == "spain_utm30":
        transformer = Transformer.from_crs("EPSG:25830", "EPSG:4326", always_xy=True)
        x = pd.to_numeric(df["COORD_UTMX_H30_ETRS89"], errors="coerce")
        y = pd.to_numeric(df["COORD_UTMY_H30_ETRS89"], errors="coerce")
        lon, lat = transformer.transform(x.to_numpy(), y.to_numpy())
        coords = pd.DataFrame({"longitude": lon, "latitude": lat})
    else:
        coords = pd.DataFrame(
            {
                "longitude": pd.to_numeric(df["longitude"], errors="coerce"),
                "latitude": pd.to_numeric(df["latitude"], errors="coerce"),
            }
        )

    coords["source_label"] = source["label"]
    coords["country_name"] = source["country"]
    coords["csv_name"] = source["csv"]
    return coords


def git_lines(*args: str) -> list[str]:
    return subprocess.check_output(["git", *args], cwd=REPO_ROOT, text=True).splitlines()


def get_fetcher_modules(treeish: str) -> set[str]:
    files = git_lines("ls-tree", "-r", "--name-only", treeish)
    fetchers = {Path(path).stem for path in files if path.startswith("rivretrieve/") and path.endswith(".py")}
    return {name for name in fetchers if name not in BASE_FETCHER_MODULES}


def fetcher_label(module_name: str) -> str:
    return FETCHER_LABELS.get(module_name, module_name.replace("_", " ").title())


def format_human_list(items: list[str]) -> str:
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return f"{', '.join(items[:-1])} and {items[-1]}"


def detect_under_implementation_labels() -> list[str]:
    main_fetchers = get_fetcher_modules("main")
    branch_names = sorted(git_lines("branch", "--format=%(refname:short)"))

    pending_modules: set[str] = set()
    for branch_name in branch_names:
        if branch_name == "main" or branch_name.startswith("backup/") or branch_name.startswith("docs-"):
            continue
        pending_modules.update(get_fetcher_modules(branch_name) - main_fetchers)

    return sorted(fetcher_label(module_name) for module_name in pending_modules)


def build_coverage_block(pending_labels: list[str]) -> str:
    return "\n".join(
        [
            "## Current coverage",
            "",
            "The map below shows station locations for the fetchers currently implemented on `main`.",
            "",
            "![Station coverage map](_static/station-coverage-map.png)",
        ]
    )


def update_readme(readme_path: Path, pending_labels: list[str]) -> None:
    content = readme_path.read_text()
    pattern = re.compile(
        r"(- Documentation: .*?\n\n)(.*?)(\n## Background\n)",
        re.DOTALL | re.MULTILINE,
    )
    new_block = build_coverage_block(pending_labels)
    updated, count = pattern.subn(
        lambda match: match.group(1) + new_block + "\n\n## Background\n",
        content,
        count=1,
    )
    if count != 1:
        raise ValueError("Could not locate the README landing-page map block.")
    readme_path.write_text(updated)


def load_station_points() -> gpd.GeoDataFrame:
    tables = [load_station_table(source) for source in STATION_SOURCES]
    stations = pd.concat(tables, ignore_index=True)

    valid_range = (
        stations["latitude"].notna()
        & stations["longitude"].notna()
        & stations["latitude"].between(-90, 90)
        & stations["longitude"].between(-180, 180)
    )
    stations = stations.loc[valid_range].copy()

    return gpd.GeoDataFrame(
        stations,
        geometry=gpd.points_from_xy(stations["longitude"], stations["latitude"]),
        crs="EPSG:4326",
    )


def drop_obvious_outliers(stations: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Remove obviously implausible coordinates without trimming real island/coastal stations."""

    keep_mask = pd.Series(True, index=stations.index)

    australia = stations["source_label"] == "Australia"
    keep_mask &= (~australia) | (
        stations["latitude"].between(-60, 0) & stations["longitude"].between(90, 180)
    )

    canada = stations["source_label"] == "Canada"
    keep_mask &= (~canada) | (stations["longitude"] < -50)

    return stations.loc[keep_mask].copy()


def wrap_text_to_axes_width(fig: plt.Figure, ax: plt.Axes, text: str, fontsize: float) -> str:
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    max_width = ax.get_window_extent(renderer=renderer).width

    words = text.split()
    if not words:
        return ""

    lines = [words[0]]
    for word in words[1:]:
        candidate = f"{lines[-1]} {word}"
        probe = ax.text(0, 0, candidate, fontsize=fontsize, alpha=0)
        probe_width = probe.get_window_extent(renderer=renderer).width
        probe.remove()

        if probe_width <= max_width:
            lines[-1] = candidate
        else:
            lines.append(word)

    return "\n".join(lines)


def render_map(
    world: gpd.GeoDataFrame,
    stations: gpd.GeoDataFrame,
    output_path: Path,
    pending_labels: list[str],
) -> None:
    world_plot = world.loc[world["name"] != "Antarctica"].copy().to_crs("+proj=robin")
    station_plot = stations.to_crs(world_plot.crs)

    fig = plt.figure(figsize=(13.5, 6.55), facecolor="#fbfcfd")
    ax = fig.add_axes([0.015, 0.13, 0.97, 0.82])
    ax.set_facecolor("#fbfcfd")

    world_plot.plot(
        ax=ax,
        color="#e6ecf1",
        edgecolor="#ffffff",
        linewidth=0.4,
    )
    station_plot.plot(
        ax=ax,
        markersize=1.3,
        color="#0f7b8f",
        alpha=0.35,
        linewidth=0,
    )

    station_minx, _, station_maxx, _ = station_plot.total_bounds
    world_minx, world_miny, world_maxx, world_maxy = world_plot.total_bounds
    x_span = station_maxx - station_minx
    y_span = world_maxy - world_miny
    ax.set_xlim(station_minx - (x_span * 0.012), station_maxx + (x_span * 0.025))
    ax.set_ylim(world_miny - (y_span * 0.008), world_maxy + (y_span * 0.008))

    total_fetchers = len(STATION_SOURCES)
    total_stations = len(station_plot)
    subheadline = f"{total_stations:,} cached station locations across {total_fetchers} fetchers"

    footer_ax = fig.add_axes([0.03, 0.012, 0.94, 0.10])
    footer_ax.set_axis_off()
    footer_ax.text(
        0.0,
        1.0,
        subheadline,
        ha="left",
        va="top",
        fontsize=10.2,
        color="#33566f",
        transform=footer_ax.transAxes,
    )
    if pending_labels:
        footer = "Under implementation: " + format_human_list(pending_labels) + "."
        footer_fontsize = 8.35
        wrapped_footer = wrap_text_to_axes_width(fig, footer_ax, footer, footer_fontsize)
        footer_ax.text(
            0.0,
            0.42,
            wrapped_footer,
            ha="left",
            va="top",
            fontsize=footer_fontsize,
            color="#647f93",
            linespacing=1.15,
            transform=footer_ax.transAxes,
        )

    ax.set_axis_off()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=220, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    world_shapefile = find_world_shapefile(args.world_shapefile)
    world = gpd.read_file(world_shapefile)
    stations = load_station_points()
    cleaned_stations = drop_obvious_outliers(stations)
    pending_labels = detect_under_implementation_labels()
    render_map(world, cleaned_stations, args.output, pending_labels)
    args.readme_output.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(args.output, args.readme_output)
    if args.update_readme:
        update_readme(args.readme_path, pending_labels)


if __name__ == "__main__":
    main()
