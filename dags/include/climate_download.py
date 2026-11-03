"""Téléchargement ERA5-Land (Copernicus CDS) et CHIRPS v3.0 (UCSB), découpés sur une emprise.

bbox = (ouest, sud, est, nord) en degrés WGS84. Les deux fonctions sont idempotentes :
un fichier déjà présent n'est pas retéléchargé, et un fichier n'apparaît sous son nom
final qu'une fois complet (écriture en .part puis renommage).

ERA5-Land : licence acceptée sur la page du dataset, et identifiants CDS fournis par
~/.cdsapirc ou les variables d'environnement CDSAPI_URL / CDSAPI_KEY.
"""

import calendar
import logging
import time
from datetime import date, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

CHIRPS_BASE_URL = "https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/final"


def download_era5_land(
    start: date,
    end: date,
    bbox: tuple[float, float, float, float],
    out_dir: Path,
    variables: tuple[str, ...] = ("2m_temperature", "total_precipitation"),
) -> list[Path]:
    """Télécharge ERA5-Land horaire, un NetCDF par mois couvert par [start, end].

    Unités : température en K, précipitations en m, cumulées depuis 00h UTC.
    """
    import cdsapi

    west, south, east, north = bbox
    out_dir = Path(out_dir) / "era5_land"
    out_dir.mkdir(parents=True, exist_ok=True)
    client = cdsapi.Client()
    files = []

    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        first = max(start, date(year, month, 1))
        last = min(end, date(year, month, calendar.monthrange(year, month)[1]))
        target = out_dir / f"era5_land_{year}_{month:02d}_{first.day:02d}-{last.day:02d}.nc"
        files.append(target)

        if target.exists():
            log.info("Déjà présent, ignoré : %s", target.name)
        else:
            request = {
                "variable": list(variables),
                "year": str(year),
                "month": f"{month:02d}",
                "day": [f"{d:02d}" for d in range(first.day, last.day + 1)],
                "time": [f"{h:02d}:00" for h in range(24)],
                "area": [north, west, south, east],  # ordre imposé par le CDS : N, O, S, E
                "data_format": "netcdf",
                "download_format": "unarchived",
            }
            log.info("ERA5-Land %s → %s : requête envoyée au CDS…", first, last)
            partial = target.with_suffix(".nc.part")
            client.retrieve("reanalysis-era5-land", request).download(str(partial))
            partial.rename(target)
            log.info("Écrit : %s", target)

        year, month = (year + 1, 1) if month == 12 else (year, month + 1)

    return files


def download_chirps(
    start: date,
    end: date,
    bbox: tuple[float, float, float, float],
    out_dir: Path,
    product: str = "rnl",
    pause: float = 1.0,
) -> list[Path]:
    """Télécharge CHIRPS v3.0 journalier (mm/jour), un GeoTIFF découpé par jour.

    product : "rnl" (répartition journalière via ERA5, dès 1981) ou "sat" (via IMERG, dès 1998).
    Lit les COG distants par fenêtre : seule l'emprise demandée est transférée.
    Les données "final" sont publiées avec environ un mois de décalage : la boucle s'arrête
    au premier jour indisponible, la liste renvoyée ne contient alors que les jours obtenus.
    """
    import rasterio
    from rasterio.errors import RasterioIOError
    from rasterio.windows import from_bounds

    out_dir = Path(out_dir) / f"chirps_v3_{product}"
    out_dir.mkdir(parents=True, exist_ok=True)
    files = []

    # Le serveur CHIRPS renvoie 429 quand on enchaîne trop de requêtes : GDAL réessaie
    # automatiquement, et on évite qu'il liste le dossier distant à chaque ouverture.
    with rasterio.Env(
        GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
        GDAL_HTTP_MAX_RETRY="5",
        GDAL_HTTP_RETRY_DELAY="10",
    ):
        day = start
        while day <= end:
            name = f"chirps-v3.0.{product}.{day:%Y.%m.%d}"
            target = out_dir / f"{name}.tif"
            if target.exists():
                log.info("Déjà présent, ignoré : %s", target.name)
                files.append(target)
                day += timedelta(days=1)
                continue

            url = f"{CHIRPS_BASE_URL}/{product}/cogs/{day.year}/{name}.cog"
            try:
                with rasterio.open(url) as src:
                    window = from_bounds(*bbox, transform=src.transform)
                    window = window.round_offsets().round_lengths()
                    data = src.read(1, window=window)
                    profile = {
                        "driver": "GTiff",
                        "dtype": data.dtype,
                        "count": 1,
                        "width": data.shape[1],
                        "height": data.shape[0],
                        "crs": src.crs,
                        "transform": src.window_transform(window),
                        "nodata": src.nodata,
                        "compress": "deflate",
                    }
            except RasterioIOError as exc:
                log.warning("CHIRPS %s indisponible (%s), arrêt.", day, exc)
                break

            partial = target.with_suffix(".tif.part")
            with rasterio.open(partial, "w", **profile) as dst:
                dst.write(data, 1)
                dst.update_tags(source=url, units="mm/day")
            partial.rename(target)
            files.append(target)
            log.info("Écrit : %s (%sx%s px)", target.name, profile["width"], profile["height"])
            time.sleep(pause)
            day += timedelta(days=1)

    return files
