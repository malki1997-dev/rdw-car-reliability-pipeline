import sys
import time
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path

PAGE_SIZE = 50000
BASE = "https://opendata.rdw.nl/resource/{id}.csv"

# id du dataset + colonnes à garder (None = toutes)
DATASETS = {
    "vehicules": ("m9d7-ebf2", [
        "kenteken", "voertuigsoort", "merk", "handelsbenaming", "inrichting",
        "datum_eerste_toelating", "datum_eerste_tenaamstelling_in_nederland",
        "datum_tenaamstelling", "catalogusprijs", "export_indicator",
        "openstaande_terugroepactie_indicator", "taxi_indicator"]),
    "carburant": ("8ys7-d773", [
        "kenteken", "brandstof_volgnummer", "brandstof_omschrijving",
        "klasse_hybride_elektrisch_voertuig"]),
    "controles": ("sgfe-77wx", [
        "kenteken", "soort_erkenning_keuringsinstantie",
        "meld_datum_door_keuringsinstantie", "meld_tijd_door_keuringsinstantie",
        "soort_erkenning_omschrijving", "soort_melding_ki_omschrijving",
        "vervaldatum_keuring"]),
    "defauts_constates": ("a34c-vvps", [
        "kenteken", "soort_erkenning_keuringsinstantie",
        "meld_datum_door_keuringsinstantie", "meld_tijd_door_keuringsinstantie",
        "gebrek_identificatie", "soort_erkenning_omschrijving",
        "aantal_gebreken_geconstateerd"]),
    "ref_defauts": ("hx2c-gt7k", None),
}


def fetch(url, retries=5):
    """Télécharge une page, avec plusieurs essais en cas d'erreur réseau."""
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=120) as r:
                return r.read()
        except Exception as e:
            wait = 10 * attempt
            print(f"\n  [retry {attempt}/{retries}] {e} — nouvel essai dans {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"Échec après {retries} essais : {url}")


def download(name, ingestion_date):
    ds_id, cols = DATASETS[name]
    out_dir = Path("data/landing") / name / f"ingestion_date={ingestion_date}"
    done_flag = out_dir / "_SUCCESS"   # présent seulement si TOUT est téléchargé

    if done_flag.exists():
        print(f"[skip] {name} déjà complet")
        return

    out_dir.mkdir(parents=True, exist_ok=True)
    page, total, t0 = 0, 0, time.time()
    while True:
        part = out_dir / f"part-{page:05d}.csv"
        if part.exists():
            # Reprise : cette page a déjà été téléchargée
            n = sum(1 for _ in open(part, "rb")) - 1
        else:
            params = {"$limit": PAGE_SIZE, "$offset": page * PAGE_SIZE, "$order": ":id"}
            if cols:
                params["$select"] = ",".join(cols)
            data = fetch(BASE.format(id=ds_id) + "?" + urllib.parse.urlencode(params))
            tmp = part.with_suffix(".part")
            tmp.write_bytes(data)
            tmp.rename(part)
            n = data.count(b"\n") - 1
        total += max(n, 0)
        print(f"\r  {name} : page {page} — {total:,} lignes", end="", flush=True)
        if n < PAGE_SIZE:   # dernière page
            break
        page += 1

    done_flag.touch()
    print(f"\n[ok] {name} : {total:,} lignes en {time.time() - t0:.0f} s")


if __name__ == "__main__":
    names = sys.argv[1:] or list(DATASETS)
    today = date.today().isoformat()
    for name in names:
        download(name, today)
