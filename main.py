import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta, time, timezone  # <-- ajoute timezone
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError  # Python 3.9+
except ImportError:
    ZoneInfo = None
    class ZoneInfoNotFoundError(Exception): ...
    pass

import db
import sys, os, shutil, subprocess, json
import hashlib, tempfile, textwrap

def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

APP_VERSION = "1.5"
LATEST_URL = "https://raw.githubusercontent.com/jolie-croquette/ludov-seeder/refs/heads/main/latest.json"

BASE_URL = "https://ludov.inlibro.net/api/v1"
ENDPOINT = "/biblios"       # ou "/items"
USERNAME = "apicatalogue"
PASSWORD = "apicatalogue"

PER_PAGE = 10000            # pagination serveur

ALL_BIBLIOS = []

print(f"""
=========================================
   LUDOV SEEDER
   Générateur de données pour LUDOV
   Auteur : jolie-croquette
   Date : 16/09/2025
   Version actuelle : {APP_VERSION}
=========================================
""")

def check_for_update():
    resp = requests.get(LATEST_URL, headers={"Cache-Control":"no-cache"}, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    latest_version = data.get("version")
    latest_tag     = data.get("tag")       # ex: "v1.6"
    download_url   = data.get("url")
    expected_sha   = data.get("sha256")

    if not latest_version or not download_url:
        print("⚠ latest.json incomplet.")
        return

    if latest_version == APP_VERSION:
        print("✅ Application à jour")
        return

    # (optionnel) cohérence tag/URL
    if latest_tag and f"/{latest_tag}/" not in download_url:
        print(f"⚠ Incohérence: l'URL ne contient pas le tag {latest_tag}")

    print(f"🔄 Nouvelle version {latest_version} trouvée (actuelle {APP_VERSION})")
    update_app(download_url, expected_sha)   # <- on passe le sha


def update_app(download_url, expected_sha256: str | None):
    exe_path = os.path.abspath(getattr(sys, "executable", sys.argv[0]))
    exe_dir  = os.path.dirname(exe_path)

    # 1) Téléchargement -> fichier temp
    with requests.get(download_url, stream=True, timeout=60) as r:
        r.raise_for_status()
        fd, tmp_path = tempfile.mkstemp(prefix="ludov-update-", suffix=".exe")
        os.close(fd)
        with open(tmp_path, "wb") as f:
            shutil.copyfileobj(r.raw, f)

    # 2) Vérif d'intégrité
    if expected_sha256:
        got = _sha256(tmp_path)
        if got.lower() != expected_sha256.lower():
            try: os.remove(tmp_path)
            except: pass
            print("❌ Hash invalide — mise à jour annulée.")
            return

    # 3) Génère un updater PowerShell qui SANS -ArgumentList si vide
    ps_code = r"""
param(
  [string]$OldPath,
  [string]$TmpPath,
  [string]$Args
)

$retries = 50
while ($retries -gt 0) {
  try {
    $fs = [System.IO.File]::Open($OldPath,'Open','ReadWrite','None')
    $fs.Close()
    break
  } catch {
    Start-Sleep -Milliseconds 200
    $retries -= 1
  }
}

if (Test-Path ($OldPath + ".bak")) { Remove-Item ($OldPath + ".bak") -Force -ErrorAction SilentlyContinue }
Move-Item -LiteralPath $OldPath -Destination ($OldPath + ".bak") -Force
Move-Item -LiteralPath $TmpPath -Destination $OldPath -Force

$psi = @{ FilePath = $OldPath }
if (-not [string]::IsNullOrWhiteSpace($Args)) {
  $psi["ArgumentList"] = $Args
}
Start-Process @psi
"""
    scripts_dir = tempfile.mkdtemp(prefix="ludov-updater-")
    ps_path = os.path.join(scripts_dir, "updater.ps1")
    with open(ps_path, "w", encoding="utf-8") as f:
        f.write(textwrap.dedent(ps_code).strip())

    # arguments à relayer (peut être vide)
    args = " ".join(f'"{a}"' for a in sys.argv[1:]) if len(sys.argv) > 1 else ""

    try:
        subprocess.Popen([
            "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
            "-File", ps_path,
            exe_path,
            tmp_path,
            args
        ], cwd=exe_dir, close_fds=True)
        print("🚀 Mise à jour en cours…")
    except Exception as e:
        print(f"❌ Échec du lanceur de mise à jour : {e}")
        try: os.remove(tmp_path)
        except: pass
        return
    sys.exit(0)

def main():
    conn = db.create_connection()
    if conn is None:
        return

    try:
        db.ensure_database(conn)
        db.use_database(conn)
        db.preview_wipe(conn)

        input(f"La BD sera vidée. Appuyez sur Entrée pour confirmer...")
        db.confirm_and_wipe(conn)

        print("\n=== Import du SQL embarqué ===")
        db.run_embedded_sql(conn)
        print("✅ Schéma importé avec succès.")

        fetch_all_biblios()

        seed_games_from_koha(conn)
        seed_console_from_game(conn)
        seed_users(conn)
        seed_reservations(conn)
    finally:
        try:
            if conn.is_connected():
                conn.close()
                print("Connexion fermée proprement.")
        except NameError:
            pass

def get_toronto_tz():
    if ZoneInfo:
        try:
            return ZoneInfo("America/Toronto")
        except ZoneInfoNotFoundError:
            pass
    return datetime.now().astimezone().tzinfo

TIMEZONE = get_toronto_tz()

def window_for_today_5am_toronto():
    now_local = datetime.now(TIMEZONE)
    today_local = now_local.date()
    start = datetime.combine(today_local - timedelta(days=1), time(5, 0), tzinfo=TIMEZONE)
    end   = datetime.combine(today_local, time(5, 0), tzinfo=TIMEZONE)
    return start, end


def iso_to_toronto(iso_str: str) -> datetime:
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = datetime.strptime(iso_str[:19], "%Y-%m-%dT%H:%M:%S")
            dt = dt.replace(tzinfo=timezone.utc)  # <-- au lieu de ZoneInfo("UTC") ou None
        except Exception:
            return datetime.now(TIMEZONE)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)      # <-- idem ici
    return dt.astimezone(TIMEZONE)


def fetch_all_biblios():
    """Récupère toutes les biblios en une seule passe et les stocke dans ALL_BIBLIOS"""
    global ALL_BIBLIOS
    page = 1
    ALL_BIBLIOS = []
    total = 0
    print("=== TÉLÉCHARGEMENT DES DONNÉES KOHA ===")

    while True:
        try:
            batch = fetch_biblios_page(page)
        except Exception as e:
            print(f"Erreur réseau/API page {page}: {e}")
            break

        if not batch:
            break

        ALL_BIBLIOS.extend(batch)
        total += len(batch)

        if len(batch) < PER_PAGE:
            break
        page += 1

    print(f"📦 Données Koha téléchargées : {len(ALL_BIBLIOS)} enregistrements.")
    return ALL_BIBLIOS

def fetch_biblios_page(page: int):
    url = f"{BASE_URL}{ENDPOINT}"
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "User-Agent": "LUDOVSeeder/1.0",
    }
    params = {"_page": page, "_per_page": PER_PAGE}  # <-- correction
    resp = requests.get(
        url,
        auth=HTTPBasicAuth(USERNAME, PASSWORD),
        headers=headers,
        params=params,
        timeout=60
    )
    resp.raise_for_status()
    return resp.json()

def seed_games_from_koha(conn):
    print("\n=== SEED JEUX KOHA: démarrage ===")
    to_upsert = []

    for b in ALL_BIBLIOS:
        if b.get("item_type") != "JEU":
            continue

        ts_str = b.get("timestamp")
        if not ts_str:
            continue
        ts_local = iso_to_toronto(ts_str)

        biblio_id = b.get("biblio_id")
        if not biblio_id:
            continue
        try:
            gid = int(biblio_id)
        except Exception:
            continue

        title = (b.get("title") or "").strip()
        subtitle = (b.get("subtitle") or "").strip()
        titre = f"{title} - {subtitle}" if subtitle else title

        author = (b.get("author") or None)
        picture = ""  
        available = 1

        to_upsert.append((
            gid, titre, gid, author, picture, available,
            ts_local.strftime("%Y-%m-%d %H:%M:%S"),
        ))

    if not to_upsert:
        print("Aucun jeu à insérer.")
        return
    
    db.insertGameIntoDatabase(conn, to_upsert)

def seed_console_from_game(conn):
    print("\n=== SEED CONSOLES: démarrage ===")
    names = set()
    for b in ALL_BIBLIOS:
        if b.get("item_type") != "JEU":
            continue
        name = (b.get("edition_statement") or "").strip()
        if name:
            names.add(name)

    to_upsert = [(n,) for n in sorted(names)]
    if not to_upsert:
        print("Aucune console à insérer.")
        return
    db.insert_console(conn, to_upsert)


def seed_users(conn):
    user = []

def seed_reservations(conn):
    reservations = []

if __name__ == "__main__":
    check_for_update()
    main()
