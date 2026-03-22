"""
INPI Bulk Processor — Cabinet ACC
Télécharge les comptes annuels JSON depuis le FTP INPI
et construit une base DuckDB avec les ratios par entreprise.

Env vars requises (GitHub Secrets) :
    INPI_FTP_USER
    INPI_FTP_PASS
"""

import os, sys, json, ftplib, gzip, io, re, logging
from pathlib import Path
from datetime import datetime

import duckdb
from tqdm import tqdm

# ── Config ──────────────────────────────────────────────────────────────────
FTP_HOST  = "www.inpi.net"
FTP_USER  = os.environ["INPI_FTP_USER"]
FTP_PASS  = os.environ["INPI_FTP_PASS"]

DATA_DIR  = Path("./inpi_bulk/raw")
DB_PATH   = Path("./inpi_bulk/benchmark.duckdb")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Connexion FTP ────────────────────────────────────────────────────────────
def connect_ftp():
    log.info(f"Connexion FTP {FTP_HOST}…")
    ftp = ftplib.FTP(FTP_HOST)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.set_pasv(True)
    log.info("Connecté ✓")
    return ftp

def list_ftp(ftp, path="/"):
    """Liste récursive des fichiers JSON/gz sur le FTP."""
    files = []
    try:
        entries = []
        ftp.retrlines(f"LIST {path}", entries.append)
        for entry in entries:
            parts = entry.split()
            if not parts: continue
            name = parts[-1]
            is_dir = entry.startswith("d")
            full = f"{path.rstrip('/')}/{name}"
            if is_dir and name not in (".", ".."):
                files += list_ftp(ftp, full)
            elif name.endswith((".json", ".json.gz", ".gz")):
                files.append(full)
    except Exception as e:
        log.warning(f"Erreur listing {path}: {e}")
    return files

def download_file(ftp, remote_path, local_path):
    """Télécharge un fichier FTP vers local."""
    local_path.parent.mkdir(parents=True, exist_ok=True)
    if local_path.exists():
        return False  # déjà téléchargé
    with open(local_path, "wb") as f:
        ftp.retrbinary(f"RETR {remote_path}", f.write)
    return True

# ── Parsing bilans JSON INPI ─────────────────────────────────────────────────
def safe_num(val):
    """Convertit en float, retourne None si impossible."""
    try:
        v = float(str(val).replace(",", ".").replace(" ", ""))
        return v if abs(v) < 1e12 else None
    except:
        return None

def parse_bilan(data):
    """
    Extrait les ratios financiers d'un bilan JSON INPI.
    Le format INPI utilise des codes Cerfa (ex: HN=CA, HI=résultat net…)
    """
    if not isinstance(data, dict):
        return None

    # Navigation dans la structure INPI
    # Structure possible : {siren, codeNaf, dateClotureExercice, compteDeResultat: {…}, bilanActif: {…}, bilanPassif: {…}}
    siren       = data.get("siren") or data.get("identifiantSiren")
    naf         = data.get("codeNaf") or data.get("codeApe") or data.get("activitePrincipale")
    date_clo    = str(data.get("dateClotureExercice") or data.get("dateCloture") or "")
    annee       = date_clo[:4] if date_clo else None

    cr   = data.get("compteDeResultat") or data.get("compteResultat") or {}
    actif = data.get("bilanActif") or data.get("actif") or {}
    passif = data.get("bilanPassif") or data.get("passif") or {}

    # Si structure plate, utiliser data directement
    if not cr and not actif:
        cr = actif = passif = data

    def g(obj, *keys):
        """Cherche une valeur dans obj par liste de clés candidates."""
        for k in keys:
            v = obj.get(k)
            if v is not None:
                n = safe_num(v)
                if n is not None:
                    return n
        return None

    # ── Compte de résultat ──
    ca      = g(cr, "HN","218","chiffreAffairesNet","totalProduits","produitsExploitation")
    achats  = g(cr, "HA","achatsMarchandises","achatsMatieres","achatsConsommes")
    charges_ext = g(cr, "HB","chargesExternes","autresChargesExternes")
    charges_pers = g(cr, "HX","chargesPersonnel","salairesEtTraitements")
    ebe_raw = g(cr, "EBE","exBrutExploitation")
    rexpl   = g(cr, "HF","280","resultatExploitation")
    rnet    = g(cr, "HI","DI","370","resultatExercice","resultatNet")

    if not ca or ca <= 0:
        return None  # Pas de CA → inutilisable

    # Calculs dérivés
    mb      = (ca - achats) if achats is not None else None
    ebe     = ebe_raw or (mb - (charges_ext or 0) - (charges_pers or 0) if mb else None)

    # ── Bilan actif ──
    immo_brut  = g(actif, "BX","totalBrut","actifBrut","immobilisationsBrutes")
    immo_net   = g(actif, "BN","totalActifImmobilise","actifImmobilise")
    immo_amort = g(actif, "BP","totalAmortissements","amortissements")
    actif_circ = g(actif, "BJ","totalActifCirculant","actifCirculant")
    stocks     = g(actif, "BL","stocks","stocksEtEnCours")
    creances   = g(actif, "BR","creancesClients","creances")
    tresorerie = g(actif, "BT","tresorerie","disponibilites")

    # ── Bilan passif ──
    cp_total   = g(passif, "DA","capitauxPropres","totalCapitauxPropres")
    total_passif = g(passif, "DL","totalPassif","total")
    dettes_tot = g(passif, "DR","dettes","totalDettes")
    dettes_ct  = g(passif, "DV","dettesMoinsUnAn","dettesCT","dettesInfUnAn")

    if not total_passif or total_passif <= 0:
        return None

    # ── Ratios ──
    def ratio(num, den):
        return round(num / den * 100, 4) if num is not None and den and den != 0 else None

    return {
        "siren":        siren,
        "naf":          naf,
        "annee":        annee,
        "ca":           round(ca),
        "mb":           round(mb) if mb else None,
        "ebe":          round(ebe) if ebe else None,
        "rexpl":        round(rexpl) if rexpl else None,
        "rnet":         round(rnet) if rnet else None,
        "total_passif": round(total_passif),
        "cp":           round(cp_total) if cp_total else None,
        "dettes_tot":   round(dettes_tot) if dettes_tot else None,
        "dettes_ct":    round(dettes_ct) if dettes_ct else None,
        "actif_circ":   round(actif_circ) if actif_circ else None,
        "immo_net":     round(immo_net) if immo_net else None,
        "immo_brut":    round(immo_brut) if immo_brut else None,
        # Ratios calculés
        "taux_mb":      ratio(mb, ca),
        "taux_ebe":     ratio(ebe, ca),
        "taux_rexpl":   ratio(rexpl, ca),
        "taux_rnet":    ratio(rnet, ca),
        "autonomie":    ratio(cp_total, total_passif),
        "endettement":  ratio(dettes_tot, total_passif),
        "liquidite":    round(actif_circ / dettes_ct, 4) if actif_circ and dettes_ct and dettes_ct > 0 else None,
        "vetuste":      ratio(immo_amort or (immo_brut - immo_net if immo_brut and immo_net else None), immo_brut) if immo_brut and immo_brut > 0 else None,
    }

def read_json_file(path):
    """Lit un fichier JSON ou JSON.gz et retourne une liste de bilans."""
    try:
        if str(path).endswith(".gz"):
            with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
                content = f.read()
        else:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()

        data = json.loads(content)
        # Le fichier peut être un objet unique ou une liste
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            # Parfois encapsulé : {"bilans": [...]} ou directement un bilan
            for key in ["bilans","comptes","data","results","items"]:
                if key in data and isinstance(data[key], list):
                    return data[key]
            return [data]
        return []
    except Exception as e:
        log.debug(f"Erreur lecture {path}: {e}")
        return []

# ── Build DuckDB ─────────────────────────────────────────────────────────────
def build_db(json_dir):
    log.info("Construction de la base DuckDB…")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))
    con.execute("""
        CREATE TABLE IF NOT EXISTS bilans (
            siren       VARCHAR,
            naf         VARCHAR,
            annee       VARCHAR,
            ca          BIGINT,
            mb          BIGINT,
            ebe         BIGINT,
            rexpl       BIGINT,
            rnet        BIGINT,
            total_passif BIGINT,
            cp          BIGINT,
            dettes_tot  BIGINT,
            dettes_ct   BIGINT,
            actif_circ  BIGINT,
            immo_net    BIGINT,
            immo_brut   BIGINT,
            taux_mb     DOUBLE,
            taux_ebe    DOUBLE,
            taux_rexpl  DOUBLE,
            taux_rnet   DOUBLE,
            autonomie   DOUBLE,
            endettement DOUBLE,
            liquidite   DOUBLE,
            vetuste     DOUBLE
        )
    """)

    # Parcourir tous les fichiers JSON
    json_files = list(Path(json_dir).rglob("*.json")) + list(Path(json_dir).rglob("*.json.gz")) + list(Path(json_dir).rglob("*.gz"))
    log.info(f"{len(json_files)} fichiers JSON trouvés")

    total_parsed = 0
    total_inserted = 0
    batch = []
    BATCH_SIZE = 5000

    for jf in tqdm(json_files, desc="Parsing JSON"):
        records = read_json_file(jf)
        for r in records:
            parsed = parse_bilan(r)
            if parsed:
                total_parsed += 1
                batch.append(parsed)
                if len(batch) >= BATCH_SIZE:
                    con.executemany("""
                        INSERT OR IGNORE INTO bilans VALUES (
                            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                        )
                    """, [[b[k] for k in [
                        "siren","naf","annee","ca","mb","ebe","rexpl","rnet",
                        "total_passif","cp","dettes_tot","dettes_ct","actif_circ",
                        "immo_net","immo_brut","taux_mb","taux_ebe","taux_rexpl",
                        "taux_rnet","autonomie","endettement","liquidite","vetuste"
                    ]] for b in batch])
                    total_inserted += len(batch)
                    batch = []

    # Dernier batch
    if batch:
        con.executemany("""INSERT OR IGNORE INTO bilans VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [[b[k] for k in ["siren","naf","annee","ca","mb","ebe","rexpl","rnet",
                "total_passif","cp","dettes_tot","dettes_ct","actif_circ",
                "immo_net","immo_brut","taux_mb","taux_ebe","taux_rexpl",
                "taux_rnet","autonomie","endettement","liquidite","vetuste"]] for b in batch])
        total_inserted += len(batch)

    # Index pour les requêtes benchmark
    log.info("Création des index…")
    con.execute("CREATE INDEX IF NOT EXISTS idx_naf ON bilans(naf)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_naf_annee ON bilans(naf, annee)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_siren ON bilans(siren)")

    # Vue pré-calculée des percentiles par NAF + tranche CA
    log.info("Création vue percentiles…")
    con.execute("""
        CREATE OR REPLACE VIEW percentiles AS
        SELECT
            naf,
            annee,
            CASE
                WHEN ca < 100000       THEN '0-100K'
                WHEN ca < 500000       THEN '100K-500K'
                WHEN ca < 2000000      THEN '500K-2M'
                WHEN ca < 10000000     THEN '2M-10M'
                WHEN ca < 50000000     THEN '10M-50M'
                ELSE                        '50M+'
            END AS tranche_ca,
            COUNT(*)                                AS nb,
            PERCENTILE_CONT(0.1)  WITHIN GROUP (ORDER BY taux_mb)    AS mb_q10,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY taux_mb)    AS mb_q25,
            PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY taux_mb)    AS mb_q50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY taux_mb)    AS mb_q75,
            PERCENTILE_CONT(0.9)  WITHIN GROUP (ORDER BY taux_mb)    AS mb_q90,
            PERCENTILE_CONT(0.1)  WITHIN GROUP (ORDER BY taux_ebe)   AS ebe_q10,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY taux_ebe)   AS ebe_q25,
            PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY taux_ebe)   AS ebe_q50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY taux_ebe)   AS ebe_q75,
            PERCENTILE_CONT(0.9)  WITHIN GROUP (ORDER BY taux_ebe)   AS ebe_q90,
            PERCENTILE_CONT(0.1)  WITHIN GROUP (ORDER BY taux_rnet)  AS rnet_q10,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY taux_rnet)  AS rnet_q25,
            PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY taux_rnet)  AS rnet_q50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY taux_rnet)  AS rnet_q75,
            PERCENTILE_CONT(0.9)  WITHIN GROUP (ORDER BY taux_rnet)  AS rnet_q90,
            PERCENTILE_CONT(0.1)  WITHIN GROUP (ORDER BY autonomie)  AS cp_q10,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY autonomie)  AS cp_q25,
            PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY autonomie)  AS cp_q50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY autonomie)  AS cp_q75,
            PERCENTILE_CONT(0.9)  WITHIN GROUP (ORDER BY autonomie)  AS cp_q90,
            PERCENTILE_CONT(0.1)  WITHIN GROUP (ORDER BY endettement) AS end_q10,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY endettement) AS end_q25,
            PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY endettement) AS end_q50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY endettement) AS end_q75,
            PERCENTILE_CONT(0.9)  WITHIN GROUP (ORDER BY endettement) AS end_q90,
            PERCENTILE_CONT(0.1)  WITHIN GROUP (ORDER BY liquidite)  AS liq_q10,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY liquidite)  AS liq_q25,
            PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY liquidite)  AS liq_q50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY liquidite)  AS liq_q75,
            PERCENTILE_CONT(0.9)  WITHIN GROUP (ORDER BY liquidite)  AS liq_q90
        FROM bilans
        WHERE taux_mb IS NOT NULL
          AND taux_ebe IS NOT NULL
          AND taux_rnet IS NOT NULL
        GROUP BY naf, annee, tranche_ca
        HAVING COUNT(*) >= 10
    """)

    con.close()
    size_mb = round(DB_PATH.stat().st_size / 1024 / 1024, 1)
    log.info(f"✅ DuckDB construit : {total_inserted} bilans · {size_mb} MB → {DB_PATH}")
    return total_inserted

# ── FTP Download ─────────────────────────────────────────────────────────────
def download_comptes_annuels():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ftp = connect_ftp()

    # Explorer l'arborescence
    log.info("Exploration du FTP…")
    all_files = list_ftp(ftp, "/")
    log.info(f"{len(all_files)} fichiers détectés")

    # Filtrer uniquement les comptes annuels JSON
    ca_files = [f for f in all_files if "comptes" in f.lower() or "bilan" in f.lower() or "annuel" in f.lower()]
    if not ca_files:
        ca_files = all_files  # Prendre tout si pas de sous-dossier évident
        log.info("Pas de dossier 'comptes' détecté — téléchargement de tous les fichiers JSON")

    log.info(f"Fichiers comptes annuels : {len(ca_files)}")

    downloaded = 0
    for remote_path in tqdm(ca_files, desc="Téléchargement FTP"):
        local_path = DATA_DIR / remote_path.lstrip("/")
        try:
            if download_file(ftp, remote_path, local_path):
                downloaded += 1
        except Exception as e:
            log.warning(f"Erreur {remote_path}: {e}")

    ftp.quit()
    log.info(f"✅ {downloaded} fichiers téléchargés dans {DATA_DIR}")

# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--explore",  action="store_true", help="Explorer le FTP sans télécharger")
    parser.add_argument("--download", action="store_true", help="Télécharger les fichiers FTP")
    parser.add_argument("--build-db", action="store_true", help="Construire le DuckDB depuis les JSON locaux")
    parser.add_argument("--full",     action="store_true", help="Tout enchaîner")
    args = parser.parse_args()

    if args.explore or args.full:
        ftp = connect_ftp()
        log.info("Arborescence FTP :")
        files = list_ftp(ftp, "/")
        for f in files[:50]:
            log.info(f"  {f}")
        if len(files) > 50:
            log.info(f"  … et {len(files)-50} autres")
        ftp.quit()

    if args.download or args.full:
        download_comptes_annuels()

    if args.build_db or args.full:
        build_db(DATA_DIR)

    if not any([args.explore, args.download, args.build_db, args.full]):
        # Mode GitHub Actions : tout enchaîner silencieusement
        download_comptes_annuels()
        build_db(DATA_DIR)
