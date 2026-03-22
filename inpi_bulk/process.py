"""
INPI Bulk Processor — Cabinet ACC
"""
import os, sys, json, gzip, logging, argparse
from pathlib import Path
import ftputil
import duckdb
from tqdm import tqdm

FTP_HOST = "www.inpi.net"
FTP_USER = os.environ["INPI_FTP_USER"]
FTP_PASS = os.environ["INPI_FTP_PASS"]
DATA_DIR = Path("./inpi_bulk/raw")
DB_PATH  = Path("./inpi_bulk/benchmark.duckdb")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

def explore_ftp():
    """Explorer l'arborescence complète du FTP avec ftputil."""
    log.info(f"Connexion FTP {FTP_HOST}…")
    with ftputil.FTPHost(FTP_HOST, FTP_USER, FTP_PASS) as host:
        log.info("Connecté ✓")
        log.info("Arborescence FTP (3 niveaux) :")
        
        def walk_dir(path, depth=0):
            if depth > 3:
                return
            try:
                entries = host.listdir(path)
                for name in entries[:50]:  # max 50 par dossier
                    full = f"{path.rstrip('/')}/{name}"
                    indent = "  " * depth
                    try:
                        if host.path.isdir(full):
                            log.info(f"{indent}📁 {full}/")
                            walk_dir(full, depth+1)
                        else:
                            size = host.path.getsize(full)
                            log.info(f"{indent}📄 {full} ({round(size/1024)}KB)")
                    except Exception as e:
                        log.info(f"{indent}? {full} — {e}")
            except Exception as e:
                log.warning(f"Erreur listing {path}: {e}")
        
        walk_dir("/")

def download_all():
    """Télécharger tous les fichiers JSON/gz des comptes annuels."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log.info(f"Connexion FTP {FTP_HOST}…")
    
    with ftputil.FTPHost(FTP_HOST, FTP_USER, FTP_PASS) as host:
        log.info("Connecté ✓")
        
        # Collecter tous les chemins de fichiers JSON
        json_files = []
        def collect_files(path, depth=0):
            if depth > 6:
                return
            try:
                entries = host.listdir(path)
                for name in entries:
                    full = f"{path.rstrip('/')}/{name}"
                    try:
                        if host.path.isdir(full):
                            collect_files(full, depth+1)
                        elif any(name.endswith(ext) for ext in ['.json', '.json.gz', '.gz']):
                            json_files.append(full)
                    except:
                        pass
            except Exception as e:
                log.warning(f"Erreur {path}: {e}")
        
        log.info("Collecte des chemins de fichiers…")
        collect_files("/")
        log.info(f"{len(json_files)} fichiers JSON trouvés")
        
        # Télécharger
        downloaded = skipped = errors = 0
        for remote in tqdm(json_files, desc="Téléchargement"):
            local = DATA_DIR / remote.lstrip("/")
            if local.exists():
                skipped += 1
                continue
            local.parent.mkdir(parents=True, exist_ok=True)
            try:
                host.download(remote, str(local))
                downloaded += 1
            except Exception as e:
                log.debug(f"Erreur {remote}: {e}")
                errors += 1
    
    log.info(f"✅ {downloaded} téléchargés · {skipped} déjà présents · {errors} erreurs")

def safe_num(val):
    try:
        v = float(str(val).replace(",", ".").replace(" ", ""))
        return v if abs(v) < 1e12 else None
    except:
        return None

def parse_bilan(data):
    if not isinstance(data, dict):
        return None
    siren = data.get("siren") or data.get("identifiantSiren")
    naf   = data.get("codeNaf") or data.get("codeApe") or data.get("activitePrincipale")
    date_clo = str(data.get("dateClotureExercice") or data.get("dateCloture") or "")
    annee = date_clo[:4] if date_clo else None

    cr     = data.get("compteDeResultat") or data.get("compteResultat") or {}
    actif  = data.get("bilanActif") or data.get("actif") or {}
    passif = data.get("bilanPassif") or data.get("passif") or {}
    if not cr and not actif:
        cr = actif = passif = data

    def g(obj, *keys):
        for k in keys:
            v = obj.get(k)
            if v is not None:
                n = safe_num(v)
                if n is not None:
                    return n
        return None

    ca     = g(cr, "HN","218","chiffreAffairesNet","totalProduits")
    achats = g(cr, "HA","achatsMarchandises","achatsMatieres")
    rexpl  = g(cr, "HF","280","resultatExploitation")
    rnet   = g(cr, "HI","DI","370","resultatExercice","resultatNet")
    ebe_r  = g(cr, "EBE","exBrutExploitation")
    cp     = g(passif, "DA","capitauxPropres","totalCapitauxPropres")
    tp     = g(passif, "DL","totalPassif","total")
    dettes = g(passif, "DR","totalDettes")
    dct    = g(passif, "DV","dettesMoinsUnAn","dettesCT")
    acirc  = g(actif,  "BJ","totalActifCirculant","actifCirculant")
    immo_n = g(actif,  "BN","totalActifImmobilise")
    immo_b = g(actif,  "BX","totalBrut","actifBrut")
    immo_a = g(actif,  "BP","totalAmortissements")

    if not ca or ca <= 0 or not tp or tp <= 0:
        return None

    mb  = (ca - achats) if achats is not None else None
    ebe = ebe_r or (mb - g(cr,"HB","chargesExternes",0) - g(cr,"HX","chargesPersonnel",0) if mb else None)

    def r(num, den): return round(num/den*100, 4) if num is not None and den and den != 0 else None

    return {
        "siren": siren, "naf": naf, "annee": annee, "ca": round(ca),
        "mb": round(mb) if mb else None,
        "ebe": round(ebe) if ebe else None,
        "rexpl": round(rexpl) if rexpl else None,
        "rnet": round(rnet) if rnet else None,
        "total_passif": round(tp),
        "cp": round(cp) if cp else None,
        "dettes_tot": round(dettes) if dettes else None,
        "dettes_ct": round(dct) if dct else None,
        "actif_circ": round(acirc) if acirc else None,
        "immo_net": round(immo_n) if immo_n else None,
        "immo_brut": round(immo_b) if immo_b else None,
        "taux_mb":    r(mb, ca),
        "taux_ebe":   r(ebe, ca),
        "taux_rexpl": r(rexpl, ca),
        "taux_rnet":  r(rnet, ca),
        "autonomie":  r(cp, tp),
        "endettement": r(dettes, tp),
        "liquidite": round(acirc/dct, 4) if acirc and dct and dct > 0 else None,
        "vetuste": r(immo_a or (immo_b-immo_n if immo_b and immo_n else None), immo_b) if immo_b and immo_b > 0 else None,
    }

def read_json_file(path):
    try:
        opener = gzip.open if str(path).endswith(".gz") else open
        mode = "rt"
        with opener(path, mode, encoding="utf-8", errors="ignore") as f:
            data = json.load(f)
        if isinstance(data, list): return data
        if isinstance(data, dict):
            for k in ["bilans","comptes","data","results","items"]:
                if k in data and isinstance(data[k], list):
                    return data[k]
            return [data]
        return []
    except Exception as e:
        log.debug(f"Erreur {path}: {e}")
        return []

def build_db():
    log.info("Construction DuckDB…")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute("""
        CREATE TABLE IF NOT EXISTS bilans (
            siren VARCHAR, naf VARCHAR, annee VARCHAR,
            ca BIGINT, mb BIGINT, ebe BIGINT, rexpl BIGINT, rnet BIGINT,
            total_passif BIGINT, cp BIGINT, dettes_tot BIGINT, dettes_ct BIGINT,
            actif_circ BIGINT, immo_net BIGINT, immo_brut BIGINT,
            taux_mb DOUBLE, taux_ebe DOUBLE, taux_rexpl DOUBLE, taux_rnet DOUBLE,
            autonomie DOUBLE, endettement DOUBLE, liquidite DOUBLE, vetuste DOUBLE
        )""")

    files = list(DATA_DIR.rglob("*.json")) + list(DATA_DIR.rglob("*.gz"))
    log.info(f"{len(files)} fichiers à traiter")

    total = 0
    batch = []
    COLS = ["siren","naf","annee","ca","mb","ebe","rexpl","rnet","total_passif",
            "cp","dettes_tot","dettes_ct","actif_circ","immo_net","immo_brut",
            "taux_mb","taux_ebe","taux_rexpl","taux_rnet","autonomie","endettement","liquidite","vetuste"]

    for jf in tqdm(files, desc="Parsing"):
        for r in read_json_file(jf):
            p = parse_bilan(r)
            if p:
                batch.append([p.get(c) for c in COLS])
                if len(batch) >= 5000:
                    con.executemany(f"INSERT INTO bilans VALUES ({','.join(['?']*len(COLS))})", batch)
                    total += len(batch)
                    batch = []
    if batch:
        con.executemany(f"INSERT INTO bilans VALUES ({','.join(['?']*len(COLS))})", batch)
        total += len(batch)

    con.execute("CREATE INDEX IF NOT EXISTS idx_naf ON bilans(naf)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_naf_annee ON bilans(naf, annee)")
    con.execute("""CREATE OR REPLACE VIEW percentiles AS
        SELECT naf, annee,
            CASE WHEN ca<100000 THEN '0-100K' WHEN ca<500000 THEN '100K-500K'
                 WHEN ca<2000000 THEN '500K-2M' WHEN ca<10000000 THEN '2M-10M'
                 WHEN ca<50000000 THEN '10M-50M' ELSE '50M+' END AS tranche_ca,
            COUNT(*) AS nb,
            PERCENTILE_CONT(0.1)  WITHIN GROUP (ORDER BY taux_mb)  AS mb_q10,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY taux_mb)  AS mb_q25,
            PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY taux_mb)  AS mb_q50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY taux_mb)  AS mb_q75,
            PERCENTILE_CONT(0.9)  WITHIN GROUP (ORDER BY taux_mb)  AS mb_q90,
            PERCENTILE_CONT(0.1)  WITHIN GROUP (ORDER BY taux_ebe) AS ebe_q10,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY taux_ebe) AS ebe_q25,
            PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY taux_ebe) AS ebe_q50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY taux_ebe) AS ebe_q75,
            PERCENTILE_CONT(0.9)  WITHIN GROUP (ORDER BY taux_ebe) AS ebe_q90,
            PERCENTILE_CONT(0.1)  WITHIN GROUP (ORDER BY taux_rnet) AS rnet_q10,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY taux_rnet) AS rnet_q25,
            PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY taux_rnet) AS rnet_q50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY taux_rnet) AS rnet_q75,
            PERCENTILE_CONT(0.9)  WITHIN GROUP (ORDER BY taux_rnet) AS rnet_q90,
            PERCENTILE_CONT(0.1)  WITHIN GROUP (ORDER BY autonomie) AS cp_q10,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY autonomie) AS cp_q25,
            PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY autonomie) AS cp_q50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY autonomie) AS cp_q75,
            PERCENTILE_CONT(0.9)  WITHIN GROUP (ORDER BY autonomie) AS cp_q90,
            PERCENTILE_CONT(0.1)  WITHIN GROUP (ORDER BY endettement) AS end_q10,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY endettement) AS end_q25,
            PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY endettement) AS end_q50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY endettement) AS end_q75,
            PERCENTILE_CONT(0.9)  WITHIN GROUP (ORDER BY endettement) AS end_q90,
            PERCENTILE_CONT(0.1)  WITHIN GROUP (ORDER BY liquidite) AS liq_q10,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY liquidite) AS liq_q25,
            PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY liquidite) AS liq_q50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY liquidite) AS liq_q75,
            PERCENTILE_CONT(0.9)  WITHIN GROUP (ORDER BY liquidite) AS liq_q90
        FROM bilans WHERE taux_mb IS NOT NULL AND taux_ebe IS NOT NULL
        GROUP BY naf, annee, tranche_ca HAVING COUNT(*) >= 10""")

    con.close()
    size = round(DB_PATH.stat().st_size / 1024 / 1024, 1)
    log.info(f"✅ {total:,} bilans · {size} MB → {DB_PATH}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--explore",  action="store_true")
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--build-db", action="store_true")
    parser.add_argument("--full",     action="store_true")
    args = parser.parse_args()

    if args.explore:
        explore_ftp()
    if args.download or args.full:
        download_all()
    if args.build_db or args.full:
        build_db()
    if not any(vars(args).values()):
        # Mode GitHub Actions : tout enchaîner
        download_all()
        build_db()
