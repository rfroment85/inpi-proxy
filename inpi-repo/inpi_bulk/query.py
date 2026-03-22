"""
Requête benchmark : percentiles pour un NAF + tranche CA.
Utilisé par le Cloudflare Worker via l'API.

Exemple :
    python query.py --naf 5610A --ca 800000
"""
import duckdb, json, argparse
from pathlib import Path

DB_PATH = Path("./inpi_bulk/benchmark.duckdb")

def get_benchmark(naf, ca=None, annee=None):
    con = duckdb.connect(str(DB_PATH), read_only=True)

    # Tranche CA
    tranche = None
    if ca:
        ca = float(ca)
        if   ca < 100000:   tranche = "0-100K"
        elif ca < 500000:   tranche = "100K-500K"
        elif ca < 2000000:  tranche = "500K-2M"
        elif ca < 10000000: tranche = "2M-10M"
        elif ca < 50000000: tranche = "10M-50M"
        else:               tranche = "50M+"

    # NAF exact ou préfixe
    naf_variants = [naf, naf.replace(".",""), naf[:4], naf[:2]]

    result = None
    for naf_try in naf_variants:
        q = f"""
            SELECT * FROM percentiles
            WHERE naf LIKE '{naf_try}%'
            {'AND tranche_ca = ' + repr(tranche) if tranche else ''}
            {'AND annee = ' + repr(annee) if annee else ''}
            ORDER BY annee DESC, nb DESC
            LIMIT 1
        """
        try:
            row = con.execute(q).fetchdf()
            if len(row):
                result = row.to_dict(orient="records")[0]
                result["naf_matched"] = naf_try
                break
        except:
            pass

    # Si rien avec tranche → retenter sans
    if not result and tranche:
        for naf_try in naf_variants:
            q = f"""
                SELECT * FROM percentiles
                WHERE naf LIKE '{naf_try}%'
                ORDER BY annee DESC, nb DESC LIMIT 1
            """
            try:
                row = con.execute(q).fetchdf()
                if len(row):
                    result = row.to_dict(orient="records")[0]
                    result["naf_matched"] = naf_try
                    result["tranche_used"] = "toutes tailles"
                    break
            except:
                pass

    con.close()
    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--naf", required=True)
    parser.add_argument("--ca",  type=float)
    parser.add_argument("--annee")
    args = parser.parse_args()
    result = get_benchmark(args.naf, args.ca, args.annee)
    print(json.dumps(result, indent=2, default=str))
