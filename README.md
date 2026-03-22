# INPI Bulk — Benchmark Sectoriel Financiel Vision

Pipeline complet pour construire une base de benchmark sectoriel depuis les données INPI.

## Architecture

```
FTP INPI (comptes annuels JSON)
    ↓ GitHub Actions (mensuel)
process.py → DuckDB (benchmark.duckdb)
    ↓ GitHub Release (hébergement gratuit)
Cloudflare Worker /benchmark?naf=5610A&ca=800000
    ↓ JSON percentiles Q10-Q90
Financiel Vision (dashboard client)
```

## Setup en 3 étapes

### 1. Ajouter les secrets GitHub
Dans ton repo GitHub → Settings → Secrets → Actions :
- `INPI_FTP_USER` = ton login FTP INPI
- `INPI_FTP_PASS` = ton mot de passe FTP INPI

### 2. Mettre les fichiers dans ton repo
```
ton-repo/
├── .github/workflows/inpi-bulk.yml
├── inpi_bulk/
│   ├── process.py
│   ├── stats.py
│   └── query.py
└── worker.js  (remplace l'ancien)
```

### 3. Lancer le premier build
GitHub → Actions → "INPI Bulk" → Run workflow

La première fois prend 2-6h selon le volume du FTP.
Ensuite : automatique le 1er de chaque mois.

## Requête benchmark

```
GET https://inpi-proxy.inpi-proxy-acc.workers.dev/benchmark?naf=5610A&ca=800000

{
  "source": "INPI_BULK",
  "naf": "5610A",
  "annee": "2023",
  "nb": 4821,
  "ratios": {
    "marge_brute": { "q10": 12.3, "q25": 18.1, "q50": 28.4, "q75": 42.1, "q90": 61.2 },
    "ebe":         { "q10": 2.1,  "q25": 5.8,  "q50": 10.2, "q75": 16.4, "q90": 24.1 },
    ...
  }
}
```

## Fallback BCE
Tant que le DuckDB n'est pas encore buildé, le Worker utilise automatiquement
le dataset BCE (data.economie.gouv.fr) — gratuit, sans auth, CORS natif.
