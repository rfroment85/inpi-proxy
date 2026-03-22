# INPI Proxy — Financiel Vision

Cloudflare Worker servant de proxy transparent vers l'API INPI pour Financiel Vision.

## Déploiement (5 minutes)

### 1. Créer un compte Cloudflare gratuit
→ https://dash.cloudflare.com/sign-up

### 2. Installer Wrangler (CLI Cloudflare)
```bash
npm install -g wrangler
```

### 3. Se connecter à Cloudflare
```bash
wrangler login
# Ouvre le navigateur → autoriser
```

### 4. Déployer le Worker
```bash
cd inpi-proxy
npm install
npm run deploy
```

→ URL obtenue : `https://inpi-proxy.TON-SOUS-DOMAINE.workers.dev`

### 5. Mettre à jour Financiel Vision
Dans le dashboard, onglet Benchmark → champ "URL du proxy" → coller l'URL du Worker.

## Architecture

```
Navigateur (GitHub Pages)
    ↓ fetch avec Bearer token INPI
Cloudflare Worker (inpi-proxy.xxx.workers.dev)
    ↓ proxy transparent
API INPI (registre-national-entreprises.inpi.fr)
    ↓ réponse JSON
Cloudflare Worker
    ↓ + headers CORS
Navigateur → calcul percentiles local
```

## Sécurité
- Le Worker ne stocke rien
- Les logs sont désactivés (observability: false)
- Le token INPI transite en HTTPS chiffré
- Seul rfroment85.github.io est autorisé en origin
- Pas de données clients transmises (seulement SIREN + requêtes INPI)

## Quotas
- Free plan : 100 000 requêtes/jour
- ~100 req/analyse × 10 analyses/jour = 1 000 req/jour → largement suffisant
