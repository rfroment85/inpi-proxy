/**
 * Cloudflare Worker — API Benchmark INPI pour Financiel Vision
 * 
 * Endpoints :
 *   GET /benchmark?naf=5610A&ca=800000     → percentiles Q10-Q90
 *   GET /health                             → status
 * 
 * Le DuckDB est hébergé sur GitHub Releases et mis en cache dans le KV.
 * Pour l'instant (avant que le DuckDB soit prêt) : fallback sur BCE dataset.
 */

const ALLOWED_ORIGINS = [
  'https://rfroment85.github.io',
  'http://localhost',
  'http://127.0.0.1',
  'null',
];

// URL du DuckDB sur GitHub Releases (à mettre à jour après le premier build)
const DUCKDB_URL = 'https://github.com/rfroment85/inpi-proxy/releases/download/db-latest/benchmark.duckdb';

// Fallback : BCE dataset (fonctionne déjà)
const BCE_BASE = 'https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/ratios_inpi_bce_sectors/records';

function cors(origin) {
  const ok = ALLOWED_ORIGINS.some(o => (origin||'').startsWith(o)) ? origin : ALLOWED_ORIGINS[0];
  return {
    'Access-Control-Allow-Origin': ok,
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '86400',
  };
}

async function getBenchmarkBCE(naf, ca) {
  /** Fallback : BCE dataset — fonctionne maintenant sans CORS */
  const prefix2 = naf.replace('.','').substring(0,2);

  // Probe format
  const probeResp = await fetch(
    `${BCE_BASE}?select=classe_naf&where=${encodeURIComponent(`classe_naf like '${prefix2}%'`)}&limit=100&group_by=classe_naf`
  );
  if (!probeResp.ok) return null;
  const probeData = await probeResp.json();
  const available = (probeData.results||[]).map(r=>r.classe_naf).filter(Boolean);

  // Match
  const nafFormats = [naf.replace('.',''), naf, naf.replace('.','').substring(0,4), prefix2];
  let matched = null;
  for (const fmt of nafFormats) {
    matched = available.find(n => n.toUpperCase() === fmt.toUpperCase());
    if (matched) break;
  }
  if (!matched) return { error: `NAF "${naf}" absent du dataset BCE`, available_prefix: available.slice(0,10) };

  // Récupérer les données
  const where = `classe_naf='${matched}'`;
  const resp = await fetch(`${BCE_BASE}?where=${encodeURIComponent(where)}&limit=200&order_by=annee+DESC`);
  if (!resp.ok) return null;
  const data = await resp.json();
  const records = data.results || [];
  if (!records.length) return null;

  // Prendre l'année la plus récente
  const annees = [...new Set(records.map(r=>r.annee).filter(Boolean))].sort().reverse();
  const anneeRef = annees[0];
  const latest = anneeRef ? records.filter(r=>r.annee===anneeRef) : records;
  const row = latest[0] || {};

  return {
    source: 'BCE',
    naf: matched,
    annee: anneeRef,
    nb: latest.length,
    ratios: {
      marge_brute:      extractPercs(row, 'marge_brute'),
      ebe:              extractPercs(row, 'ebe'),
      resultat_net:     extractPercs(row, 'resultat_net'),
      endettement:      extractPercs(row, 'taux_d_endettement'),
      liquidite:        extractPercs(row, 'ratio_de_liquidite'),
      vetuste:          extractPercs(row, 'ratio_de_vetuste'),
    }
  };
}

function extractPercs(row, field) {
  // Format wide : field_q10, field_q50… OU format long avec q10/q25/q50/q75/q90
  const q50 = row[`${field}_q50`] ?? row[field] ?? null;
  if (q50 === null) return null;
  return {
    q10: row[`${field}_q10`] ?? null,
    q25: row[`${field}_q25`] ?? null,
    q50,
    q75: row[`${field}_q75`] ?? null,
    q90: row[`${field}_q90`] ?? null,
  };
}

export default {
  async fetch(request, env, ctx) {
    const origin = request.headers.get('Origin') || '';
    const headers = { ...cors(origin), 'Content-Type': 'application/json' };

    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: cors(origin) });
    }

    const url = new URL(request.url);

    // ── Health check ──
    if (url.pathname === '/health') {
      return new Response(JSON.stringify({ status: 'ok', version: '2.0' }), { headers });
    }

    // ── Benchmark endpoint ──
    if (url.pathname === '/benchmark') {
      const naf = url.searchParams.get('naf');
      const ca  = url.searchParams.get('ca');
      if (!naf) {
        return new Response(JSON.stringify({ error: 'Paramètre naf requis' }), { status: 400, headers });
      }

      try {
        // Pour l'instant : BCE fallback (DuckDB sera activé après le premier build GitHub Actions)
        const result = await getBenchmarkBCE(naf, ca ? parseFloat(ca) : null);
        if (!result) {
          return new Response(JSON.stringify({ error: `Pas de données pour NAF "${naf}"` }), { status: 404, headers });
        }
        return new Response(JSON.stringify(result), { headers: { ...headers, 'Cache-Control': 'public, max-age=3600' } });
      } catch(e) {
        return new Response(JSON.stringify({ error: e.message }), { status: 500, headers });
      }
    }

    // ── Proxy INPI (ancien mode) ──
    const INPI_BASE = 'https://registre-national-entreprises.inpi.fr/api';
    const inpiPath = url.pathname + url.search;
    const inpiUrl = `${INPI_BASE}${inpiPath}`;
    const reqHeaders = new Headers();
    const auth = request.headers.get('Authorization');
    if (auth) reqHeaders.set('Authorization', auth);
    reqHeaders.set('Content-Type', 'application/json');

    try {
      const r = await fetch(inpiUrl, {
        method: request.method,
        headers: reqHeaders,
        body: ['GET','HEAD'].includes(request.method) ? undefined : await request.text(),
      });
      const respHeaders = new Headers(cors(origin));
      respHeaders.set('Content-Type', r.headers.get('Content-Type') || 'application/json');
      return new Response(r.body, { status: r.status, headers: respHeaders });
    } catch(e) {
      return new Response(JSON.stringify({ error: e.message }), { status: 502, headers });
    }
  }
};
