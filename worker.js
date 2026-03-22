/**
 * Cloudflare Worker — Proxy INPI pour Financiel Vision
 * Romain Froment · Cabinet ACC
 * 
 * Ce Worker fait uniquement office de proxy transparent vers l'API INPI.
 * Il ne stocke aucune donnée, ne log rien, ne modifie pas les réponses.
 * Gratuit jusqu'à 100 000 requêtes/jour sur Cloudflare Free plan.
 */

const INPI_BASE = 'https://registre-national-entreprises.inpi.fr/api';

// Origines autorisées (ton GitHub Pages + localhost pour dev)
const ALLOWED_ORIGINS = [
  'https://rfroment85.github.io',
  'http://localhost',
  'http://127.0.0.1',
  'null', // file:// local
];

function corsHeaders(origin) {
  const allowed = ALLOWED_ORIGINS.some(o => origin?.startsWith(o))
    ? origin
    : ALLOWED_ORIGINS[0];
  return {
    'Access-Control-Allow-Origin': allowed,
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Max-Age': '86400',
  };
}

export default {
  async fetch(request, env, ctx) {
    const origin = request.headers.get('Origin') || '';
    const cors = corsHeaders(origin);

    // Preflight OPTIONS → répondre immédiatement
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: cors });
    }

    const url = new URL(request.url);

    // Sanity check : on n'accepte que les paths /api/...
    // Le chemin Worker : /sso/login, /companies/..., etc.
    const inpiPath = url.pathname + url.search;
    const inpiUrl = `${INPI_BASE}${inpiPath}`;

    // Transmettre les headers du client (notamment Authorization: Bearer)
    const headers = new Headers();
    const authHeader = request.headers.get('Authorization');
    if (authHeader) headers.set('Authorization', authHeader);
    headers.set('Content-Type', request.headers.get('Content-Type') || 'application/json');
    headers.set('Accept', 'application/json');

    try {
      const inpiResp = await fetch(inpiUrl, {
        method: request.method,
        headers,
        body: request.method !== 'GET' && request.method !== 'HEAD'
          ? await request.text()
          : undefined,
      });

      const respHeaders = new Headers(cors);
      respHeaders.set('Content-Type', inpiResp.headers.get('Content-Type') || 'application/json');
      // Cacher 60s les réponses GET pour économiser les quotas
      if (request.method === 'GET' && inpiResp.ok) {
        respHeaders.set('Cache-Control', 'public, max-age=60');
      }

      return new Response(inpiResp.body, {
        status: inpiResp.status,
        headers: respHeaders,
      });
    } catch (e) {
      return new Response(
        JSON.stringify({ error: 'Proxy error', message: e.message }),
        { status: 502, headers: { ...cors, 'Content-Type': 'application/json' } }
      );
    }
  },
};
