'use strict';

const axios = require('axios');

module.exports = function makeEnvioMasivoServices({ cfg, core }) {
  const {
    randomInt,
    dbg,
    warn,
    pretty,
    logHttpFailure
  } = core;

  // ==================== STATUS REPORT ====================
  async function postStatus(payload) {
    try {
      await axios.post(cfg.statusEndpoint, payload, {
        timeout: 15000,
        headers: { 'Content-Type': 'application/json' }
      });
    } catch (e) {
      logHttpFailure('postStatus', e);
    }
  }

  // ==================== VARIABLES GLOBALES (MIX: ##NAME## y #ID#) ====================
  //
  // 1) ##NOMBRE##  -> trae mapa completo una vez (si el endpoint lo soporta).
  // 2) #12#        -> modo "primer archivo": POST {id:12} y devuelve texto "a,b,c" -> random.
  //
  // Se mantiene TODO lo que ya tenías, y se agrega el motor #ID# sin romper nada.

  // Tokens tipo ##NAME##
  const TOKEN_REGEX = /##([a-zA-Z0-9_]+)##/g;

  // Tokens tipo #ID# (primer archivo)
  const HASH_ID_TOKEN_REGEX = /#(\d+)#/g;

  function pickValue(val) {
    if (Array.isArray(val)) {
      if (!val.length) return '';
      return val[randomInt(0, val.length)] ?? '';
    }
    if (val === null || val === undefined) return '';
    return String(val);
  }

  function normalizeVarsMap(varsResp) {
    // Soporta varias formas: {variables:{...}} o {data:{...}} o directamente {...}
    const vars = varsResp?.variables || varsResp?.data || varsResp || {};
    if (!vars || typeof vars !== 'object') return {};
    return vars;
  }

  async function fetchVariablesGlobalesMap(campana = null) {
    // Intentamos traer "todo el mapa" (para ##NAME##).
    // Si tu endpoint no acepta nada, igual mandamos {}. Si acepta iduser/empresa, lo agregamos sin romper.
    const body = {};
    try {
      if (campana && typeof campana === 'object') {
        if (campana.iduser != null) body.iduser = campana.iduser;
        if (campana.empresa != null) body.empresa = campana.empresa;
      }
    } catch {}

    try {
      dbg('POST variables_globales (MAP) → url:', cfg.varsApiEndpoint, 'body:', pretty(body));
      const r = await axios.post(cfg.varsApiEndpoint, body, {
        timeout: 15000,
        headers: { 'Content-Type': 'application/json' },
        validateStatus: () => true
      });
      dbg('POST variables_globales (MAP) ← status:', r.status, 'resp:', pretty(r.data, 1200));

      if (r.status >= 200 && r.status < 300) return r.data;
      warn('fetchVariablesGlobalesMap HTTP:', r.status);
      return null;
    } catch (e) {
      logHttpFailure('variables_globales_map', e);
      return null;
    }
  }

  function extractTextoFromResponse(data) {
    // Primer archivo esperaba "texto" o data.texto o data.data.texto
    const t1 = (typeof data?.texto === 'string') ? data.texto : null;
    const t2 = (typeof data?.data?.texto === 'string') ? data.data.texto : null;
    const t3 = (typeof data?.variables?.texto === 'string') ? data.variables.texto : null;
    return t1 ?? t2 ?? t3 ?? '';
  }

  function splitAlternatives(texto) {
    return String(texto || '')
      .split(',')
      .map(s => s.trim())
      .filter(s => s.length > 0);
  }

  async function fetchVarAlternativesById(id, campana = null) {
    // Implementación "primer archivo": POST {id} y leer texto con alternativas separadas por comas.
    const url = cfg.varsApiEndpoint;

    const basePayload = { id: String(id) };
    try {
      if (campana && typeof campana === 'object') {
        if (campana.iduser != null) basePayload.iduser = campana.iduser;
        if (campana.empresa != null) basePayload.empresa = campana.empresa;
      }
    } catch {}

    // Intento 1: JSON
    try {
      dbg('POST variables_globales (#ID# JSON) → url:', url, 'body:', pretty(basePayload));
      const resp = await axios.post(url, basePayload, {
        timeout: 15000,
        validateStatus: () => true,
        headers: { 'Content-Type': 'application/json' }
      });
      dbg('POST variables_globales (#ID# JSON) ← status:', resp.status, 'resp:', pretty(resp.data, 1200));

      if (resp.status >= 200 && resp.status < 300) {
        const texto = extractTextoFromResponse(resp.data);
        const arr = splitAlternatives(texto);
        dbg(`fetchVarAlternativesById #${id}# (JSON): opciones=${arr.length}`, arr);
        return arr;
      }
      warn(`fetchVarAlternativesById(JSON): HTTP ${resp.status}`);
    } catch (e) {
      logHttpFailure('fetchVarAlternativesById(JSON)', e);
    }

    // Intento 2: FORM (PHP friendly)
    try {
      const params = new URLSearchParams();
      params.append('id', String(id));
      if (basePayload.iduser != null) params.append('iduser', String(basePayload.iduser));
      if (basePayload.empresa != null) params.append('empresa', String(basePayload.empresa));

      dbg('POST variables_globales (#ID# FORM) → url:', url, 'body:', pretty(params.toString(), 1200));
      const resp = await axios.post(url, params, {
        timeout: 15000,
        validateStatus: () => true,
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
      });
      dbg('POST variables_globales (#ID# FORM) ← status:', resp.status, 'resp:', pretty(resp.data, 1200));

      if (resp.status >= 200 && resp.status < 300) {
        const texto = extractTextoFromResponse(resp.data);
        const arr = splitAlternatives(texto);
        dbg(`fetchVarAlternativesById #${id}# (FORM): opciones=${arr.length}`, arr);
        return arr;
      }
      warn(`fetchVarAlternativesById(FORM): HTTP ${resp.status}`);
    } catch (e) {
      logHttpFailure('fetchVarAlternativesById(FORM)', e);
    }

    warn(`No se pudo obtener texto para #${id}# via POST en ${url}`);
    return [];
  }

  function makeTokenReplacerFromMap(varsMap) {
    const map = new Map();
    if (varsMap && typeof varsMap === 'object') {
      for (const [k, v] of Object.entries(varsMap)) {
        if (!k) continue;
        map.set(k, v);
      }
    }

    return {
      replaceDoubleHashTokens(text) {
        if (!text) return text;
        return String(text).replace(TOKEN_REGEX, (_m, token) => {
          if (!map.has(token)) return '';
          return pickValue(map.get(token));
        });
      }
    };
  }

  async function prepareTokenReplacer(message, campana = null) {
    // ✅ Replacer COMBINADO:
    // - Primero reemplaza ##NAME##
    // - Luego reemplaza #ID# (primer archivo)
    //
    // Con cache en memoria para que #ID# no pegue al endpoint 1 vez por destinatario.

    // 1) Detectar tokens ##NAME##
    const hasDoubleHash = TOKEN_REGEX.test(String(message || ''));
    TOKEN_REGEX.lastIndex = 0;

    // 2) Detectar tokens #ID#
    const ids = Array.from(
      new Set([...String(message || '').matchAll(HASH_ID_TOKEN_REGEX)].map(m => m[1]))
    );

    dbg('prepareTokenReplacer: tokens detectados', {
      doubleHash: hasDoubleHash,
      hashIds: ids
    });

    // Cache para #ID#
    const hashIdCache = new Map();

    // Traer mapa (si hace falta)
    let mapVars = {};
    try {
      if (hasDoubleHash) {
        const varsResp = await fetchVariablesGlobalesMap(campana);
        mapVars = normalizeVarsMap(varsResp);
      }
    } catch (e) {
      warn('prepareTokenReplacer: fetch mapa falló, continuo sin mapa:', e?.message || e);
      mapVars = {};
    }

    const mapReplacer = makeTokenReplacerFromMap(mapVars);

    // Pre-cargar #ID# (si hay)
    try {
      for (const id of ids) {
        const opts = await fetchVarAlternativesById(id, campana);
        hashIdCache.set(id, opts);
      }
    } catch (e) {
      warn('prepareTokenReplacer: precarga #ID# falló:', e?.message || e);
    }

    function replaceHashIdTokens(text) {
      if (!text) return text;
      return String(text).replace(HASH_ID_TOKEN_REGEX, (_full, id) => {
        const opts = hashIdCache.get(String(id)) || [];
        if (!opts.length) return '';
        if (opts.length === 1) return String(opts[0] ?? '');
        return String(opts[randomInt(0, opts.length)] ?? '');
      });
    }

    return {
      replaceWithRandom(text) {
        if (!text) return text;

        // Orden recomendado:
        // 1) ##NAME## (mapa)
        // 2) #ID# (primer archivo)
        const a = mapReplacer.replaceDoubleHashTokens(text);
        const b = replaceHashIdTokens(a);

        // Validación liviana de preview (solo debug)
        dbg('Token replace preview:', String(b || '').slice(0, 120));
        return b;
      }
    };
  }

  // ========= Params dinámicos ##paramN## =========
  const paramTokenRegex = /##param(\d+)##/gi;

  function makeParamReplacerFromParts(parts) {
    return function replaceParams(msg) {
      if (!msg) return msg;
      return String(msg).replace(paramTokenRegex, (_m, nStr) => {
        const n = parseInt(nStr, 10);
        if (!Number.isFinite(n) || n < 1) return '';
        const val = (parts[n - 1] ?? '').toString(); // sin trim
        return val;
      });
    };
  }

  // ========= Pre-chequeo de sesión para en_tiempo_real =========
  async function preflightSessionOrError(modo, baseUrl, sessionId) {
    const isRealtime = String(modo).toLowerCase() === 'en_tiempo_real';
    if (!baseUrl || !sessionId) {
      return { ok: !isRealtime, reason: 'Faltan baseUrl/sessionId' };
    }
    try {
      const url = `${baseUrl}/check-session`;
      const body = { sessionId };
      dbg('POST check-session → url:', url, 'body:', pretty(body));
      const r = await axios.post(url, body, { timeout: 8000 });
      dbg('POST check-session ← status:', r.status, 'resp:', pretty(r.data));
      return { ok: true, data: r.data };
    } catch (e) {
      const st = e?.response?.status;
      const data = e?.response?.data;
      warn('POST check-session ERROR status:', st, 'resp:', pretty(data), 'msg:', e?.message);
      if (isRealtime && st === 404) {
        return { ok: false, reason: 'SesionNoEncontradaEnMemoria', server: data };
      }
      return { ok: !isRealtime, reason: `ErrorPreflight:${st || e.message}`, server: data };
    }
  }


  // ==================== WHATSAPP CHECK / REPORT EXISTENCIA ====================

  async function checkWhatsAppNumber(baseUrl, sessionId, numero) {
    // probamos primero tu endpoint real; y dejamos fallbacks por si cambias rutas
    const endpoints = [
      '/check-whatsapp',
      '/check-number',
      '/checkNumber',
      '/check_number',
      '/api/check-whatsapp',
      '/api/check-number'
    ];

    const body = { sessionId, number: numero };

    for (const ep of endpoints) {
      const url = `${baseUrl}${ep}`;
      try {
        dbg('POST check-whatsapp → url:', url, 'body:', pretty(body));
        const r = await axios.post(url, body, {
          timeout: 15000,
          headers: { 'Content-Type': 'application/json' }
        });
        dbg('POST check-whatsapp ← status:', r.status, 'resp:', pretty(r.data));

        // Normalizamos respuesta
        let exists = null;
        if (typeof r.data?.exists === 'boolean') exists = r.data.exists;
        else if (typeof r.data === 'boolean') exists = r.data;
        else if (typeof r.data?.data?.exists === 'boolean') exists = r.data.data.exists;

        return { ok: true, exists, raw: r.data, used: ep };
      } catch (e) {
        const st = e?.response?.status;
        const data = e?.response?.data;
        const msg = e?.message || '';

        if (st === 404) {
          dbg('check endpoint 404 en', ep, '→ probando otro');
          continue;
        }

        warn('POST check-whatsapp ERROR', 'ep:', ep, 'status:', st, 'resp:', pretty(data), 'msg:', msg);
        return { ok: false, exists: null, error: msg, raw: data, used: ep };
      }
    }

    return { ok: false, exists: null, error: 'No existe endpoint check-whatsapp en el servidor', raw: null, used: null };
  }

  async function reportExistencia({
    tipo = 'check',         // 'check' | 'send'
    sessionId = '',
    numero = '',
    exists = null,          // bool|null
    sent = null,            // bool|null (para reporte de envío)
    motivo = '',            // texto explicativo
    extra = {}              // payload extra
  }) {
    try {
      // ✅ TU API pide JSON (NO x-www-form-urlencoded)
      const payload = {
        tipo,
        sessionId,
        numero,
        exists,
        sent,
        motivo,
        ...extra
      };

      dbg('POST existencia → url:', cfg.existenciaEndpoint, 'body:', pretty(payload, 1200));

      const r = await axios.post(cfg.existenciaEndpoint, payload, {
        timeout: 15000,
        headers: { 'Content-Type': 'application/json' },
        validateStatus: () => true
      });

      dbg('POST existencia ← status:', r.status, 'resp:', pretty(r.data, 1200));

      if (!(r.status >= 200 && r.status < 300)) {
        warn('existencia HTTP no-2xx:', r.status, pretty(r.data, 400));
      }
    } catch (e) {
      logHttpFailure('reportExistencia', e);
    }
  }

  return {
    postStatus,
    prepareTokenReplacer,
    makeParamReplacerFromParts,
    preflightSessionOrError,
    checkWhatsAppNumber,
    reportExistencia
  };
};