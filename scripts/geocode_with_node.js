#!/usr/bin/env node
// Geocode cleaned Olympic host cities using Nominatim (OpenStreetMap)
// Reads `data/olympic_host_cities_normalized_clean.csv` and `data/geocoded_hosts.json`,
// queries Nominatim for missing entries (1s between requests), and writes updated JSON
// and a CSV summary `data/geocoded_hosts.csv`.

const fs = require('fs');
const path = require('path');
const fetch = global.fetch || require('node-fetch');
const { normalizeCountry, normalizeCity, shouldSplitCity, getSplitCities } = require('./geocoding_mapping.js');

const INPUT_CSV = 'data/olympic_host_cities_normalized_clean.csv';
const CACHE_JSON = 'data/geocoded_hosts.json';
const OUT_CSV = 'data/geocoded_hosts.csv';

// USER_AGENT should identify your application and include a contact (email or URL).
// You can override by setting the USER_AGENT env var.
const USER_AGENT = process.env.USER_AGENT || 'MyOlympicsScraper/1.0 (you@example.com)';
// optional CONTACT_EMAIL used for Nominatim queries
const CONTACT_EMAIL = process.env.CONTACT_EMAIL || '';
// choose provider via GEOCODER env: 'photon' | 'mapsco' | 'nominatim'
const GEOCODER = (process.env.GEOCODER || 'photon').toLowerCase();

function parseLine(line) {
  const fields = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && i + 1 < line.length && line[i+1] === '"') {
        cur += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === ',' && !inQuotes) {
      fields.push(cur);
      cur = '';
      continue;
    }
    cur += ch;
  }
  fields.push(cur);
  return fields.map(s => s.trim());
}

function parseCSV(text) {
  const lines = text.split(/\r?\n/).filter(l => l.trim() !== '');
  if (lines.length === 0) return { headers: [], rows: [] };
  const headers = parseLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const parts = parseLine(lines[i]);
    const row = {};
    for (let j = 0; j < headers.length; j++) {
      row[headers[j]] = parts[j] === undefined ? '' : parts[j];
    }
    rows.push(row);
  }
  return { headers, rows };
}

function makeKey(row) {
  const y = (row.Year || '').trim();
  const c = (row.City || '').trim();
  const co = (row.Country || '').trim();
  return `${y}|${c}|${co}`;
}

function escapeCsvField(s) {
  if (s === null || s === undefined) return '';
  const str = String(s);
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

async function geocode() {
  if (!fs.existsSync(INPUT_CSV)) {
    console.error('Input CSV not found:', INPUT_CSV);
    process.exit(1);
  }

  const csvText = fs.readFileSync(INPUT_CSV, 'utf8');
  const { headers, rows } = parseCSV(csvText);

  let cache = {};
  if (fs.existsSync(CACHE_JSON)) {
    try {
      cache = JSON.parse(fs.readFileSync(CACHE_JSON, 'utf8'));
    } catch (e) {
      console.warn('Failed to parse existing cache, starting fresh:', e.message);
      cache = {};
    }
  }

  for (const r of rows) {
    const key = makeKey(r);
    const city = (r.City || '').trim();
    const country = (r.Country || '').trim();
    
    // Normalize city and country using keyword mappings
    let normCity = normalizeCity(city);
    let normCountry = normalizeCountry(country);
    const query = [normCity, normCountry].filter(Boolean).join(', ');

    // Skip if already successfully geocoded
    if (cache[key] && cache[key].status === 'ok') {
      continue;
    }

    const entry = {
      key: key,
      year: (r.Year || '').trim(),
      city: city,
      country: country,
      query: query,
      lat: null,
      lon: null,
      provider: 'nominatim',
      status: 'pending',
      timestamp: new Date().toISOString(),
      raw: null
    };

    if (!query) {
      entry.status = 'no_query';
      cache[key] = entry;
      continue;
    }

    console.log(`Geocoding ${key}: ${query}`);
    
    // Try main query; if combined city, also try split entries
    let bestResult = null;
    const queriesToTry = [{ city: normCity, country: normCountry }];
    
    if (shouldSplitCity(city)) {
      const splits = getSplitCities(city);
      queriesToTry.push(...splits);
      console.log(`  (will also try split cities for combined entry)`);
    }

    for (const queryPair of queriesToTry) {
      const tryCity = queryPair.city || normCity;
      const tryCountry = queryPair.country || normCountry;
      const tryQuery = [tryCity, tryCountry].filter(Boolean).join(', ');
      
      let url;
      if (GEOCODER === 'mapsco') {
        url = `https://geocode.maps.co/search?q=${encodeURIComponent(tryQuery)}&limit=1`;
      } else if (GEOCODER === 'nominatim') {
        const emailParam = CONTACT_EMAIL ? `&email=${encodeURIComponent(CONTACT_EMAIL)}` : '';
        url = `https://nominatim.openstreetmap.org/search?format=json&limit=1&q=${encodeURIComponent(tryQuery)}${emailParam}`;
      } else {
        // default: photon (Komoot) — a free OSM-based geocoder
        url = `https://photon.komoot.io/api/?q=${encodeURIComponent(tryQuery)}&limit=1`;
      }
      
      try {
        const res = await fetch(url, { headers: { 'User-Agent': USER_AGENT, 'Referer': 'https://example.com', 'Accept-Language': 'en' } });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        
        // Normalize photon result shape to nominatim-like for downstream handling
        if (GEOCODER === 'photon') {
          // photon returns {features:[{geometry:{coordinates:[lon,lat]}, properties:{...}}]}
          if (data && Array.isArray(data.features) && data.features.length > 0) {
            const f = data.features[0];
            bestResult = {
              lon: f.geometry.coordinates[0],
              lat: f.geometry.coordinates[1],
              display_name: f.properties.name || f.properties.city || f.properties.state || f.properties.country || '',
              properties: f.properties
            };
            break;  // found result, stop trying other queries
          }
        } else if (Array.isArray(data) && data.length > 0) {
          bestResult = data[0];
          break;  // found result
        }
      } catch (e) {
        // continue to next query attempt
      }
    }

    // Process best result if found
    if (bestResult) {
      const loc = bestResult;
      entry.lat = parseFloat(loc.lat);
      entry.lon = parseFloat(loc.lon);
      entry.status = 'ok';
      entry.raw = loc;
    } else {
      entry.status = 'not_found';
    }

    cache[key] = entry;
    // save incrementally
    try {
      fs.mkdirSync(path.dirname(CACHE_JSON), { recursive: true });
      fs.writeFileSync(CACHE_JSON, JSON.stringify(cache, null, 2), 'utf8');
    } catch (e) {
      console.warn('Failed to write cache:', e.message);
    }

    // polite delay
    await new Promise(r => setTimeout(r, 1000));
  }

  // final save
  fs.writeFileSync(CACHE_JSON, JSON.stringify(cache, null, 2), 'utf8');

  // write CSV summary
  const keys = Object.keys(cache);
  const outHeaders = ['key','year','city','country','query','lat','lon','provider','status','timestamp'];
  const lines = [outHeaders.join(',')];
  for (const k of keys) {
    const it = cache[k];
    const vals = outHeaders.map(h => escapeCsvField(it[h] === undefined || it[h] === null ? '' : it[h]));
    lines.push(vals.join(','));
  }
  fs.writeFileSync(OUT_CSV, lines.join('\n'), 'utf8');

  console.log(`Geocoding complete. Wrote ${keys.length} entries to ${CACHE_JSON} and ${OUT_CSV}`);
}

geocode().catch(e => {
  console.error('Fatal error:', e);
  process.exit(1);
});
