// Keyword mapping to normalize city and country names for better geocoding results
// Maps historic/problematic names to modern/geocodable equivalents

const COUNTRY_MAP = {
  // Historic country name changes
  'West Germany': 'Germany',
  'West Berlin': 'Berlin, Germany',
  'East Germany': 'Germany',
  'USSR': 'Russia',
  'Soviet Union': 'Russia',
  'Czechoslovakia': 'Czech Republic',
  'Yugoslavia': 'Serbia',
  'S008 VIII': 'France',  // scraping artifact from Summer Olympics table
  'S010 X': 'United States',
  'S011 XI': 'Germany',
  'S013': 'United Kingdom',
  'S014 XIV': 'United Kingdom',
  'S015 XV': 'Finland',
  'S016 XVI': 'Australia',
  'S017 XVII': 'Italy',
  'S018 XVIII': 'Japan',
  'S019 XIX': 'Mexico',
  'S020 XX': 'Germany',
  'S021 XXI': 'Canada',
  'S022 XXII': 'Russia',
  'S023 XXIII': 'United States',
  'S024 XXIV': 'South Korea',
  'S025 XXV': 'Spain',
  'S026 XXVI': 'United States',
  'S027 XXVII': 'Australia',
  'S028 XXVIII': 'Greece',
  'S029 XXIX': 'China',
  'S030 XXX': 'United Kingdom',
  'S031 XXXI': 'Brazil',
  'S032 XXXII': 'Japan',
  'S033 XXXIII': 'France',
  'S034 XXXIV': 'United States',
  'W001 I': 'France',
  'W002 II': 'Switzerland',
  'W003 III': 'United States',
  'W004 IV': 'Germany',
  'W005a': 'Japan',
  'W005c V': 'Switzerland',
  'W006 VI': 'Norway',
  'W007 VII': 'Italy',
  'W008 VIII': 'United States',
  'W009 IX': 'Austria',
  'W010 X': 'France',
  'W011 XI': 'Japan',
  'W012 XII': 'Austria',
  'W013 XIII': 'United States',
  'W014 XIV': 'Yugoslavia',
  'W015 XV': 'Canada',
  'W016 XVI': 'France',
  'W017 XVII': 'Norway',
  'W018 XVIII': 'Japan',
  'W019 XIX': 'United States',
  'W020 XX': 'Italy',
  'W021 XXI': 'Canada',
  'W022 XXII': 'Russia',
  'W023 XXIII': 'South Korea',
  'W024 XXIV': 'China',
  'W025 XXV': 'Italy',
  'W026 XXVI': 'France',
  'W027 XXVII': 'United States',
  'Japan Germany': 'Japan',  // 1940 mixed entry, use primary
  'Japan Finland': 'Japan',  // mixed, use primary
  'Australia Sweden': 'Australia',  // 1956 split, use primary
};

const CITY_MAP = {
  // City name normalizations
  'St. Louis': 'Saint Louis',
  'St. Moritz': 'Saint Moritz',
  'Sapporo Garmisch-Partenkirchen': 'Sapporo',  // split combined entries
  'Melbourne Stockholm': 'Melbourne',  // split combined entries
  'Tokyo Helsinki': 'Tokyo',  // split combined entries
  'Milan Cortina d\'Ampezzo': 'Milan',  // use primary
  'Squaw Valley': 'Payette, Idaho',  // renamed but geocoding hint
  'Lake Placid': 'Lake Placid, New York',  // add state for clarity
  'Salt Lake City': 'Salt Lake City, Utah',
  'Lillehammer': 'Lillehammer, Norway',
  'Sarajevo': 'Sarajevo, Bosnia',
  'Pyeongchang': 'Pyeongchang, South Korea',
};

// For combined city+country entries, split into separate queries
const SPLIT_CITIES = {
  'Sapporo Garmisch-Partenkirchen': [
    { city: 'Sapporo', country: 'Japan' },
    { city: 'Garmisch-Partenkirchen', country: 'Germany' }
  ],
  'Melbourne Stockholm': [
    { city: 'Melbourne', country: 'Australia' },
    { city: 'Stockholm', country: 'Sweden' }
  ],
  'Tokyo Helsinki': [
    { city: 'Tokyo', country: 'Japan' },
    { city: 'Helsinki', country: 'Finland' }
  ],
  'Milan Cortina d\'Ampezzo': [
    { city: 'Milan', country: 'Italy' },
    { city: 'Cortina d\'Ampezzo', country: 'Italy' }
  ],
};

function normalizeCountry(country) {
  if (!country) return '';
  const trimmed = country.trim();
  return COUNTRY_MAP[trimmed] || trimmed;
}

function normalizeCity(city) {
  if (!city) return '';
  const trimmed = city.trim();
  return CITY_MAP[trimmed] || trimmed;
}

function shouldSplitCity(city) {
  return city && city.trim() in SPLIT_CITIES;
}

function getSplitCities(city) {
  return SPLIT_CITIES[city.trim()] || [];
}

// Export for Node/CommonJS
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    COUNTRY_MAP,
    CITY_MAP,
    SPLIT_CITIES,
    normalizeCountry,
    normalizeCity,
    shouldSplitCity,
    getSplitCities
  };
}
