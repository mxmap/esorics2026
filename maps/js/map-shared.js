/* map-shared.js — shared utilities for map pages */

function escapeHtml(str) {
  var el = document.createElement('span');
  el.textContent = str;
  return el.innerHTML;
}

function initMap(elementId, options) {
  if (!CSS.supports('height', '100dvh')) {
    document.body.style.height = window.innerHeight + 'px';
  }

  var map = L.map(elementId, {
    center: options.center,
    zoom: options.zoom,
    minZoom: options.minZoom || 5,
    maxZoom: options.maxZoom || 14,
    renderer: L.canvas()
  });

  L.tileLayer('https://{s}.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>',
    subdomains: 'abcd',
    maxZoom: 19
  }).addTo(map);

  L.tileLayer('https://{s}.basemaps.cartocdn.com/light_only_labels/{z}/{x}/{y}{r}.png', {
    subdomains: 'abcd',
    maxZoom: 19,
    pane: 'shadowPane'
  }).addTo(map);

  var resizeTimer;
  window.addEventListener('resize', function () {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(function () {
      map.invalidateSize({ animate: false });
    }, 100);
  });

  return map;
}

function setupInfoBar(map) {
  function toggleInfo() {
    var bar = document.getElementById('info-bar');
    var btn = document.getElementById('toggle-info');
    bar.classList.toggle('collapsed');
    var collapsed = bar.classList.contains('collapsed');
    btn.innerHTML = collapsed ? 'About \u25be' : 'About \u25b4';
    btn.setAttribute('aria-expanded', String(!collapsed));
  }

  document.getElementById('toggle-info').addEventListener('click', function () {
    toggleInfo();
    map.invalidateSize({ animate: false });
  });

  if (window.innerWidth <= 600) {
    document.getElementById('info-bar').classList.add('collapsed');
    var btn = document.getElementById('toggle-info');
    btn.innerHTML = 'About \u25be';
    btn.setAttribute('aria-expanded', 'false');
  }
}

function toggleLegend() {
  var legend = document.querySelector('.legend');
  var btn = legend.querySelector('.legend-toggle');
  legend.classList.toggle('legend-collapsed');
  var collapsed = legend.classList.contains('legend-collapsed');
  btn.textContent = collapsed ? 'Legend \u25B8' : '\u2715';
  btn.setAttribute('aria-expanded', String(!collapsed));
}

function showGenerated(data) {
  if (data.generated) {
    var date = new Date(data.generated);
    var text = 'Updated ' + date.toLocaleString('de-CH', { dateStyle: 'medium', timeStyle: 'short' });
    if (data.commit) {
      text += ' \u00b7 ' + data.commit;
    }
    document.getElementById('generated').textContent = text;
  }
}

function addLakes(map, topo, lakeColor) {
  if (topo.objects.lakes) {
    var lakes = topojson.feature(topo, topo.objects.lakes);
    return L.geoJSON(lakes, {
      interactive: false,
      style: { fillColor: lakeColor, fillOpacity: 1, weight: 0, color: 'transparent' }
    }).addTo(map);
  }
  return null;
}

function indexMunicipalities(data) {
  if (Array.isArray(data.municipalities)) {
    var indexed = {};
    data.municipalities.forEach(function (m) {
      indexed[String(m.code)] = m;
    });
    data.municipalities = indexed;
  }
  return data;
}

async function fetchMapData(topoUrl, dataUrl) {
  var responses = await Promise.all([
    fetch(topoUrl),
    fetch(dataUrl)
  ]);
  if (!responses[0].ok) throw new Error('Failed to fetch topology: ' + responses[0].status);
  if (!responses[1].ok) throw new Error('Failed to fetch provider data: ' + responses[1].status);
  var topo = await responses[0].json();
  var providerData = await responses[1].json();
  indexMunicipalities(providerData);
  return { topo: topo, providerData: providerData };
}

function removeLoading() {
  var loading = document.getElementById('map-loading');
  if (loading) loading.remove();
}

function handleLoadError(err) {
  console.error('Failed to load data:', err);
  var loading = document.getElementById('map-loading');
  if (loading) {
    loading.textContent = 'Failed to load map data. Please try again later.';
    loading.style.color = '#dc2626';
  }
}
