/* map-security.js — security map logic (SPF + DMARC) for unified DACH view */

var SEC_COLOR_SCHEMES = {
  default: {
    'both':       '#5eecc8',
    'spf-only':   '#ffe08a',
    'dmarc-only': '#b8a0ff',
    'none':       '#ff9e9e',
    lake: '#89B3D6',
  },
  colorblind: {
    'both':       '#06b6d4',
    'spf-only':   '#fbbf24',
    'dmarc-only': '#c2410c',
    'none':       '#7c3aed',
    lake: '#c4afff',
  },
};
var SEC_COLORS = SEC_COLOR_SCHEMES.default;
var SEC_GRAY = '#BFBFBF';

function securityCategory(m) {
  if (!m || !m.scan_valid) return null;
  var dss = m.dss;
  if (!dss) return null;
  if (dss.has_good_spf && dss.has_good_dmarc) return 'both';
  if (dss.has_good_spf) return 'spf-only';
  if (dss.has_good_dmarc) return 'dmarc-only';
  return 'none';
}

function getSecurityColor(m) {
  var cat = securityCategory(m);
  if (!cat) return SEC_GRAY;
  return SEC_COLORS[cat];
}

function spfLabel(dss) {
  if (!dss) return { text: 'Missing', cls: 'sec-missing' };
  if (dss.has_good_spf) return { text: 'Good (hard fail)', cls: 'sec-good' };
  if (dss.has_spf) return { text: 'Present (not enforced)', cls: 'sec-partial' };
  return { text: 'Missing', cls: 'sec-missing' };
}

function dmarcLabel(dss) {
  if (!dss) return { text: 'Missing', cls: 'sec-missing' };
  if (dss.has_good_dmarc) return { text: 'Good (p=reject)', cls: 'sec-good' };
  if (dss.has_dmarc) return { text: 'Present (not enforced)', cls: 'sec-partial' };
  return { text: 'Missing', cls: 'sec-missing' };
}

function secLegendItem(label, colorKey, count) {
  return '<div class="legend-group" data-filter="' + colorKey + '">' +
    '<i class="legend-swatch" data-sec="' + colorKey + '" style="background:' + SEC_COLORS[colorKey] + '"></i>' +
    '<span>' + label + ' (' + count + ')</span>' +
    '</div>';
}

function loadSecurityMap(mapConfig, countries) {
  var map = initMap('map', mapConfig);
  setupInfoBar(map);
  var isMobile = window.innerWidth <= 600;

  window.toggleSection = function(el) {
    el.classList.toggle('popup-toggle-open');
    var body = el.nextElementSibling;
    body.style.display = body.style.display === 'none' ? '' : 'none';
  };

  var allMuniLayers = [];
  var allLakeLayers = [];
  var allMuniData = [];

  function toggleColorScheme() {
    var isDefault = SEC_COLORS === SEC_COLOR_SCHEMES.default;
    SEC_COLORS = isDefault ? SEC_COLOR_SCHEMES.colorblind : SEC_COLOR_SCHEMES.default;

    for (var i = 0; i < allMuniLayers.length; i++) {
      var cc = countries[i];
      var md = allMuniData[i];
      allMuniLayers[i].eachLayer(function (layer) {
        var code = cc.featureCodeFn(layer.feature);
        var m = md[code];
        layer.setStyle({ fillColor: getSecurityColor(m) });
      });
    }

    for (var i = 0; i < allLakeLayers.length; i++) {
      if (allLakeLayers[i]) allLakeLayers[i].setStyle({ fillColor: SEC_COLORS.lake });
    }

    document.querySelectorAll('.legend-swatch').forEach(function (el) {
      if (el.dataset.sec) el.style.background = SEC_COLORS[el.dataset.sec];
    });

    var btn = document.querySelector('.color-toggle');
    btn.textContent = isDefault ? '\u25D0 Default colors' : '\u25D0 Colorblind mode';
  }

  return fetchMultiCountryData(countries).then(function(results) {
    var catCounts = { 'both': 0, 'spf-only': 0, 'dmarc-only': 0, 'none': 0, 'no-data': 0 };

    var latestGenerated = null;
    var latestCommit = null;

    for (var ci = 0; ci < results.length; ci++) {
      var data = results[ci].data;
      var muni = data.municipalities;
      allMuniData.push(muni);

      if (data.generated && (!latestGenerated || data.generated > latestGenerated)) {
        latestGenerated = data.generated;
        latestCommit = data.commit;
      }

      var keys = Object.keys(muni);
      for (var i = 0; i < keys.length; i++) {
        var cat = securityCategory(muni[keys[i]]);
        if (cat) {
          catCounts[cat]++;
        } else {
          catCounts['no-data']++;
        }
      }
    }

    if (latestGenerated) {
      showGenerated({ generated: latestGenerated, commit: latestCommit });
    }

    var hatchSvg = 'data:image/svg+xml,' +
      encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="8" height="8"><rect width="8" height="8" fill="#BFBFBF"/><path d="M-1,1 l2,-2 M0,8 l8,-8 M7,9 l2,-2" stroke="#000" stroke-width="1"/></svg>');

    // Legend
    var legend = L.control({ position: isMobile ? 'topright' : 'bottomright' });
    legend.onAdd = function () {
      var div = L.DomUtil.create('div', 'legend');
      if (isMobile) div.classList.add('legend-collapsed');
      div.innerHTML =
        '<button class="legend-toggle" aria-label="Toggle legend" aria-expanded="' + (!isMobile) + '">' + (isMobile ? 'Legend \u25B8' : '') + '</button>' +
        '<div class="legend-content">' +
        '<strong>Email Security</strong><span class="legend-hint">click to filter</span>' +
        secLegendItem('Good SPF + Good DMARC', 'both', catCounts['both']) +
        secLegendItem('Good SPF only', 'spf-only', catCounts['spf-only']) +
        secLegendItem('Good DMARC only', 'dmarc-only', catCounts['dmarc-only']) +
        secLegendItem('Neither', 'none', catCounts['none']) +
        '<div class="legend-group" data-filter="no-data"><i style="background-image:url(\'' + hatchSvg + '\')"></i>No scan data (' + catCounts['no-data'] + ')</div>' +
        '<button class="color-toggle" aria-label="Switch to colorblind-safe colors">\u25D0 Colorblind mode</button>' +
        '</div>';
      L.DomEvent.disableClickPropagation(div);
      return div;
    };
    legend.addTo(map);
    document.querySelector('.legend-toggle').addEventListener('click', toggleLegend);
    document.querySelector('.color-toggle').addEventListener('click', toggleColorScheme);

    // Category filter toggles
    var hiddenCats = {};

    function applyFilters() {
      for (var i = 0; i < allMuniLayers.length; i++) {
        var cc = countries[i];
        var md = allMuniData[i];
        allMuniLayers[i].eachLayer(function (layer) {
          var code = cc.featureCodeFn(layer.feature);
          var m = md[code];
          var cat = securityCategory(m) || 'no-data';
          var visible = !hiddenCats[cat];
          layer.setStyle({ fillOpacity: visible ? 1 : 0, opacity: visible ? 1 : 0 });
        });
      }
    }

    document.querySelectorAll('.legend-group[data-filter]').forEach(function (el) {
      el.addEventListener('click', function () {
        var cat = el.dataset.filter;
        hiddenCats[cat] = !hiddenCats[cat];
        el.classList.toggle('legend-hidden', hiddenCats[cat]);
        applyFilters();
      });
    });

    // Render each country
    for (var ci = 0; ci < results.length; ci++) {
      var topo = results[ci].topo;
      var cc = countries[ci];
      var muni = allMuniData[ci];

      allLakeLayers.push(addLakes(map, topo, SEC_COLORS.lake));

      var geojson = topojson.feature(topo, topo.objects[cc.topoObject]);

      var muniLayer = (function(cc, muni, geojson) {
        return L.geoJSON(geojson, {
          style: function (feature) {
            var code = cc.featureCodeFn(feature);
            var m = muni[code];
            return {
              fillColor: getSecurityColor(m),
              weight: 0.6,
              color: '#666',
              fillOpacity: 1
            };
          },
          onEachFeature: function (feature, layer) {
            var code = cc.featureCodeFn(feature);
            var m = muni[code];
            if (!m) {
              layer.bindPopup('<div class="info-popup"><strong>ID ' + escapeHtml(code) + '</strong><br>No data</div>');
              return;
            }

            var regionCode = cc.REGION_CODES[m.region] || '';
            var nameDisplay = regionCode ? escapeHtml(m.name) + ' (' + regionCode + ')' : escapeHtml(m.name);
            var eDomain = escapeHtml(m.domain || 'unknown');
            var showBody = isMobile ? 'display:none' : '';

            var spf = spfLabel(m.dss);
            var dmarc = dmarcLabel(m.dss);

            var statusHtml =
              '<div class="sec-status">' +
              '<div class="sec-row"><span class="sec-label">SPF</span><span class="sec-badge ' + spf.cls + '">' + spf.text + '</span></div>' +
              '<div class="sec-row"><span class="sec-label">DMARC</span><span class="sec-badge ' + dmarc.cls + '">' + dmarc.text + '</span></div>' +
              '</div>';

            // MX section
            var mxHosts = m.mx_records || [];
            var mxSection;
            if (mxHosts.length > 0) {
              var mxRows = mxHosts.map(function(h) {
                return '<tr><td class="host">' + escapeHtml(h) + '</td></tr>';
              }).join('');
              mxSection = '<div class="popup-section"><div class="popup-toggle" onclick="window.toggleSection(this)">MX records</div><div class="popup-section-body" style="display:none"><table class="dns-table">' + mxRows + '</table></div></div>';
            } else {
              mxSection = '<div class="popup-section"><div class="popup-toggle" onclick="window.toggleSection(this)">MX records</div><div class="popup-section-body" style="display:none"><span class="popup-empty">No MX records</span></div></div>';
            }

            layer.bindPopup(
              '<div class="info-popup">' +
              '<strong>' + nameDisplay + '</strong><br>' +
              eDomain +
              statusHtml +
              mxSection +
              '</div>',
              { maxWidth: isMobile ? 300 : 400 }
            );
            layer.on('mouseover', function () { this.setStyle({ weight: 2, color: '#333' }); });
            layer.on('mouseout', function () { this.setStyle({ weight: 0.6, color: '#666' }); });
          }
        }).addTo(map);
      })(cc, muni, geojson);

      allMuniLayers.push(muniLayer);
    }

    // Country outlines on top
    for (var ci = 0; ci < results.length; ci++) {
      addCountryOutline(map, results[ci].topo, countries[ci].topoObject);
    }

    removeLoading();
    map.invalidateSize({ animate: false });
    addDownloadButton(map, 'security-map.png');
  });
}
