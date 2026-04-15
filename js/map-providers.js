/* map-providers.js — shared provider map logic for CH / AT / DE */

var COLOR_SCHEMES = {
  default: {
    'us-cloud':  { high: '#ffa199', medium: '#ffccb6', low: '#cccccc' },
    'domestic':  { high: '#88faaa', medium: '#daffc2', low: '#cccccc' },
    'foreign':   { high: '#f5c842', medium: '#fde7c4', low: '#cccccc' },
    lake: '#89B3D6',
  },
  colorblind: {
    'us-cloud':  { high: '#f1be61', medium: '#fde7c4', low: '#cccccc' },
    'domestic':  { high: '#6fa2d0', medium: '#C0D8E8', low: '#cccccc' },
    'foreign':   { high: '#e8916d', medium: '#f5c9b5', low: '#cccccc' },
    lake: '#c4afff',
  },
};
var CATEGORY_COLORS = COLOR_SCHEMES.default;
var GRAY = '#BFBFBF';

var LABELS = {
  microsoft: 'Microsoft 365',
  google: 'Google Workspace',
  aws: 'Amazon Web Services',
  domestic: 'Domestic Provider',
  foreign: 'Foreign Provider',
  unknown: 'Unknown',
};

var SIGNAL_LABELS = {
  mx: 'MX', spf: 'SPF', dkim: 'DKIM', asn: 'ASN',
  tenant: 'Tenant', spf_ip: 'SPF IP', txt_verification: 'TXT',
  autodiscover: 'Autodisc.', dmarc: 'DMARC', cname_chain: 'CNAME', smtp: 'SMTP'
};

function effectiveCategory(m, domesticCategory) {
  if (!m || !m.category || m.category === 'unknown') return null;
  if (m.category === domesticCategory) return 'domestic';
  if (m.category === 'us-cloud') return 'us-cloud';
  if (m.category === 'foreign') return 'foreign';
  return null;
}

function confidenceLevel(confidence) {
  if (confidence >= 75) return 'high';
  if (confidence >= 50) return 'medium';
  if (confidence >= 25) return 'low';
  return 'insufficient';
}

function getColor(m, domesticCategory) {
  var cat = effectiveCategory(m, domesticCategory);
  if (!cat) return GRAY;
  var level = confidenceLevel(m.classification_confidence || 0);
  if (level === 'insufficient') return GRAY;
  return CATEGORY_COLORS[cat][level];
}

function classifyHost(host, domesticTLDs) {
  var h = host.toLowerCase();
  // Hyperscalers
  if (['mail.protection.outlook.com','outlook.com','microsoft','office365','onmicrosoft','spf.protection.outlook.com','sharepointonline'].some(function(k) { return h.includes(k); }))
    return { name: 'Microsoft', category: 'hyperscaler' };
  if (['google','googlemail','gmail','aspmx.l.google.com','_spf.google.com'].some(function(k) { return h.includes(k); }))
    return { name: 'Google', category: 'hyperscaler' };
  if (['amazonaws','amazonses','awsdns'].some(function(k) { return h.includes(k); }))
    return { name: 'AWS', category: 'hyperscaler' };
  // Foreign senders / services
  if (['mandrillapp.com','mandrill','mcsv.net'].some(function(k) { return h.includes(k); }))
    return { name: 'Mailchimp', category: 'foreign' };
  if (h.includes('sendgrid')) return { name: 'SendGrid', category: 'foreign' };
  if (h.includes('mailjet')) return { name: 'Mailjet', category: 'foreign' };
  if (h.includes('mailgun')) return { name: 'Mailgun', category: 'foreign' };
  if (['sendinblue','brevo'].some(function(k) { return h.includes(k); }))
    return { name: 'Brevo', category: 'foreign' };
  if (h.includes('mailchannels')) return { name: 'MailChannels', category: 'foreign' };
  if (h.includes('smtp2go')) return { name: 'SMTP2GO', category: 'foreign' };
  if (h.includes('nl2go')) return { name: 'Newsletter2Go', category: 'foreign' };
  if (h.includes('hubspotemail')) return { name: 'HubSpot', category: 'foreign' };
  if (h.includes('knowbe4')) return { name: 'KnowBe4', category: 'foreign' };
  if (['sophos','sophosxl'].some(function(k) { return h.includes(k); }))
    return { name: 'Sophos', category: 'foreign' };
  if (h.includes('trendmicro')) return { name: 'Trend Micro', category: 'foreign' };
  if (['hornetsecurity','hornetdmarc'].some(function(k) { return h.includes(k); }))
    return { name: 'Hornetsecurity', category: 'foreign' };
  if (h.includes('barracudanetworks')) return { name: 'Barracuda', category: 'foreign' };
  if (h.includes('mlsend')) return { name: 'MailerLite', category: 'foreign' };
  if (h.includes('createsend')) return { name: 'Campaign Monitor', category: 'foreign' };
  if (h.includes('exclaimer')) return { name: 'Exclaimer', category: 'foreign' };
  if (h.includes('turbo-smtp')) return { name: 'turboSMTP', category: 'foreign' };
  if (h.includes('letsignit')) return { name: 'Letsignit', category: 'foreign' };
  if (h.includes('codetwo')) return { name: 'CodeTwo', category: 'foreign' };
  if (h.includes('freshservice')) return { name: 'Freshservice', category: 'foreign' };
  if (['ppe-hosted','pphosted'].some(function(k) { return h.includes(k); }))
    return { name: 'Proofpoint', category: 'foreign' };
  if (h.includes('vadesecure')) return { name: 'Vade Secure', category: 'foreign' };
  if (h.includes('fortimailcloud')) return { name: 'Fortinet FortiMail', category: 'foreign' };
  if (h.includes('spamtitan')) return { name: 'SpamTitan', category: 'foreign' };
  if (['spamexperts','antispamcloud'].some(function(k) { return h.includes(k); }))
    return { name: 'SpamExperts', category: 'foreign' };
  if (h.includes('iphmx')) return { name: 'Cisco IronPort', category: 'foreign' };
  if (h.includes('mailcontrol')) return { name: 'MailControl', category: 'foreign' };
  if (h.includes('mimecast')) return { name: 'Mimecast', category: 'foreign' };
  if (h.includes('messagelabs')) return { name: 'MessageLabs', category: 'foreign' };
  if (h.includes('mtaroutes')) return { name: 'MTARoutes', category: 'foreign' };
  if (h.includes('jimdo')) return { name: 'Jimdo', category: 'foreign' };
  if (h.includes('arsmtp')) return { name: 'Aruba', category: 'foreign' };
  if (h.includes('stackmail')) return { name: 'StackMail', category: 'foreign' };
  if (h.includes('umantis')) return { name: 'Haufe', category: 'foreign' };
  if (['emailsignatures365','signature365'].some(function(k) { return h.includes(k); }))
    return { name: 'Email Signatures 365', category: 'foreign' };
  if (h.includes('crsend')) return { name: 'CleverReach', category: 'foreign' };
  if (h.includes('mailersend')) return { name: 'MailerSend', category: 'foreign' };
  if (h.includes('emailsrvr')) return { name: 'Rackspace', category: 'foreign' };
  if (h.includes('edgepilot')) return { name: 'EdgePilot', category: 'foreign' };
  if (h.includes('appriver')) return { name: 'AppRiver', category: 'foreign' };
  if (h.includes('scnem')) return { name: 'Evalanche', category: 'foreign' };
  if (h.includes('rubicon.eu')) return { name: 'Rubicon', category: 'foreign' };
  // Known domestic providers (Swiss-specific with non-.ch TLDs)
  var KNOWN_DOMESTIC = [
    'abxsec.com','swiss-egov.cloud','sui-inter.net','ch-dns.net','cyon.net',
    'seppmail.cloud','swisscom.com','privasphere.com','tinext.com','cloudrexx.com',
    'mailomat.cloud','comp-sys.net','engadin.cloud','ti-informatique.com',
    'assolo.net','moresi.com','infomaniak',
    'naveum.services','spamvor.com','swisscenter.com','seabix.cloud',
    'swizzonic.email','tinext.net','hosttech.eu','mailpro.com','tizoo.com'
  ];
  if (KNOWN_DOMESTIC.some(function(d) { return h.endsWith(d) || h.endsWith('.' + d) || h.includes(d); }))
    return { name: h.replace(/^_/, '').split('.').slice(-2).join('.'), category: 'domestic' };
  // TLD-based domestic fallback
  var parts = host.replace(/^_/, '').split('.');
  var name = parts.length >= 2 ? parts.slice(-2).join('.') : host;
  if (domesticTLDs.some(function(tld) { return h.endsWith(tld); }))
    return { name: name, category: 'domestic' };
  return { name: name, category: 'unknown' };
}

function parseSpfDelegations(spf) {
  if (!spf) return [];
  var results = [];
  var includeRe = /include:([^\s]+)/g;
  var m;
  while ((m = includeRe.exec(spf)) !== null) results.push(m[1]);
  var redirectRe = /redirect=([^\s]+)/g;
  while ((m = redirectRe.exec(spf)) !== null) results.push(m[1]);
  return results;
}

function renderTable(hosts, domesticTLDs) {
  var rows = hosts.map(function(host) {
    var cls = classifyHost(host, domesticTLDs);
    var eName = escapeHtml(cls.name);
    var eCat = escapeHtml(cls.category);
    var eHost = escapeHtml(host);
    return '<tr><td class="dn">' + eName + '</td><td><span class="cat-badge ' + eCat + '">' + eCat + '</span></td><td class="host" title="' + eHost + '">' + eHost + '</td></tr>';
  }).join('');
  return '<table class="dns-table">' + rows + '</table>';
}

function deduplicateSignals(signals) {
  var seen = new Map();
  for (var i = 0; i < signals.length; i++) {
    var s = signals[i];
    var key = s.kind + '|' + s.detail;
    if (seen.has(key)) {
      seen.get(key).count++;
    } else {
      seen.set(key, { kind: s.kind, provider: s.provider, weight: s.weight, detail: s.detail, count: 1 });
    }
  }
  return Array.from(seen.values()).sort(function(a, b) { return (b.weight || 0) - (a.weight || 0); });
}

function condenseDetail(kind, detail) {
  var m;
  switch (kind) {
    case 'mx':
      m = detail.match(/^MX\s+(\S+)\s+matches\s+\S+$/);
      return m ? m[1] : detail;
    case 'spf':
      m = detail.match(/^SPF\s+include:(\S+)\s+matches\s+\S+$/);
      return m ? m[1] : detail;
    case 'dkim':
      m = detail.match(/^DKIM\s+(selector\d+)\._domainkey\.\S+\s+CNAME\s+\u2192\s+\S+\._domainkey\.(\S+)$/);
      return m ? m[1] + ' \u2192 ' + m[2] : detail;
    case 'asn':
      m = detail.match(/^ASN\s+(\d+)\s+matches\s+\S+$/);
      if (m) return 'AS' + m[1];
      m = detail.match(/^ASN\s+(\d+)\s+is Swiss ISP:\s+(.+)$/);
      if (m) return 'AS' + m[1] + ' (' + m[2] + ')';
      m = detail.match(/^ASN\s+(\d+)\s+registered in\s+\S+$/);
      if (m) return 'AS' + m[1];
      return detail;
    case 'tenant':
      m = detail.match(/^MS365 tenant detected:\s+(.+)$/);
      return m ? m[1] : detail;
    case 'autodiscover':
      m = detail.match(/^autodiscover\s+(CNAME|SRV)\s+\u2192\s+(.+)$/);
      if (m) return (m[1] === 'CNAME' ? '\u2192 ' : m[1] + ' \u2192 ') + m[2];
      return detail;
    case 'txt_verification':
      m = detail.match(/^TXT verification matches\s+(\S+)$/);
      if (m) return m[1];
      if (detail.includes('AWS SES')) return 'AWS SES';
      return detail;
    case 'spf_ip':
      m = detail.match(/ASN\s+(\d+)\s+is Swiss ISP:\s+(.+)$/);
      if (m) return 'AS' + m[1] + ' (' + m[2] + ')';
      m = detail.match(/ASN\s+(\d+)\s+matches\s+(\S+)$/);
      if (m) return 'AS' + m[1];
      m = detail.match(/ASN\s+(\d+)\s+registered in\s+\S+$/);
      if (m) return 'AS' + m[1];
      return detail;
    case 'dmarc':
      m = detail.match(/^DMARC record\s+(.+)$/);
      return m ? m[1] : detail;
    default:
      return detail;
  }
}

function legendCategoryHtml(label, catKey, count, levels) {
  var colors = CATEGORY_COLORS[catKey];
  return '<div class="legend-group" data-filter="' + catKey + '">' +
    '<span class="legend-group-label">' + label + ' (' + count + ')</span>' +
    '<i class="legend-swatch" data-cat="' + catKey + '" data-level="high" style="background:' + colors.high + '"></i>High confidence (' + levels.high + ')<br>' +
    '<i class="legend-swatch" data-cat="' + catKey + '" data-level="medium" style="background:' + colors.medium + '"></i>Medium (' + levels.medium + ')<br>' +
    '<i class="legend-swatch" data-cat="' + catKey + '" data-level="low" style="background:' + colors.low + '"></i>Low (' + levels.low + ')' +
    '</div>';
}

function loadProviderMap(config) {
  var map = initMap('map', config);
  setupInfoBar(map);
  var isMobile = window.innerWidth <= 600;

  window.toggleSection = function(el) {
    el.classList.toggle('popup-toggle-open');
    var body = el.nextElementSibling;
    body.style.display = body.style.display === 'none' ? '' : 'none';
  };

  var muni = {};
  var muniLayer;
  var lakeLayer;

  function toggleColorScheme() {
    var isDefault = CATEGORY_COLORS === COLOR_SCHEMES.default;
    CATEGORY_COLORS = isDefault ? COLOR_SCHEMES.colorblind : COLOR_SCHEMES.default;

    muniLayer.eachLayer(function (layer) {
      var code = config.featureCodeFn(layer.feature);
      var m = muni[code];
      layer.setStyle({ fillColor: getColor(m, config.domesticCategory) });
    });

    if (lakeLayer) {
      lakeLayer.setStyle({ fillColor: CATEGORY_COLORS.lake });
    }

    document.querySelectorAll('.legend-swatch').forEach(function (el) {
      el.style.background = CATEGORY_COLORS[el.dataset.cat][el.dataset.level];
    });

    var btn = document.querySelector('.color-toggle');
    btn.textContent = isDefault ? '\u25D0 Default colors' : '\u25D0 Colorblind mode';
  }

  return fetchMapData(config.topoUrl, config.dataUrl).then(function(result) {
    var topo = result.topo;
    var providerData = result.providerData;
    muni = providerData.municipalities;
    showGenerated(providerData);

    // Count by category and confidence level
    var catCounts = { 'us-cloud': 0, 'domestic': 0, 'foreign': 0, 'insufficient': 0 };
    var levelCounts = {
      'us-cloud': { high: 0, medium: 0, low: 0 },
      'domestic': { high: 0, medium: 0, low: 0 },
      'foreign':  { high: 0, medium: 0, low: 0 }
    };
    var keys = Object.keys(muni);
    for (var i = 0; i < keys.length; i++) {
      var m = muni[keys[i]];
      var cat = effectiveCategory(m, config.domesticCategory);
      if (cat) {
        catCounts[cat]++;
        var level = confidenceLevel(m.classification_confidence || 0);
        if (level !== 'insufficient') levelCounts[cat][level]++;
      } else {
        catCounts['insufficient']++;
      }
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
        '<strong>Email Jurisdiction</strong>' +
        legendCategoryHtml('US Cloud', 'us-cloud', catCounts['us-cloud'], levelCounts['us-cloud']) +
        legendCategoryHtml('Foreign', 'foreign', catCounts['foreign'], levelCounts['foreign']) +
        legendCategoryHtml(config.domesticLabel, 'domestic', catCounts['domestic'], levelCounts['domestic']) +
        '<div class="legend-group"><i style="background-image:url(\'' + hatchSvg + '\')"></i>Insufficient data (' + catCounts['insufficient'] + ')</div>' +
        '<button class="color-toggle" aria-label="Switch to colorblind-safe colors">\u25D0 Colorblind mode</button>' +
        '</div>';
      L.DomEvent.disableClickPropagation(div);
      return div;
    };
    legend.addTo(map);
    document.querySelector('.legend-toggle').addEventListener('click', toggleLegend);
    document.querySelector('.color-toggle').addEventListener('click', toggleColorScheme);

    // Lakes
    lakeLayer = addLakes(map, topo, CATEGORY_COLORS.lake);

    // Municipalities
    var geojson = topojson.feature(topo, topo.objects[config.topoObject]);

    muniLayer = L.geoJSON(geojson, {
      style: function (feature) {
        var code = config.featureCodeFn(feature);
        var m = muni[code];
        return {
          fillColor: getColor(m, config.domesticCategory),
          weight: 0.6,
          color: '#666',
          fillOpacity: 1
        };
      },
      onEachFeature: function (feature, layer) {
        var code = config.featureCodeFn(feature);
        var m = muni[code];
        if (!m) {
          layer.bindPopup('<div class="info-popup"><strong>ID ' + escapeHtml(code) + '</strong><br>No data</div>');
          return;
        }
        var color = getColor(m, config.domesticCategory);
        var label = LABELS[m.provider] || m.provider;

        var regionCode = config.REGION_CODES[m.region] || '';
        var nameDisplay = regionCode ? escapeHtml(m.name) + ' (' + regionCode + ')' : escapeHtml(m.name);
        var eDomain = escapeHtml(m.domain || 'unknown');
        var eLabel = escapeHtml(label);
        var badge = '<span class="provider-badge" style="background:' + color + ';color:#000">' + eLabel + '</span>';

        var confidence = m.classification_confidence != null ? m.classification_confidence : 0;
        var confPct = Math.round(confidence) + '%';
        var metaParts = ['Confidence: ' + confPct];
        if (m.gateway) metaParts.push('Gateway: ' + escapeHtml(m.gateway));
        var metaLine = '<div class="popup-meta">' + metaParts.join(' &middot; ') + '</div>';

        var showBody = isMobile ? 'display:none' : '';

        // MX section
        var mxSection;
        if (m.mx && m.mx.length > 0) {
          mxSection = '<div class="popup-section"><div class="popup-toggle' + (isMobile ? '' : ' popup-toggle-open') + '" onclick="window.toggleSection(this)">Incoming mail handled by <span class="popup-section-hint">(MX)</span></div><div class="popup-section-body" style="' + showBody + '">' + renderTable(m.mx, config.domesticTLDs) + '</div></div>';
        } else {
          mxSection = '<div class="popup-section"><div class="popup-toggle' + (isMobile ? '' : ' popup-toggle-open') + '" onclick="window.toggleSection(this)">Incoming mail handled by <span class="popup-section-hint">(MX)</span></div><div class="popup-section-body" style="' + showBody + '"><span class="popup-empty">No MX records found</span></div></div>';
        }

        // SPF section
        var spfDelegations = parseSpfDelegations(m.spf);
        var spfSection;
        if (spfDelegations.length > 0) {
          spfSection = '<div class="popup-section"><div class="popup-toggle' + (isMobile ? '' : ' popup-toggle-open') + '" onclick="window.toggleSection(this)">Authorized to send mail <span class="popup-section-hint">(SPF)</span></div><div class="popup-section-body" style="' + showBody + '">' + renderTable(spfDelegations, config.domesticTLDs) + '</div></div>';
        } else if (m.spf) {
          spfSection = '<div class="popup-section"><div class="popup-toggle' + (isMobile ? '' : ' popup-toggle-open') + '" onclick="window.toggleSection(this)">Authorized to send mail <span class="popup-section-hint">(SPF)</span></div><div class="popup-section-body" style="' + showBody + '"><span class="popup-empty">' + escapeHtml(m.spf) + '</span></div></div>';
        } else {
          spfSection = '<div class="popup-section"><div class="popup-toggle' + (isMobile ? '' : ' popup-toggle-open') + '" onclick="window.toggleSection(this)">Authorized to send mail <span class="popup-section-hint">(SPF)</span></div><div class="popup-section-body" style="' + showBody + '"><span class="popup-empty">No SPF record found</span></div></div>';
        }

        // Signals section
        var signalsSection;
        if (m.classification_signals && m.classification_signals.length > 0) {
          var deduped = deduplicateSignals(m.classification_signals);
          var uniqueCount = deduped.length;
          var signalItems = deduped.map(function(s) {
            var kindLabel = SIGNAL_LABELS[s.kind] || s.kind;
            var condensed = condenseDetail(s.kind, s.detail);
            var dup = s.count > 1 ? '<span class="signal-dup">\u00d7' + s.count + '</span>' : '';
            return '<div class="signal-item" title="' + escapeHtml(s.detail).replace(/"/g, '&quot;') + '"><span class="signal-kind">' + escapeHtml(kindLabel) + '</span><span class="signal-text">' + escapeHtml(condensed) + '</span>' + dup + '</div>';
          }).join('');
          signalsSection = '<div class="popup-section"><div class="popup-toggle" onclick="window.toggleSection(this)">Classification signals <span class="popup-signal-count" title="DNS and network evidence used to identify the email provider">' + uniqueCount + '</span></div><div class="popup-section-body" style="display:none"><div class="signal-list">' + signalItems + '</div></div></div>';
        } else {
          signalsSection = '<div class="popup-section"><div class="popup-toggle" onclick="window.toggleSection(this)">Classification signals</div><div class="popup-section-body" style="display:none"><span class="popup-empty">No classification signals</span></div></div>';
        }

        layer.bindPopup(
          '<div class="info-popup">' +
          '<strong>' + nameDisplay + '</strong><br>' +
          eDomain + ' ' + badge +
          metaLine +
          mxSection + spfSection + signalsSection +
          '</div>',
          { maxWidth: isMobile ? 300 : 450 }
        );
        layer.on('mouseover', function () { this.setStyle({ weight: 2, color: '#333' }); });
        layer.on('mouseout', function () { this.setStyle({ weight: 0.6, color: '#666' }); });
      }
    }).addTo(map);

    removeLoading();
    map.invalidateSize({ animate: false });
  });
}

/* --- Unified multi-country provider map --- */

function loadUnifiedProviderMap(mapConfig, countries) {
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
    var isDefault = CATEGORY_COLORS === COLOR_SCHEMES.default;
    CATEGORY_COLORS = isDefault ? COLOR_SCHEMES.colorblind : COLOR_SCHEMES.default;

    for (var i = 0; i < allMuniLayers.length; i++) {
      var cc = countries[i];
      var md = allMuniData[i];
      allMuniLayers[i].eachLayer(function (layer) {
        var code = cc.featureCodeFn(layer.feature);
        var m = md[code];
        layer.setStyle({ fillColor: getColor(m, cc.domesticCategory) });
      });
    }

    for (var i = 0; i < allLakeLayers.length; i++) {
      if (allLakeLayers[i]) allLakeLayers[i].setStyle({ fillColor: CATEGORY_COLORS.lake });
    }

    document.querySelectorAll('.legend-swatch').forEach(function (el) {
      el.style.background = CATEGORY_COLORS[el.dataset.cat][el.dataset.level];
    });

    var btn = document.querySelector('.color-toggle');
    btn.textContent = isDefault ? '\u25D0 Default colors' : '\u25D0 Colorblind mode';
  }

  return fetchMultiCountryData(countries).then(function(results) {
    // Aggregate counts across all countries
    var catCounts = { 'us-cloud': 0, 'domestic': 0, 'foreign': 0, 'insufficient': 0 };
    var levelCounts = {
      'us-cloud': { high: 0, medium: 0, low: 0 },
      'domestic': { high: 0, medium: 0, low: 0 },
      'foreign':  { high: 0, medium: 0, low: 0 }
    };

    var latestGenerated = null;
    var latestCommit = null;

    for (var ci = 0; ci < results.length; ci++) {
      var data = results[ci].data;
      var cc = countries[ci];
      var muni = data.municipalities;
      allMuniData.push(muni);

      if (data.generated && (!latestGenerated || data.generated > latestGenerated)) {
        latestGenerated = data.generated;
        latestCommit = data.commit;
      }

      var keys = Object.keys(muni);
      for (var i = 0; i < keys.length; i++) {
        var m = muni[keys[i]];
        var cat = effectiveCategory(m, cc.domesticCategory);
        if (cat) {
          catCounts[cat]++;
          var level = confidenceLevel(m.classification_confidence || 0);
          if (level !== 'insufficient') levelCounts[cat][level]++;
        } else {
          catCounts['insufficient']++;
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
        '<strong>Email Jurisdiction</strong><span class="legend-hint">click to filter</span>' +
        legendCategoryHtml('US Cloud', 'us-cloud', catCounts['us-cloud'], levelCounts['us-cloud']) +
        legendCategoryHtml('Foreign', 'foreign', catCounts['foreign'], levelCounts['foreign']) +
        legendCategoryHtml('Domestic', 'domestic', catCounts['domestic'], levelCounts['domestic']) +
        '<div class="legend-group" data-filter="insufficient"><i style="background-image:url(\'' + hatchSvg + '\')"></i>Insufficient data (' + catCounts['insufficient'] + ')</div>' +
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
          var cat = effectiveCategory(m, cc.domesticCategory) || 'insufficient';
          var visible = !hiddenCats[cat];
          layer.setStyle({ fillOpacity: visible ? 1 : 0, opacity: visible ? 1 : 0 });
          if (visible) layer.bindPopup(layer.getPopup()); // keep interactive
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

      // Lakes
      allLakeLayers.push(addLakes(map, topo, CATEGORY_COLORS.lake));

      // Municipalities
      var geojson = topojson.feature(topo, topo.objects[cc.topoObject]);

      var muniLayer = (function(cc, muni, geojson) {
        return L.geoJSON(geojson, {
          style: function (feature) {
            var code = cc.featureCodeFn(feature);
            var m = muni[code];
            return {
              fillColor: getColor(m, cc.domesticCategory),
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
            var color = getColor(m, cc.domesticCategory);
            var label = LABELS[m.provider] || m.provider;

            var regionCode = cc.REGION_CODES[m.region] || '';
            var nameDisplay = regionCode ? escapeHtml(m.name) + ' (' + regionCode + ')' : escapeHtml(m.name);
            var eDomain = escapeHtml(m.domain || 'unknown');
            var eLabel = escapeHtml(label);
            var badge = '<span class="provider-badge" style="background:' + color + ';color:#000">' + eLabel + '</span>';

            var confidence = m.classification_confidence != null ? m.classification_confidence : 0;
            var confPct = Math.round(confidence) + '%';
            var metaParts = ['Confidence: ' + confPct];
            if (m.gateway) metaParts.push('Gateway: ' + escapeHtml(m.gateway));
            var metaLine = '<div class="popup-meta">' + metaParts.join(' &middot; ') + '</div>';

            var showBody = isMobile ? 'display:none' : '';

            var mxSection;
            if (m.mx && m.mx.length > 0) {
              mxSection = '<div class="popup-section"><div class="popup-toggle' + (isMobile ? '' : ' popup-toggle-open') + '" onclick="window.toggleSection(this)">Incoming mail handled by <span class="popup-section-hint">(MX)</span></div><div class="popup-section-body" style="' + showBody + '">' + renderTable(m.mx, cc.domesticTLDs) + '</div></div>';
            } else {
              mxSection = '<div class="popup-section"><div class="popup-toggle' + (isMobile ? '' : ' popup-toggle-open') + '" onclick="window.toggleSection(this)">Incoming mail handled by <span class="popup-section-hint">(MX)</span></div><div class="popup-section-body" style="' + showBody + '"><span class="popup-empty">No MX records found</span></div></div>';
            }

            var spfDelegations = parseSpfDelegations(m.spf);
            var spfSection;
            if (spfDelegations.length > 0) {
              spfSection = '<div class="popup-section"><div class="popup-toggle' + (isMobile ? '' : ' popup-toggle-open') + '" onclick="window.toggleSection(this)">Authorized to send mail <span class="popup-section-hint">(SPF)</span></div><div class="popup-section-body" style="' + showBody + '">' + renderTable(spfDelegations, cc.domesticTLDs) + '</div></div>';
            } else if (m.spf) {
              spfSection = '<div class="popup-section"><div class="popup-toggle' + (isMobile ? '' : ' popup-toggle-open') + '" onclick="window.toggleSection(this)">Authorized to send mail <span class="popup-section-hint">(SPF)</span></div><div class="popup-section-body" style="' + showBody + '"><span class="popup-empty">' + escapeHtml(m.spf) + '</span></div></div>';
            } else {
              spfSection = '<div class="popup-section"><div class="popup-toggle' + (isMobile ? '' : ' popup-toggle-open') + '" onclick="window.toggleSection(this)">Authorized to send mail <span class="popup-section-hint">(SPF)</span></div><div class="popup-section-body" style="' + showBody + '"><span class="popup-empty">No SPF record found</span></div></div>';
            }

            var signalsSection;
            if (m.classification_signals && m.classification_signals.length > 0) {
              var deduped = deduplicateSignals(m.classification_signals);
              var uniqueCount = deduped.length;
              var signalItems = deduped.map(function(s) {
                var kindLabel = SIGNAL_LABELS[s.kind] || s.kind;
                var condensed = condenseDetail(s.kind, s.detail);
                var dup = s.count > 1 ? '<span class="signal-dup">\u00d7' + s.count + '</span>' : '';
                return '<div class="signal-item" title="' + escapeHtml(s.detail).replace(/"/g, '&quot;') + '"><span class="signal-kind">' + escapeHtml(kindLabel) + '</span><span class="signal-text">' + escapeHtml(condensed) + '</span>' + dup + '</div>';
              }).join('');
              signalsSection = '<div class="popup-section"><div class="popup-toggle" onclick="window.toggleSection(this)">Classification signals <span class="popup-signal-count" title="DNS and network evidence used to identify the email provider">' + uniqueCount + '</span></div><div class="popup-section-body" style="display:none"><div class="signal-list">' + signalItems + '</div></div></div>';
            } else {
              signalsSection = '<div class="popup-section"><div class="popup-toggle" onclick="window.toggleSection(this)">Classification signals</div><div class="popup-section-body" style="display:none"><span class="popup-empty">No classification signals</span></div></div>';
            }

            layer.bindPopup(
              '<div class="info-popup">' +
              '<strong>' + nameDisplay + '</strong><br>' +
              eDomain + ' ' + badge +
              metaLine +
              mxSection + spfSection + signalsSection +
              '</div>',
              { maxWidth: isMobile ? 300 : 450 }
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
    addDownloadButton(map, 'providers-map.png');
  });
}
