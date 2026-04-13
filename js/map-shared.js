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
    maxZoom: 19,
    crossOrigin: ''
  }).addTo(map);

  L.tileLayer('https://{s}.basemaps.cartocdn.com/light_only_labels/{z}/{x}/{y}{r}.png', {
    subdomains: 'abcd',
    maxZoom: 19,
    pane: 'shadowPane',
    crossOrigin: ''
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

function addCountryOutline(map, topo, topoObject) {
  var merged = topojson.merge(topo, topo.objects[topoObject].geometries);
  return L.geoJSON(merged, {
    interactive: false,
    style: { fill: false, weight: 1.5, color: '#333', opacity: 0.6 }
  }).addTo(map);
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

async function fetchMultiCountryData(countries) {
  var urls = [];
  for (var i = 0; i < countries.length; i++) {
    urls.push(fetch(countries[i].topoUrl));
    urls.push(fetch(countries[i].dataUrl));
  }
  var responses = await Promise.all(urls);
  var results = [];
  for (var i = 0; i < countries.length; i++) {
    var topoResp = responses[i * 2];
    var dataResp = responses[i * 2 + 1];
    if (!topoResp.ok) throw new Error('Failed to fetch topology for ' + (countries[i].label || 'country') + ': ' + topoResp.status);
    if (!dataResp.ok) throw new Error('Failed to fetch data for ' + (countries[i].label || 'country') + ': ' + dataResp.status);
    var topo = await topoResp.json();
    var data = await dataResp.json();
    indexMunicipalities(data);
    results.push({ topo: topo, data: data });
  }
  return results;
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

function fetchTileLayer(ctx, layer, zoom, minTX, maxTX, minTY, maxTY, tileSize, originX, originY) {
  var promises = [];
  for (var tx = minTX; tx <= maxTX; tx++) {
    for (var ty = minTY; ty <= maxTY; ty++) {
      (function (tx, ty) {
        var sub = 'abcd'.charAt(Math.abs(tx + ty) % 4);
        var url = 'https://' + sub + '.basemaps.cartocdn.com/' + layer + '/' + zoom + '/' + tx + '/' + ty + '.png';
        promises.push(
          fetch(url, { mode: 'cors' })
            .then(function (r) { return r.blob(); })
            .then(function (b) { return createImageBitmap(b); })
            .then(function (bmp) {
              ctx.drawImage(bmp, tx * tileSize - originX, ty * tileSize - originY, tileSize, tileSize);
              bmp.close();
            })
            .catch(function () {})
        );
      })(tx, ty);
    }
  }
  return Promise.all(promises);
}

function drawExportFeature(layer, ctx, map, exportZoom, originX, originY) {
  if (!layer._latlngs) return;
  var opts = layer.options;
  var hasFill = opts.fill !== false;
  var hasStroke = (opts.weight || 0) > 0 && opts.color !== 'transparent';
  if (!hasFill && !hasStroke) return;

  var rings = layer._latlngs;
  // Detect multi-polygon: _latlngs[0][0] is an array, not a LatLng
  var isMulti = rings[0] && rings[0][0] && Array.isArray(rings[0][0]) && !rings[0][0].lat;
  var polygons = isMulti ? rings : [rings];

  for (var p = 0; p < polygons.length; p++) {
    ctx.beginPath();
    var polyRings = polygons[p];
    for (var r = 0; r < polyRings.length; r++) {
      var ring = polyRings[r];
      for (var i = 0; i < ring.length; i++) {
        var pt = map.project(ring[i], exportZoom);
        var x = pt.x - originX;
        var y = pt.y - originY;
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      }
      ctx.closePath();
    }
    if (hasFill) {
      ctx.globalAlpha = opts.fillOpacity != null ? opts.fillOpacity : 0.2;
      ctx.fillStyle = opts.fillColor || opts.color || '#3388ff';
      ctx.fill();
    }
    if (hasStroke) {
      ctx.globalAlpha = opts.opacity != null ? opts.opacity : 1;
      ctx.strokeStyle = opts.color || '#3388ff';
      ctx.lineWidth = opts.weight || 1;
      ctx.stroke();
    }
  }
  ctx.globalAlpha = 1;
}

function drawMapFeatures(map, ctx, exportZoom, originX, originY) {
  map.eachLayer(function (layer) {
    if (layer._url) return;
    if (typeof layer.eachLayer === 'function') {
      layer.eachLayer(function (sub) {
        drawExportFeature(sub, ctx, map, exportZoom, originX, originY);
      });
    } else {
      drawExportFeature(layer, ctx, map, exportZoom, originX, originY);
    }
  });
}

function renderLegendToCanvas(legendEl, scale) {
  return new Promise(function (resolve) {
    if (!legendEl) { resolve(null); return; }
    var clone = legendEl.cloneNode(true);
    clone.classList.remove('legend-collapsed');
    var content = clone.querySelector('.legend-content');
    if (content) content.style.display = '';
    var toggle = clone.querySelector('.legend-toggle');
    if (toggle) toggle.style.display = 'none';
    var hidden = clone.querySelectorAll('.legend-hidden');
    for (var i = 0; i < hidden.length; i++) hidden[i].classList.remove('legend-hidden');
    var queue = [[legendEl, clone]];
    while (queue.length) {
      var pair = queue.shift();
      var orig = pair[0], copy = pair[1];
      if (orig.nodeType === 1) {
        var cs = window.getComputedStyle(orig);
        var style = '';
        for (var j = 0; j < cs.length; j++) {
          style += cs[j] + ':' + cs.getPropertyValue(cs[j]) + ';';
        }
        copy.setAttribute('style', style);
        if (cs.display === 'none' && copy !== toggle) copy.style.display = '';
      }
      var origChildren = orig.childNodes;
      var copyChildren = copy.childNodes;
      for (var k = 0; k < origChildren.length && k < copyChildren.length; k++) {
        if (origChildren[k].nodeType === 1) queue.push([origChildren[k], copyChildren[k]]);
      }
    }
    clone.style.margin = '0';
    clone.style.position = 'static';
    clone.style.boxShadow = '0 2px 8px rgba(0,0,0,0.2)';
    clone.style.background = 'white';
    clone.style.padding = '10px 14px';
    clone.style.borderRadius = '6px';
    if (toggle) toggle.style.display = 'none';
    if (content) { content.style.display = ''; content.style.background = 'transparent'; content.style.boxShadow = 'none'; content.style.padding = '0'; content.style.margin = '0'; }

    var xml = new XMLSerializer().serializeToString(clone);
    var svgWidth = legendEl.offsetWidth + 4;
    var svgHeight = legendEl.scrollHeight + 4;
    var svg = '<svg xmlns="http://www.w3.org/2000/svg" width="' + (svgWidth * scale) + '" height="' + (svgHeight * scale) + '">' +
      '<foreignObject width="' + svgWidth + '" height="' + svgHeight + '" transform="scale(' + scale + ')">' +
      xml + '</foreignObject></svg>';
    var blob = new Blob([svg], { type: 'image/svg+xml;charset=utf-8' });
    var url = URL.createObjectURL(blob);
    var img = new Image();
    img.onload = function () { URL.revokeObjectURL(url); resolve(img); };
    img.onerror = function () { URL.revokeObjectURL(url); resolve(null); };
    img.src = url;
  });
}

function exportMapImage(map, filename, onDone) {
  map.closePopup();

  // Render at current zoom + 2 for 4× tile detail in each dimension
  var viewZoom = map.getZoom();
  var exportZoom = Math.min(Math.round(viewZoom) + 2, 18);
  var bounds = map.getBounds();

  // Project bounds to pixel coordinates at export zoom
  var nw = map.project(bounds.getNorthWest(), exportZoom);
  var se = map.project(bounds.getSouthEast(), exportZoom);
  var originX = Math.floor(nw.x);
  var originY = Math.floor(nw.y);
  var w = Math.round(se.x - nw.x);
  var h = Math.round(se.y - nw.y);

  // Cap at 8192 — fall back to zoom+1 if too large
  if (w > 8192 || h > 8192) {
    exportZoom = Math.min(Math.round(viewZoom) + 1, 18);
    nw = map.project(bounds.getNorthWest(), exportZoom);
    se = map.project(bounds.getSouthEast(), exportZoom);
    originX = Math.floor(nw.x);
    originY = Math.floor(nw.y);
    w = Math.round(se.x - nw.x);
    h = Math.round(se.y - nw.y);
  }

  var canvas = document.createElement('canvas');
  canvas.width = w;
  canvas.height = h;
  var ctx = canvas.getContext('2d');

  ctx.fillStyle = '#f2efe9';
  ctx.fillRect(0, 0, w, h);

  // Tile range
  var tileSize = 256;
  var minTX = Math.floor(originX / tileSize);
  var maxTX = Math.floor((originX + w - 1) / tileSize);
  var minTY = Math.floor(originY / tileSize);
  var maxTY = Math.floor((originY + h - 1) / tileSize);

  // Legend (start rendering now, use later)
  var legendEl = document.querySelector('.legend');
  var legendScale = Math.max(2, Math.round(w / map.getContainer().offsetWidth));
  var legendPromise = renderLegendToCanvas(legendEl, legendScale);

  // 1. Base tiles → 2. GeoJSON features → 3. Label tiles → 4. Legend + attribution
  fetchTileLayer(ctx, 'light_nolabels', exportZoom, minTX, maxTX, minTY, maxTY, tileSize, originX, originY)
    .then(function () {
      drawMapFeatures(map, ctx, exportZoom, originX, originY);
      return fetchTileLayer(ctx, 'light_only_labels', exportZoom, minTX, maxTX, minTY, maxTY, tileSize, originX, originY);
    })
    .then(function () { return legendPromise; })
    .then(function (legendImg) {
      if (legendImg) {
        ctx.drawImage(legendImg, w - legendImg.width - 20, h - legendImg.height - 20);
      }

      // Attribution
      var fontSize = Math.max(13, Math.round(w / 300));
      ctx.font = fontSize + 'px -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif';
      var attrText = '\u00a9 OpenStreetMap \u00a9 CARTO';
      var tw = ctx.measureText(attrText).width;
      ctx.fillStyle = 'rgba(255,255,255,0.8)';
      ctx.fillRect(0, h - fontSize * 2.2, tw + fontSize * 2, fontSize * 2.2);
      ctx.fillStyle = 'rgba(0,0,0,0.5)';
      ctx.fillText(attrText, fontSize, h - fontSize * 0.6);

      canvas.toBlob(function (blob) {
        if (!blob) { if (onDone) onDone(); return; }
        var url = URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = url;
        a.download = filename;
        a.click();
        setTimeout(function () { URL.revokeObjectURL(url); }, 1000);
        if (onDone) onDone();
      }, 'image/png');
    })
    .catch(function (e) {
      console.error('Export failed:', e);
      alert('Export failed: ' + e.message);
      if (onDone) onDone();
    });
}

function addDownloadButton(map, filename) {
  var btn = document.createElement('button');
  btn.className = 'toggle-info';
  btn.id = 'download-btn';
  btn.innerHTML = '\u2913 Download';
  btn.setAttribute('aria-label', 'Download map as PNG');

  var headerRight = document.querySelector('.header-right');
  var aboutBtn = document.getElementById('toggle-info');
  headerRight.insertBefore(btn, aboutBtn);

  btn.addEventListener('click', function () {
    btn.disabled = true;
    btn.textContent = 'Exporting\u2026';
    setTimeout(function () {
      exportMapImage(map, filename, function () {
        btn.disabled = false;
        btn.innerHTML = '\u2913 Download';
      });
    }, 50);
  });
}
