/* nav.js — renders CH / AT / DE navigation tabs */
(function () {
  var path = window.location.pathname;
  var file = path.split('/').pop() || 'index.html';
  var primary = [
    { href: 'ch.html', label: 'Switzerland', match: ['ch.html'] },
    { href: 'at.html', label: 'Austria', match: ['at.html'] },
    { href: 'de.html', label: 'Germany', match: ['de.html'] },
  ];

  var nav = document.getElementById('nav');
  if (!nav) return;

  function isActive(link) {
    return link.match.indexOf(file) !== -1;
  }

  function makeLink(link, extraClass) {
    var a = document.createElement('a');
    a.href = link.href;
    a.className = 'header-link' + (extraClass ? ' ' + extraClass : '');
    a.textContent = link.label;
    if (isActive(link)) a.classList.add('active');
    return a;
  }

  var inlineWrap = document.createElement('span');
  inlineWrap.className = 'nav-primary';
  primary.forEach(function (link) {
    inlineWrap.appendChild(makeLink(link));
  });
  nav.appendChild(inlineWrap);

  var toggle = document.createElement('button');
  toggle.className = 'nav-menu-toggle';
  toggle.setAttribute('aria-label', 'More links');
  toggle.setAttribute('aria-expanded', 'false');
  toggle.textContent = '\u22EF';
  nav.appendChild(toggle);

  var menu = document.createElement('div');
  menu.className = 'nav-menu';
  primary.forEach(function (link) {
    menu.appendChild(makeLink(link, 'nav-menu-mobile'));
  });
  nav.appendChild(menu);

  toggle.addEventListener('click', function (e) {
    e.stopPropagation();
    var open = menu.classList.toggle('open');
    toggle.setAttribute('aria-expanded', String(open));
  });

  document.addEventListener('click', function (e) {
    if (!menu.contains(e.target) && e.target !== toggle) {
      menu.classList.remove('open');
      toggle.setAttribute('aria-expanded', 'false');
    }
  });
})();
