/* web/js/partials.js */
(async () => {
  const slots = document.querySelectorAll('[data-include]');
  for (const el of slots) {
    const src = el.getAttribute('data-include');
    try {
      const res = await fetch(src, { cache: 'no-store' });
      el.outerHTML = await res.text();
    } catch {
      // fail silently
    }
  }
})();
