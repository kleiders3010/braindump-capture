// Service worker: makes the app open instantly and offline, and uploads queued dumps in the background
// (Background Sync, where the browser has it) even after the app was closed.
importScripts('outbox.js');

const CACHE = 'braindump-v3';
const SHELL = ['./', 'index.html', 'outbox.js', 'manifest.webmanifest', 'icon-192.png', 'icon-512.png'];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(SHELL)).then(() => self.skipWaiting()));
});

self.addEventListener('activate', e => {
  e.waitUntil(caches.keys()
    .then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k))))
    .then(() => self.clients.claim()));
});

// App shell: answer from the cache at once, refresh the cache in the background.
self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);
  if (e.request.method !== 'GET' || url.origin !== location.origin) return;
  e.respondWith(caches.open(CACHE).then(async cache => {
    const hit = await cache.match(e.request, { ignoreSearch: true });
    const fresh = fetch(e.request).then(r => { if (r.ok) cache.put(e.request, r.clone()); return r; }).catch(() => hit);
    return hit || fresh;
  }));
});

// Periodic Background Sync (installed app, Chrome on Android): the browser wakes this up now and then with the app closed.
self.addEventListener('periodicsync', e => {
  if (e.tag === 'flush') e.waitUntil(flush().catch(() => {}));
});

self.addEventListener('sync', e => {
  if (e.tag === 'flush') {
    e.waitUntil(flush().then(r => { if (r.left) throw new Error('not all sent yet'); }));  // rejecting = try again later
  }
});
