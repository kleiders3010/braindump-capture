// Shared by the page and the service worker: the queue of dumps waiting to reach the inbox.
// A dump is written to this queue first and deleted only after the server confirmed it, so nothing is lost offline.
const DB_NAME = 'braindump', OUTBOX = 'outbox', KV = 'kv', REC = 'rec';

function openDb() {
  return new Promise((resolve, reject) => {
    const r = indexedDB.open(DB_NAME, 1);
    r.onupgradeneeded = () => {
      r.result.createObjectStore(OUTBOX, { keyPath: 'id', autoIncrement: true });
      r.result.createObjectStore(REC, { autoIncrement: true });
      r.result.createObjectStore(KV);
    };
    r.onsuccess = () => resolve(r.result);
    r.onerror = () => reject(r.error);
  });
}

async function tx(store, mode, fn) {
  const d = await openDb();
  return new Promise((resolve, reject) => {
    const t = d.transaction(store, mode);
    const req = fn(t.objectStore(store));
    t.oncomplete = () => resolve(req ? req.result : undefined);
    t.onerror = t.onabort = () => reject(t.error);
  });
}

const kvGet = k => tx(KV, 'readonly', s => s.get(k));
const kvSet = (k, v) => tx(KV, 'readwrite', s => s.put(v, k));
const kvDel = k => tx(KV, 'readwrite', s => s.delete(k));
const outboxAdd = item => tx(OUTBOX, 'readwrite', s => s.add(item));
const outboxAll = () => tx(OUTBOX, 'readonly', s => s.getAll());
const outboxDel = id => tx(OUTBOX, 'readwrite', s => s.delete(id));
const recAdd = chunk => tx(REC, 'readwrite', s => s.add(chunk));
const recAll = () => tx(REC, 'readonly', s => s.getAll());
const recClear = () => tx(REC, 'readwrite', s => s.clear());

async function toBase64(blob) {
  const bytes = new Uint8Array(await blob.arrayBuffer());
  let bin = '';
  for (let i = 0; i < bytes.length; i += 0x8000) bin += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000));
  return btoa(bin);
}

let flushing = null;
function flush() {
  if (!flushing) flushing = doFlush().finally(() => { flushing = null; });
  return flushing;
}

// Returns {left, error}: error is null, 'setup', 'token', 'offline' or 'retry'.
async function doFlush() {
  const items = await outboxAll();
  const cfg = await kvGet('config');
  if (!cfg) return { left: items.length, error: 'setup' };
  let left = items.length;
  for (const it of items) {
    const path = ('phone/' + it.name).split('/').map(encodeURIComponent).join('/');
    const url = `https://api.github.com/repos/${cfg.repo}/contents/${path}`;
    const headers = { Authorization: 'Bearer ' + cfg.token, Accept: 'application/vnd.github+json' };
    const branch = cfg.branch || 'inbox';
    try {
      const r = await fetch(url, {
        method: 'PUT', headers,
        body: JSON.stringify({ message: 'phone: ' + it.name, content: await toBase64(it.blob), branch }),
      });
      if (r.ok) { await outboxDel(it.id); left--; continue; }
      if (r.status === 401 || r.status === 404) return { left, error: 'token' };
      if (r.status === 422) {  // usually: it is already there, from a try whose reply never arrived
        const g = await fetch(url + '?ref=' + encodeURIComponent(branch), { headers });
        if (g.ok) { await outboxDel(it.id); left--; continue; }
      }
      return { left, error: 'retry' };
    } catch (e) {
      return { left, error: 'offline' };
    }
  }
  return { left, error: null };
}
