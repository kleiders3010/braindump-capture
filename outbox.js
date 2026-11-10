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
  if (!flushing) {
    // One uploader at a time across the page and the service worker: two at once upload the same file twice.
    const locks = typeof navigator !== 'undefined' && navigator.locks;
    flushing = (locks ? locks.request('braindump-flush', doFlush) : doFlush()).finally(() => { flushing = null; });
  }
  return flushing;
}

const pause = ms => new Promise(r => setTimeout(r, ms));

// Returns {left, error}: error is null, 'setup', 'token', 'offline' or 'retry'.
async function doFlush() {
  const cfg = await kvGet('config');
  if (!cfg) return { left: (await outboxAll()).length, error: 'setup' };
  const headers = { Authorization: 'Bearer ' + cfg.token, Accept: 'application/vnd.github+json' };
  const branch = cfg.branch || 'inbox';
  const fail = async error => ({ left: (await outboxAll()).length, error });
  let conflicts = 0;
  for (;;) {
    const it = (await outboxAll())[0];  // read again every time: dumps are added while this runs
    if (!it) return { left: 0, error: null };
    const path = ('phone/' + it.name).split('/').map(encodeURIComponent).join('/');
    const url = `https://api.github.com/repos/${cfg.repo}/contents/${path}`;
    try {
      const r = await fetch(url, {
        method: 'PUT', headers,
        body: JSON.stringify({ message: 'phone: ' + it.name, content: await toBase64(it.blob), branch }),
      });
      if (r.ok) { await outboxDel(it.id); conflicts = 0; continue; }
      if (r.status === 401 || r.status === 404) return fail('token');
      if (r.status === 422 || r.status === 409) {  // usually: it is already there, from a try whose reply never arrived
        const g = await fetch(url + '?ref=' + encodeURIComponent(branch), { headers, cache: 'no-store' });
        if (g.ok) { await outboxDel(it.id); conflicts = 0; continue; }
        if (r.status === 409 && ++conflicts <= 3) { await pause(1500); continue; }  // the branch moved under us: again
      }
      return fail('retry');
    } catch (e) {
      return fail('offline');
    }
  }
}
