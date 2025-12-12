const el = (id) => document.getElementById(id);

const notesListEl = el('notesList');
const newNoteBtn = el('newNoteBtn');
const deleteNoteBtn = el('deleteNoteBtn');
const togglePreviewBtn = el('togglePreviewBtn');

const searchInput = el('searchInput');
const tagFilterInput = el('tagFilterInput');
const clearTagFilterBtn = el('clearTagFilterBtn');

const emptyState = el('emptyState');
const editor = el('editor');

const titleInput = el('titleInput');
const tagsInput = el('tagsInput');
const pinnedCheckbox = el('pinnedCheckbox');
const contentInput = el('contentInput');

const previewPane = el('previewPane');
const previewInner = el('previewInner');
const statusPill = el('statusPill');

let selectedId = null;
let previewOn = false;
let pendingSaveTimer = null;
let lastLoadedRevision = null;

function setStatus(text, kind = 'ok') {
  statusPill.textContent = text;
  statusPill.classList.remove('pill--ok', 'pill--warn');
  statusPill.classList.add(kind === 'warn' ? 'pill--warn' : 'pill--ok');
}

function fmtTime(iso) {
  if (!iso) return '';
  try {
    const d = new Date(iso);
    return d.toLocaleString(undefined, { month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit' });
  } catch {
    return '';
  }
}

function escapeHtml(s) {
  return String(s)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    ...opts,
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error || `HTTP ${res.status}`);
  return json;
}

function getFilters() {
  const query = searchInput.value.trim();
  const tag = tagFilterInput.value.trim().toLowerCase();
  return { query, tag };
}

async function refreshNotesList({ keepSelection = true } = {}) {
  const { query, tag } = getFilters();
  const qp = new URLSearchParams();
  if (query) qp.set('query', query);
  if (tag) qp.set('tag', tag);

  const data = await api(`/api/notes?${qp.toString()}`);
  renderNotesList(data.notes);

  if (keepSelection && selectedId) {
    const stillThere = data.notes.some((n) => n.id === selectedId);
    if (!stillThere) {
      selectedId = null;
      showEmpty();
    }
  }
}

function renderNotesList(notes) {
  notesListEl.innerHTML = '';

  if (!notes.length) {
    const div = document.createElement('div');
    div.className = 'noteItem';
    div.innerHTML = `<div class="noteItem__title"><span>No notes yet</span></div>
      <div class="noteItem__meta"><span>Create one to start.</span><span></span></div>`;
    notesListEl.appendChild(div);
    return;
  }

  for (const n of notes) {
    const item = document.createElement('div');
    item.className = 'noteItem' + (n.id === selectedId ? ' noteItem--active' : '');
    item.dataset.id = n.id;

    const title = (n.title || '').trim() || 'Untitled';
    const tags = Array.isArray(n.tags) ? n.tags : [];

    item.innerHTML = `
      <div class="noteItem__title">
        <span title="${escapeHtml(title)}">${escapeHtml(title)}</span>
        ${n.pinned ? '<span class="pin">Pinned</span>' : ''}
      </div>
      <div class="noteItem__meta">
        <span>${escapeHtml(fmtTime(n.updated_at))}</span>
        <span>${tags.length ? escapeHtml(tags.join(', ')) : ''}</span>
      </div>
      <div class="noteItem__excerpt">${escapeHtml((n.excerpt || '').replace(/\s+/g, ' ').trim())}</div>
      ${tags.length ? `<div class="noteItem__tags">${tags.map((t) => `<span class="tag">${escapeHtml(t)}</span>`).join('')}</div>` : ''}
    `;

    item.addEventListener('click', () => selectNote(n.id));
    notesListEl.appendChild(item);
  }
}

function showEmpty() {
  emptyState.classList.remove('hidden');
  editor.classList.add('hidden');
  deleteNoteBtn.disabled = true;
  setStatus('Ready', 'ok');
}

function showEditor() {
  emptyState.classList.add('hidden');
  editor.classList.remove('hidden');
  deleteNoteBtn.disabled = !selectedId;
}

function currentEditorState() {
  return {
    title: titleInput.value,
    content: contentInput.value,
    pinned: pinnedCheckbox.checked,
    tags: tagsInput.value,
  };
}

async function selectNote(id) {
  selectedId = id;
  showEditor();
  setStatus('Loading…', 'warn');

  const data = await api(`/api/notes/${id}`);
  const n = data.note;

  titleInput.value = n.title || '';
  contentInput.value = n.content || '';
  pinnedCheckbox.checked = !!n.pinned;
  tagsInput.value = (n.tags || []).join(', ');

  lastLoadedRevision = JSON.stringify(currentEditorState());

  setStatus('Ready', 'ok');
  await refreshNotesList({ keepSelection: true });

  if (previewOn) await refreshPreview();
}

async function createNewNote() {
  setStatus('Creating…', 'warn');
  const data = await api('/api/notes', {
    method: 'POST',
    body: JSON.stringify({ title: '', content: '', tags: [], pinned: false }),
  });
  const n = data.note;
  await refreshNotesList({ keepSelection: false });
  await selectNote(n.id);
}

function scheduleSave() {
  if (!selectedId) return;

  setStatus('Editing…', 'warn');
  if (pendingSaveTimer) clearTimeout(pendingSaveTimer);

  pendingSaveTimer = setTimeout(async () => {
    pendingSaveTimer = null;
    await saveNow();
  }, 450);
}

async function saveNow() {
  if (!selectedId) return;

  const payload = currentEditorState();
  const nextRevision = JSON.stringify(payload);
  if (nextRevision === lastLoadedRevision) {
    setStatus('Saved', 'ok');
    return;
  }

  try {
    setStatus('Saving…', 'warn');
    const data = await api(`/api/notes/${selectedId}`, {
      method: 'PUT',
      body: JSON.stringify(payload),
    });
    const n = data.note;
    lastLoadedRevision = nextRevision;

    setStatus('Saved', 'ok');
    await refreshNotesList({ keepSelection: true });

    if (previewOn) {
      // Preview can drift if server sanitizes; re-render.
      await refreshPreview();
    }

    // If title was empty and user hasn't focused title, don't steal focus.
    if ((payload.title || '').trim() === '' && (n.title || '').trim() !== '') {
      titleInput.value = n.title;
    }
  } catch (e) {
    console.error(e);
    setStatus('Save failed', 'warn');
  }
}

async function deleteSelected() {
  if (!selectedId) return;
  const yes = confirm('Delete this note? This cannot be undone.');
  if (!yes) return;

  await api(`/api/notes/${selectedId}`, { method: 'DELETE' });
  selectedId = null;
  lastLoadedRevision = null;

  await refreshNotesList({ keepSelection: false });
  showEmpty();
}

async function refreshPreview() {
  const content = contentInput.value;
  const data = await api('/api/preview', { method: 'POST', body: JSON.stringify({ content }) });
  previewInner.innerHTML = data.html || '';
}

function togglePreview() {
  previewOn = !previewOn;
  previewPane.classList.toggle('hidden', !previewOn);
  togglePreviewBtn.textContent = previewOn ? 'Editor' : 'Preview';
  if (previewOn) refreshPreview();
}

function debounce(fn, ms) {
  let t = null;
  return (...args) => {
    if (t) clearTimeout(t);
    t = setTimeout(() => fn(...args), ms);
  };
}

const debouncedRefreshList = debounce(() => refreshNotesList({ keepSelection: true }), 180);
const debouncedRefreshPreview = debounce(() => refreshPreview(), 220);

newNoteBtn.addEventListener('click', createNewNote);
deleteNoteBtn.addEventListener('click', deleteSelected);
togglePreviewBtn.addEventListener('click', togglePreview);

clearTagFilterBtn.addEventListener('click', async () => {
  tagFilterInput.value = '';
  await refreshNotesList({ keepSelection: true });
});

searchInput.addEventListener('input', debouncedRefreshList);
tagFilterInput.addEventListener('input', debouncedRefreshList);

for (const input of [titleInput, tagsInput, contentInput, pinnedCheckbox]) {
  const evt = input === pinnedCheckbox ? 'change' : 'input';
  input.addEventListener(evt, () => {
    scheduleSave();
    if (previewOn && input === contentInput) {
      // Keep preview responsive without hammering server.
      debouncedRefreshPreview();
    }
  });
}

window.addEventListener('keydown', (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 's') {
    e.preventDefault();
    saveNow();
  }
});

(async function boot() {
  setStatus('Loading…', 'warn');
  await refreshNotesList({ keepSelection: false });
  showEmpty();
})();
