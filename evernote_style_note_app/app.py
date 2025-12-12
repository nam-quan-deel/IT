import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
import re

import bleach
from flask import Flask, jsonify, render_template, request
from markdown import Markdown


APP_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("NOTES_DB_PATH", os.path.join(APP_DIR, "notes.db"))


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    with get_db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS notes (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              title TEXT NOT NULL DEFAULT '',
              content TEXT NOT NULL DEFAULT '',
              pinned INTEGER NOT NULL DEFAULT 0,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tags (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS note_tags (
              note_id INTEGER NOT NULL,
              tag_id INTEGER NOT NULL,
              PRIMARY KEY (note_id, tag_id),
              FOREIGN KEY (note_id) REFERENCES notes(id) ON DELETE CASCADE,
              FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
            );

            CREATE VIRTUAL TABLE IF NOT EXISTS notes_fts
            USING fts5(title, content, content='notes', content_rowid='id');

            CREATE TRIGGER IF NOT EXISTS notes_ai AFTER INSERT ON notes BEGIN
              INSERT INTO notes_fts(rowid, title, content) VALUES (new.id, new.title, new.content);
            END;

            CREATE TRIGGER IF NOT EXISTS notes_ad AFTER DELETE ON notes BEGIN
              INSERT INTO notes_fts(notes_fts, rowid, title, content) VALUES('delete', old.id, old.title, old.content);
            END;

            CREATE TRIGGER IF NOT EXISTS notes_au AFTER UPDATE ON notes BEGIN
              INSERT INTO notes_fts(notes_fts, rowid, title, content) VALUES('delete', old.id, old.title, old.content);
              INSERT INTO notes_fts(rowid, title, content) VALUES (new.id, new.title, new.content);
            END;
            """
        )


def parse_tags(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        parts = raw
    else:
        parts = str(raw).split(",")
    seen: set[str] = set()
    out: list[str] = []
    for p in parts:
        t = str(p).strip().lower()
        if not t:
            continue
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def upsert_tags(conn: sqlite3.Connection, note_id: int, tags: list[str]) -> None:
    conn.execute("DELETE FROM note_tags WHERE note_id = ?", (note_id,))
    for name in tags:
        conn.execute("INSERT OR IGNORE INTO tags(name) VALUES (?)", (name,))
        tag_id = conn.execute("SELECT id FROM tags WHERE name = ?", (name,)).fetchone()[0]
        conn.execute(
            "INSERT OR IGNORE INTO note_tags(note_id, tag_id) VALUES (?, ?)",
            (note_id, tag_id),
        )


@dataclass
class NoteSummary:
    id: int
    title: str
    excerpt: str
    pinned: bool
    updated_at: str
    tags: list[str]


_fts_token_re = re.compile(r"[a-zA-Z0-9_]+")


def normalize_fts_query(raw: str | None) -> str | None:
    """
    Turn free-form user input into a safe FTS5 query:
    - tokens are ANDed together
    - each token is treated as a prefix query (token*)
    """
    if not raw:
        return None
    tokens = _fts_token_re.findall(raw.lower())
    tokens = [t for t in tokens if t]
    if not tokens:
        return None
    return " AND ".join(f"{t}*" for t in tokens)


def list_notes(conn: sqlite3.Connection, query: str | None, tag: str | None) -> list[NoteSummary]:
    where = []
    params: list[Any] = []

    fts_query = normalize_fts_query(query)
    if fts_query:
        where.append(
            "n.id IN (SELECT rowid FROM notes_fts WHERE notes_fts MATCH ? ORDER BY bm25(notes_fts))"
        )
        params.append(fts_query)

    if tag:
        where.append(
            "EXISTS (SELECT 1 FROM note_tags nt JOIN tags t ON t.id = nt.tag_id WHERE nt.note_id = n.id AND t.name = ?)"
        )
        params.append(tag.strip().lower())

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    try:
        rows = conn.execute(
            f"""
            SELECT
              n.id,
              n.title,
              COALESCE(substr(n.content, 1, 240), '') AS excerpt,
              n.pinned,
              n.updated_at,
              COALESCE(group_concat(t.name, ','), '') AS tags
            FROM notes n
            LEFT JOIN note_tags nt ON nt.note_id = n.id
            LEFT JOIN tags t ON t.id = nt.tag_id
            {where_sql}
            GROUP BY n.id
            ORDER BY n.pinned DESC, n.updated_at DESC
            LIMIT 300;
            """,
            params,
        ).fetchall()
    except sqlite3.OperationalError:
        # If FTS parsing fails for any reason, degrade gracefully to "no search filter".
        where = []
        params = []
        if tag:
            where.append(
                "EXISTS (SELECT 1 FROM note_tags nt JOIN tags t ON t.id = nt.tag_id WHERE nt.note_id = n.id AND t.name = ?)"
            )
            params.append(tag.strip().lower())
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        rows = conn.execute(
            f"""
            SELECT
              n.id,
              n.title,
              COALESCE(substr(n.content, 1, 240), '') AS excerpt,
              n.pinned,
              n.updated_at,
              COALESCE(group_concat(t.name, ','), '') AS tags
            FROM notes n
            LEFT JOIN note_tags nt ON nt.note_id = n.id
            LEFT JOIN tags t ON t.id = nt.tag_id
            {where_sql}
            GROUP BY n.id
            ORDER BY n.pinned DESC, n.updated_at DESC
            LIMIT 300;
            """,
            params,
        ).fetchall()

    out: list[NoteSummary] = []
    for r in rows:
        tags_list = [t for t in (r["tags"] or "").split(",") if t]
        out.append(
            NoteSummary(
                id=int(r["id"]),
                title=str(r["title"] or ""),
                excerpt=str(r["excerpt"] or ""),
                pinned=bool(r["pinned"]),
                updated_at=str(r["updated_at"]),
                tags=tags_list,
            )
        )
    return out


def get_note(conn: sqlite3.Connection, note_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT id, title, content, pinned, created_at, updated_at FROM notes WHERE id = ?",
        (note_id,),
    ).fetchone()
    if not row:
        return None
    tags = conn.execute(
        """
        SELECT t.name
        FROM tags t
        JOIN note_tags nt ON nt.tag_id = t.id
        WHERE nt.note_id = ?
        ORDER BY t.name ASC;
        """,
        (note_id,),
    ).fetchall()
    return {
        "id": int(row["id"]),
        "title": row["title"],
        "content": row["content"],
        "pinned": bool(row["pinned"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "tags": [t[0] for t in tags],
    }


_md = Markdown(extensions=["extra", "fenced_code", "tables", "sane_lists"], output_format="html5")
_ALLOWED_TAGS = set(bleach.sanitizer.ALLOWED_TAGS).union(
    {
        "p",
        "pre",
        "code",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "table",
        "thead",
        "tbody",
        "tr",
        "th",
        "td",
        "blockquote",
        "hr",
        "br",
        "span",
    }
)
_ALLOWED_ATTRS = {
    **bleach.sanitizer.ALLOWED_ATTRIBUTES,
    "span": ["class"],
    "code": ["class"],
    "pre": ["class"],
}


def render_markdown_safe(text: str) -> str:
    # Markdown instance retains state; reset per render.
    _md.reset()
    html = _md.convert(text or "")
    return bleach.clean(html, tags=list(_ALLOWED_TAGS), attributes=_ALLOWED_ATTRS, strip=True)


app = Flask(__name__, static_folder="static", template_folder="templates")


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/notes")
def api_list_notes():
    query = request.args.get("query")
    tag = request.args.get("tag")
    with get_db() as conn:
        notes = list_notes(conn, query=query, tag=tag)
    return jsonify(
        {
            "notes": [
                {
                    "id": n.id,
                    "title": n.title,
                    "excerpt": n.excerpt,
                    "pinned": n.pinned,
                    "updated_at": n.updated_at,
                    "tags": n.tags,
                }
                for n in notes
            ]
        }
    )


@app.post("/api/notes")
def api_create_note():
    payload = request.get_json(force=True, silent=True) or {}
    title = str(payload.get("title") or "").strip()
    content = str(payload.get("content") or "")
    pinned = 1 if bool(payload.get("pinned")) else 0
    tags = parse_tags(payload.get("tags"))
    now = utcnow_iso()

    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO notes(title, content, pinned, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (title, content, pinned, now, now),
        )
        note_id = int(cur.lastrowid)
        upsert_tags(conn, note_id, tags)
        note = get_note(conn, note_id)

    return jsonify({"note": note}), 201


@app.get("/api/notes/<int:note_id>")
def api_get_note(note_id: int):
    with get_db() as conn:
        note = get_note(conn, note_id)
    if not note:
        return jsonify({"error": "not_found"}), 404
    return jsonify({"note": note})


@app.put("/api/notes/<int:note_id>")
def api_update_note(note_id: int):
    payload = request.get_json(force=True, silent=True) or {}
    title = str(payload.get("title") or "").strip()
    content = str(payload.get("content") or "")
    pinned = 1 if bool(payload.get("pinned")) else 0
    tags = parse_tags(payload.get("tags"))
    now = utcnow_iso()

    with get_db() as conn:
        exists = conn.execute("SELECT 1 FROM notes WHERE id = ?", (note_id,)).fetchone()
        if not exists:
            return jsonify({"error": "not_found"}), 404

        conn.execute(
            "UPDATE notes SET title = ?, content = ?, pinned = ?, updated_at = ? WHERE id = ?",
            (title, content, pinned, now, note_id),
        )
        upsert_tags(conn, note_id, tags)
        note = get_note(conn, note_id)

    return jsonify({"note": note})


@app.delete("/api/notes/<int:note_id>")
def api_delete_note(note_id: int):
    with get_db() as conn:
        cur = conn.execute("DELETE FROM notes WHERE id = ?", (note_id,))
        if cur.rowcount == 0:
            return jsonify({"error": "not_found"}), 404
    return jsonify({"ok": True})


@app.post("/api/preview")
def api_preview():
    payload = request.get_json(force=True, silent=True) or {}
    content = str(payload.get("content") or "")
    return jsonify({"html": render_markdown_safe(content)})


@app.get("/health")
def health():
    return jsonify({"ok": True})


def create_app() -> Flask:
    init_db()
    return app


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), debug=True)
