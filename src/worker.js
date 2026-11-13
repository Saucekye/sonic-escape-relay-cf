// Sonic Escape relay on Cloudflare Workers: room codes for online multiplayer.
const MAX_PLAYERS = 8;
const CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ"; // no I or O, easy to read out
const SEAT_KEPT_MS = 20000;

function roomName(msg) {
  return String(msg.name || "").replace(/[\x00-\x1f]/g, "").trim().slice(0, 24) || "Room";
}

function roomMax(msg) {
  const n = parseInt(msg.max, 10);
  return Number.isFinite(n) ? Math.min(Math.max(n, 2), MAX_PLAYERS) : MAX_PLAYERS;
}

export default {
  async fetch(request, env) {
    const stub = env.RELAY.get(env.RELAY.idFromName("main"));
    return stub.fetch(request);
  },
};

export class Relay {
  constructor(state, env) {
    this.state = state;
    this.rooms = new Map(); // code -> { players: Map<id, ws>, name, max, public, started }
    this.away = new Map();  // "code:id" -> timer, for seats being kept
    this.saved = {};        // code -> { name, max, public, started }, as stored
    this.bootAt = Date.now();
    // Waking up: put the connections that stayed open back in their rooms
    this.state.blockConcurrencyWhile(async () => {
      this.saved = (await this.state.storage.get("rooms")) || {};
      for (const ws of this.state.getWebSockets()) {
        const me = ws.deserializeAttachment() || {};
        if (!me.room) continue;
        const room = this.roomFor(me.room);
        if (room) room.players.set(me.id, ws);
      }
    });
  }

  // A room by code: the live one, or one brought back from storage
  roomFor(code) {
    let room = this.rooms.get(code);
    if (!room && this.saved[code]) {
      room = { players: new Map(), ...this.saved[code] };
      this.rooms.set(code, room);
    }
    return room;
  }

  async save() {
    const out = {};
    // Just after a restart, keep the rooms whose players haven't come back yet
    if (Date.now() - this.bootAt < 60000) Object.assign(out, this.saved);
    for (const [code, r] of this.rooms) out[code] = { name: r.name, max: r.max, public: r.public, started: r.started };
    this.saved = out;
    try { await this.state.storage.put("rooms", out); } catch (e) { /* best effort */ }
  }

  listRooms() {
    const out = [];
    for (const [code, room] of this.rooms) {
      if (room.public && !room.started && room.players.size > 0) out.push({ code, name: room.name, players: room.players.size, max: room.max });
    }
    return out;
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      // A plain page, to check it's running
      return new Response(`Sonic Escape relay (Cloudflare): ${this.rooms.size} room(s) open\n`, {
        headers: { "Content-Type": "text/plain" },
      });
    }
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.state.acceptWebSocket(server);
    server.serializeAttachment({ room: null, id: 0 });
    return new Response(null, { status: 101, webSocket: client });
  }

  newCode() {
    let code;
    do {
      code = Array.from({ length: 4 }, () => CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)]).join("");
    } while (this.rooms.has(code) || this.saved[code]);
    return code;
  }

  send(ws, msg) {
    try { ws.send(JSON.stringify(msg)); } catch (e) { /* already closed */ }
  }

  broadcast(room, msg, exceptId) {
    for (const [id, ws] of room.players) if (id !== exceptId) this.send(ws, msg);
  }

  seat(ws, code, id) {
    ws.serializeAttachment({ room: code, id });
  }

  async webSocketMessage(ws, data) {
    let msg;
    try { msg = JSON.parse(typeof data === "string" ? data : new TextDecoder().decode(data)); } catch { return; }
    if (!msg || typeof msg !== "object") return;
    const me = ws.deserializeAttachment() || { room: null, id: 0 };

    if (!me.room) {
      if (msg.t === "host") {
        const code = this.newCode();
        const room = { players: new Map([[1, ws]]), name: roomName(msg), max: roomMax(msg), public: msg.public !== false, started: false };
        this.rooms.set(code, room);
        this.seat(ws, code, 1);
        this.send(ws, { t: "room", code, id: 1, name: room.name, max: room.max });
        await this.save();
      } else if (msg.t === "list") {
        this.send(ws, { t: "rooms", rooms: this.listRooms() });
      } else if (msg.t === "join") {
        const code = String(msg.code || "").toUpperCase().trim();
        const room = this.rooms.get(code);
        if (!room) return this.send(ws, { t: "error", msg: "No room with that code" });
        if (room.players.size >= room.max) return this.send(ws, { t: "error", msg: "Room is full" });
        if (room.started) return this.send(ws, { t: "error", msg: "That game has already started" });
        let id = 2;
        while (room.players.has(id) || this.away.has(code + ":" + id)) id++;
        room.players.set(id, ws);
        this.seat(ws, code, id);
        this.send(ws, { t: "joined", code, id, ids: [...room.players.keys()], name: room.name, max: room.max });
        this.broadcast(room, { t: "peer_join", id }, id);
      } else if (msg.t === "rejoin") {
        const code = String(msg.code || "").toUpperCase().trim();
        const id = parseInt(msg.id, 10);
        const room = this.roomFor(code);
        if (!room || !(id >= 1)) return this.send(ws, { t: "error", msg: "That room has closed" });
        const key = code + ":" + id;
        const kept = this.away.has(key);
        if (kept) { clearTimeout(this.away.get(key)); this.away.delete(key); }
        const old = room.players.get(id);
        if (old && old !== ws) { try { old.close(1000, "replaced"); } catch (e) {} }
        const known = kept || !!old;
        room.players.set(id, ws);
        this.seat(ws, code, id);
        this.send(ws, { t: "rejoined", code, id, ids: [...room.players.keys()], name: room.name, max: room.max });
        if (!known) this.broadcast(room, { t: "peer_join", id }, id);
      }
      return;
    }

    const room = this.rooms.get(me.room);
    if (!room) return;
    if (msg.t === "bye") return this.gone(me.room, me.id, ws);
    if (msg.t === "start" && me.id === 1) { room.started = true; await this.save(); }
    if (msg.t === "lobby" && me.id === 1) { room.started = false; await this.save(); } // back in the room after a run
    msg.from = me.id;
    if (msg.to) {
      const target = room.players.get(msg.to);
      if (target) this.send(target, msg);
    } else {
      this.broadcast(room, msg, me.id);
    }
  }

  // The connection dropped: keep the seat a while in case they come back
  dropped(ws) {
    const me = ws.deserializeAttachment() || {};
    const room = this.rooms.get(me.room);
    if (!room || room.players.get(me.id) !== ws) return;
    room.players.delete(me.id);
    const key = me.room + ":" + me.id;
    if (this.away.has(key)) clearTimeout(this.away.get(key));
    this.away.set(key, setTimeout(() => {
      this.away.delete(key);
      this.gone(me.room, me.id, null);
    }, SEAT_KEPT_MS));
  }

  // Really gone: tell the others
  gone(code, id, ws) {
    const room = this.rooms.get(code);
    if (!room) return;
    if (ws && room.players.get(id) === ws) room.players.delete(id);
    const key = code + ":" + id;
    if (this.away.has(key)) { clearTimeout(this.away.get(key)); this.away.delete(key); }
    if (ws) ws.serializeAttachment({ room: null, id: 0 });
    if (id === 1) {
      // The host left: the game can't go on
      this.broadcast(room, { t: "host_left" });
      for (const other of room.players.values()) { try { other.close(1000, "host left"); } catch (e) {} }
      this.rooms.delete(code);
    } else {
      this.broadcast(room, { t: "peer_left", id });
      let waiting = false;
      for (const k of this.away.keys()) if (k.startsWith(code + ":")) waiting = true;
      if (room.players.size === 0 && !waiting) this.rooms.delete(code);
    }
    this.save();
  }

  async webSocketClose(ws, code, reason, wasClean) {
    this.dropped(ws);
    try { ws.close(code === 1005 ? 1000 : code, reason); } catch (e) { /* already closed */ }
  }

  async webSocketError(ws, error) {
    this.dropped(ws);
  }
}
