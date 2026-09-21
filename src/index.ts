/**
 * RFC 6455 WebSocket streaming frame parser.
 *
 * The parser accepts arbitrarily chunked bytes and produces ordered events
 * (message fragments plus interleaved control frames) through an async
 * iterator. Backpressure is explicit: when the consumer stops reading, the
 * parser stops retaining payload bytes, and {@link FrameParser.write} reports a
 * non-zero `bufferedAmount` so the transport can pause its reads.
 */

export const OPCODE_CONTINUATION = 0x0;
export const OPCODE_TEXT = 0x1;
export const OPCODE_BINARY = 0x2;
export const OPCODE_CLOSE = 0x8;
export const OPCODE_PING = 0x9;
export const OPCODE_PONG = 0xa;

/** Largest payload permitted on a (non-fragmentable) control frame. */
export const MAX_CONTROL_PAYLOAD = 125;
/** Largest possible frame header: 2 + 8 (extended length) + 4 (mask key). */
export const MAX_HEADER_SIZE = 14;

/** Reason codes for protocol violations. */
export type ProtocolErrorCode =
  | 'invalid_opcode'
  | 'reserved_bits'
  | 'mask_required'
  | 'mask_forbidden'
  | 'fragmented_control'
  | 'control_too_large'
  | 'bad_length_encoding'
  | 'length_overflow'
  | 'unexpected_continuation'
  | 'expected_continuation'
  | 'message_too_large'
  | 'invalid_close_payload'
  | 'invalid_close_code'
  | 'invalid_utf8'
  | 'premature_eof'
  | 'cancelled'
  | 'closed';

export class FrameProtocolError extends Error {
  readonly code: ProtocolErrorCode;
  constructor(code: ProtocolErrorCode, message?: string) {
    super(message ? `${code}: ${message}` : code);
    this.name = 'FrameProtocolError';
    this.code = code;
  }
}

export type Role = 'client' | 'server';

export interface MessageStartEvent {
  type: 'messageStart';
  opcode: typeof OPCODE_TEXT | typeof OPCODE_BINARY;
}
export interface MessageFragmentEvent {
  type: 'messageFragment';
  /** Application payload chunk, already unmasked. */
  payload: Uint8Array;
  fin: boolean;
}
export interface MessageEndEvent {
  type: 'messageEnd';
}
export interface PingEvent {
  type: 'ping';
  payload: Uint8Array;
}
export interface PongEvent {
  type: 'pong';
  payload: Uint8Array;
}
export interface CloseEvent {
  type: 'close';
  code: number | null;
  reason: string;
  payload: Uint8Array;
}

export type ParserEvent =
  | MessageStartEvent
  | MessageFragmentEvent
  | MessageEndEvent
  | PingEvent
  | PongEvent
  | CloseEvent;

export interface FrameParserOptions {
  /**
   * Side of the connection whose *incoming* bytes are parsed.
   * A client receives server frames (unmasked); a server receives client
   * frames (masked).
   */
  role?: Role;
  /** Fragment consumption window; each fragment is at most this many bytes. */
  highWaterMark?: number;
  /** Reject any assembled message once it exceeds this size. */
  maxMessageSize?: number;
}

export interface WriteResult {
  /** True once retained bytes fit inside the consumption window again. */
  ok: boolean;
  bufferedAmount: number;
}

export interface ParsedFrame {
  fin: boolean;
  opcode: number;
  masked: boolean;
  maskKey: number | null;
  payload: Uint8Array;
  consumed: number;
}

/**
 * Chunk queue. Input chunks are referenced (not copied) until consumed;
 * payload fragments are copied out so callers never mutate caller-owned bytes
 * when applying the mask.
 */
class ByteQueue {
  private chunks: { data: Uint8Array; start: number }[] = [];
  private bytes = 0;

  get length(): number {
    return this.bytes;
  }

  push(chunk: Uint8Array): void {
    if (chunk.length) this.chunks.push({ data: chunk, start: 0 });
    this.bytes += chunk.length;
  }

  peekByte(offset: number): number | null {
    let remaining = offset;
    for (const c of this.chunks) {
      const avail = c.data.length - c.start;
      if (remaining < avail) return c.data[c.start + remaining];
      remaining -= avail;
    }
    return null;
  }

  readByte(): number | null {
    const c = this.chunks[0];
    if (!c || c.start >= c.data.length) return null;
    const v = c.data[c.start++];
    this.bytes--;
    if (c.start >= c.data.length) this.chunks.shift();
    return v;
  }

  /** Copies up to `max` bytes out, unmasked with a running payload offset. */
  readUnmasked(max: number, maskKey: number, maskOffset: number): { out: Uint8Array; read: number } {
    let want = Math.min(max, this.bytes);
    const out = new Uint8Array(want);
    let written = 0;
    while (want > 0 && this.chunks.length) {
      const c = this.chunks[0];
      const avail = c.data.length - c.start;
      const n = Math.min(avail, want);
      for (let i = 0; i < n; i++) {
        const j = maskOffset + written + i;
        out[written + i] = c.data[c.start + i] ^ ((maskKey >>> ((3 - (j & 3)) * 8)) & 0xff);
      }
      written += n;
      want -= n;
      c.start += n;
      this.bytes -= n;
      if (c.start >= c.data.length) this.chunks.shift();
    }
    return { out, read: written };
  }

  /** Copies exactly `len` bytes; returns null when insufficient data. */
  readExact(len: number): Uint8Array | null {
    if (this.bytes < len) return null;
    const { out } = this.readUnmasked(len, 0, 0);
    return out;
  }
}

interface Waiter {
  resolve: () => void;
  reject: (e: unknown) => void;
}

const hwmDefault = 16 * 1024;
const noLimit = Number.POSITIVE_INFINITY;

export class FrameParser {
  private readonly role: Role;
  private readonly hwm: number;
  private readonly maxMessageSize: number;

  private input = new ByteQueue();
  private events: ParserEvent[] = [];
  private eventBytes = 0;

  private error: FrameProtocolError | null = null;
  private ended = false;
  private cancelled = false;
  private sawEnd = false;

  private drainWaiters: Waiter[] = [];
  private readableWaiters: Waiter[] = [];

  // Header / frame state.
  private stage: 'b1' | 'extLen' | 'maskKey' | 'payload' | 'idle' = 'idle';
  private b0 = 0;
  private lengthCode = 0;
  private masked = false;
  private frameLength = 0; // control frames; or *remaining* length for data frames
  private maskKey = 0;
  private payloadMaskOffset = 0;

  // Message (continuation) state.
  private msgOpcode: 0 | 1 | 2 = 0;
  private msgSize = 0;
  private decoder: TextDecoder | null = null;

  constructor(options: FrameParserOptions = {}) {
    this.role = options.role ?? 'server';
    this.hwm = options.highWaterMark ?? hwmDefault;
    this.maxMessageSize = options.maxMessageSize ?? noLimit;
    if (!(this.hwm > 0)) throw new Error('highWaterMark must be positive');
  }

  // ---------------------------------------------------------------- public

  /** Feed one transport chunk. Returns the current backpressure status. */
  write(chunk: Uint8Array): WriteResult {
    if (this.cancelled) throw new FrameProtocolError('cancelled');
    if (this.ended) throw new FrameProtocolError('closed', 'parser already finished');
    this.input.push(chunk);
    this.pump();
    return { ok: this.retained() <= this.hwm, bufferedAmount: this.retained() };
  }

  /** Resolves once retained bytes drop back into the consumption window. */
  get drain(): Promise<void> {
    if (this.retained() <= this.hwm || this.error) return Promise.resolve();
    return new Promise<void>((resolve, reject) => {
      this.drainWaiters.push({ resolve, reject });
    });
  }

  /** Signal end of the byte stream; truncated frames become protocol errors. */
  end(): void {
    this.sawEnd = true;
    this.pump();
  }

  /** Abort parsing; pending drains reject and the iterator terminates. */
  cancel(reason?: string): void {
    if (this.cancelled || this.ended) return;
    this.cancelled = true;
    this.fail(new FrameProtocolError('cancelled', reason), false);
  }

  get done(): boolean {
    return this.ended;
  }

  get protocolError(): FrameProtocolError | null {
    return this.error;
  }

  /** All bytes currently retained (undelivered events + unparsed input). */
  get bufferedAmount(): number {
    return this.retained();
  }

  get isMessageOpen(): boolean {
    return this.msgOpcode !== 0;
  }

  async *[Symbol.asyncIterator](): AsyncIterator<ParserEvent> {
    try {
      for (;;) {
        if (this.events.length) {
          const ev = this.events.shift()!;
          if ('payload' in ev) this.eventBytes -= ev.payload.length;
          this.pump();
          this.wakeDrainers();
          yield ev;
          continue;
        }
        if (this.isFinishedIdle()) {
          // Cancellation is a normal completion; protocol violations throw.
          if (this.error && !this.cancelled) throw this.error;
          return;
        }
        await this.whenReadable();
      }
    } finally {
      // Consumer broke out of the loop / called iterator.return().
      if (!this.cancelled && !this.ended) this.cancel('consumer cancelled');
    }
  }

  // ------------------------------------------------------------- machinery

  private isFinishedIdle(): boolean {
    if (this.error)
      // On error, undelivered events are flushed first; remaining input is dead.
      return this.events.length === 0;
    return (
      (this.sawEnd || this.ended) &&
      this.events.length === 0 &&
      this.stage === 'idle' &&
      this.input.length === 0
    );
  }

  /**
   * Bytes the parser is holding for the consumer: queued event payloads plus
   * unparsed input. Header bytes in progress are inside `input` already.
   */
  private retained(): number {
    return this.eventBytes + this.input.length;
  }

  private whenReadable(): Promise<void> {
    if (this.events.length || this.isFinishedIdle()) return Promise.resolve();
    return new Promise<void>((resolve, reject) => {
      this.readableWaiters.push({ resolve, reject });
    });
  }

  private wakeReadable(): void {
    if (this.readableWaiters.length === 0) return;
    if (this.events.length === 0 && !this.isFinishedIdle()) return;
    const waiters = this.readableWaiters;
    this.readableWaiters = [];
    for (const w of waiters) w.resolve();
  }

  private wakeDrainers(): void {
    if (this.drainWaiters.length === 0) return;
    if (this.retained() > this.hwm && !this.error) return;
    const waiters = this.drainWaiters;
    this.drainWaiters = [];
    for (const w of waiters) (this.error ? w.reject : w.resolve).call(w, this.error ?? undefined);
  }

  private fail(err: FrameProtocolError): void {
    if (this.error) return;
    this.error = err;
    this.ended = true;
    this.stage = 'idle';
    this.wakeReadable();
    this.wakeDrainers();
  }

  private eof(): void {
    this.fail(new FrameProtocolError('premature_eof', `truncated frame in stage "${this.stage}"`));
  }

  private pump(): void {
    loop: for (;;) {
      if (this.error) break;

      switch (this.stage) {
        case 'idle': {
          const b0 = this.input.peekByte(0);
          if (b0 === null) {
            if (this.sawEnd && this.msgOpcode !== 0) this.eof();
            break loop;
          }
          if (b0 & 0x70) return this.fail(new FrameProtocolError('reserved_bits'));
          this.b0 = b0;
          this.stage = 'b1';
        }
        // fallthrough
        case 'b1': {
          const b1 = this.input.peekByte(1);
          if (b1 === null) {
            if (this.sawEnd) this.eof();
            break loop;
          }
          this.input.readByte();
          this.input.readByte();
          const fin = !!(this.b0 & 0x80);
          const opcode = this.b0 & 0x0f;

          this.masked = !!(b1 & 0x80);
          if (this.role === 'server' && !this.masked)
            return this.fail(new FrameProtocolError('mask_required', 'client frames must be masked'));
          if (this.role === 'client' && this.masked)
            return this.fail(new FrameProtocolError('mask_forbidden', 'server frames must not be masked'));

          this.lengthCode = b1 & 0x7f;
          this.fin = fin;
          this.opcode = opcode;
          this.stage = 'extLen';
        }
        // fallthrough
        case 'extLen': {
          if (this.lengthCode === 126 || this.lengthCode === 127) {
            const need = this.lengthCode === 126 ? 2 : 8;
            if (this.input.length < need) {
              if (this.sawEnd) this.eof();
              break loop;
            }
            const bytes = this.input.readExact(need)!;
            let len: number;
            if (need === 2) {
              len = (bytes[0] << 8) | bytes[1];
            } else {
              // Most-significant bit is reserved (RFC 6455 §5.2).
              if (bytes[0] & 0x80)
                return this.fail(new FrameProtocolError('length_overflow', '64-bit length has high bit set'));
              let big = 0n;
              for (let i = 0; i < 8; i++) big = (big << 8n) | BigInt(bytes[i]!);
              if (big > BigInt(Number.MAX_SAFE_INTEGER))
                return this.fail(new FrameProtocolError('length_overflow', `length ${big} exceeds safe integer`));
              len = Number(big);
            }
            this.resolveLength(len);
            if (this.error) return;
          } else {
            this.resolveLength(this.lengthCode);
            if (this.error) return;
          }
          break;
        }
        case 'maskKey': {
          if (this.input.length < 4) {
            if (this.sawEnd) this.eof();
            break loop;
          }
          const k = this.input.readExact(4)!;
          this.maskKey = (k[0] << 24) | (k[1] << 16) | (k[2] << 8) | k[3];
          this.stage = 'payload';
          break;
        }
        case 'payload': {
          if (this.isControl) {
            // Control frames deliver immediately, but a runaway producer of
            // them is still bounded by the consumption window. A control frame
            // only blocks here if its own payload would overflow the window;
            // since it is <=125 bytes and the window is the same unit, simply
            // require the queue to be drainable — a queued data fragment always
            // gets consumed before the consumer parks again.
            if (this.eventBytes > 0 && this.eventBytes + this.frameLength > this.hwm) break loop;
            if (this.input.length < this.frameLength) {
              if (this.sawEnd) this.eof();
              break loop;
            }
            const payload = this.readPayload(this.frameLength);
            this.emitControl(payload);
            if (this.error) return;
            this.resetFrame();
            break;
          }

          // Data frame: keep queued event payload within the window.
          if (this.eventBytes >= this.hwm) break loop;
          if (this.frameLength === 0) {
            // Empty frame: no fragment event, just close the message if FIN.
            const fin = this.fin;
            if (fin && this.msgOpcode === OPCODE_TEXT) this.flushText();
            if (this.error) return;
            this.resetFrame();
            if (fin) {
              this.enqueue({ type: 'messageEnd' });
              this.msgOpcode = 0;
              this.msgSize = 0;
              this.decoder = null;
            }
            break;
          }
          const room = this.hwm - this.eventBytes;
          const want = Math.min(room, this.frameLength);
          if (want === 0) {
            if (this.sawEnd && this.input.length === 0) this.eof();
            break loop;
          }
          if (this.input.length === 0) {
            if (this.sawEnd && this.frameLength > 0) this.eof();
            break loop;
          }
          const { out, read } = this.input.readUnmasked(want, this.maskKey, this.payloadMaskOffset);
          if (read === 0) break loop;
          this.payloadMaskOffset += read;
          this.frameLength -= read;

          if (this.msgOpcode === OPCODE_TEXT && read > 0) {
            try {
              // `stream: true` accepts incomplete multibyte sequences between
              // fragments and throws on a definite encoding error.
              this.decoder!.decode(out, { stream: true });
            } catch {
              return this.fail(new FrameProtocolError('invalid_utf8'));
            }
          }

          const frameDone = this.frameLength === 0;
          this.enqueue({ type: 'messageFragment', payload: out, fin: this.fin && frameDone });
          if (frameDone) {
            const fin = this.fin;
            if (fin && this.msgOpcode === OPCODE_TEXT) this.flushText();
            if (this.error) return;
            this.resetFrame();
            if (fin) {
              this.enqueue({ type: 'messageEnd' });
              this.msgOpcode = 0;
              this.msgSize = 0;
              this.decoder = null;
            }
          }
          // One data fragment per pump: the consumer must drain it (freeing the
          // window) before more payload is retained.
          break loop;
        }
      }
    }

    if (this.sawEnd && !this.error && this.input.length === 0) {
      if (this.stage !== 'idle' || this.msgOpcode !== 0 || this.frameLength > 0) this.eof();
    }
    this.wakeReadable();
    this.wakeDrainers();
  }

  private fin = false;
  private opcode = 0;
  private isControl = false;

  private resolveLength(len: number): void {
    const opcode = this.opcode;
    const fin = this.fin;
    const control = opcode >= OPCODE_CLOSE;

    if (control) {
      if (opcode > OPCODE_PONG) return this.fail(new FrameProtocolError('invalid_opcode', `opcode ${opcode}`));
      if (this.lengthCode !== len)
        return this.fail(
          new FrameProtocolError('bad_length_encoding', 'control frames must not use extended length'),
        );
      if (!fin) return this.fail(new FrameProtocolError('fragmented_control', 'control frames must not be fragmented'));
      if (len > MAX_CONTROL_PAYLOAD)
        return this.fail(new FrameProtocolError('control_too_large', `${len} > ${MAX_CONTROL_PAYLOAD}`));
    } else if (opcode > OPCODE_BINARY && opcode !== OPCODE_CONTINUATION) {
      return this.fail(new FrameProtocolError('invalid_opcode', `opcode ${opcode}`));
    } else if (opcode === OPCODE_CONTINUATION) {
      if (this.msgOpcode === 0)
        return this.fail(new FrameProtocolError('unexpected_continuation', 'continuation without start frame'));
    } else if (this.msgOpcode !== 0) {
      return this.fail(new FrameProtocolError('expected_continuation', 'new data frame while a message is open'));
    }

    if (!control) {
      if (this.msgSize + len > this.maxMessageSize)
        return this.fail(
          new FrameProtocolError(
            'message_too_large',
            `${this.msgSize + len} > ${this.maxMessageSize}`,
          ),
        );
      this.msgSize += len;
      if (opcode !== OPCODE_CONTINUATION) {
        this.msgOpcode = opcode as 1 | 2;
        if (opcode === OPCODE_TEXT) this.decoder = new TextDecoder('utf-8', { fatal: true });
        this.enqueue({ type: 'messageStart', opcode: opcode as 1 | 2 });
      }
    }

    this.frameLength = len;
    this.isControl = control;
    this.payloadMaskOffset = 0;
    this.stage = this.masked ? 'maskKey' : 'payload';
  }

  private flushText(): void {
    try {
      // Final decode rejects a dangling multibyte sequence at message end.
      this.decoder!.decode(new Uint8Array(0), { stream: false });
    } catch {
      this.fail(new FrameProtocolError('invalid_utf8'));
    }
  }

  private readPayload(len: number): Uint8Array {
    if (len === 0) return new Uint8Array(0);
    const { out } = this.input.readUnmasked(len, this.maskKey, 0);
    return out;
  }

  private emitControl(payload: Uint8Array): void {
    switch (this.opcode) {
      case OPCODE_PING:
        this.enqueue({ type: 'ping', payload });
        break;
      case OPCODE_PONG:
        this.enqueue({ type: 'pong', payload });
        break;
      case OPCODE_CLOSE: {
        let code: number | null = null;
        let reason = '';
        if (payload.length === 1)
          return this.fail(new FrameProtocolError('invalid_close_payload', 'close payload must be >= 2 bytes'));
        if (payload.length >= 2) {
          code = (payload[0] << 8) | payload[1];
          if (!isValidCloseCode(code))
            return this.fail(new FrameProtocolError('invalid_close_code', String(code)));
          try {
            reason = new TextDecoder('utf-8', { fatal: true }).decode(payload.subarray(2));
          } catch {
            return this.fail(new FrameProtocolError('invalid_close_payload', 'reason is not valid UTF-8'));
          }
        }
        this.enqueue({ type: 'close', code, reason, payload });
        // After Close the WebSocket is closing; no further frames are accepted.
        this.ended = true;
        break;
      }
    }
  }

  private resetFrame(): void {
    this.stage = 'idle';
    this.frameLength = 0;
    this.maskKey = 0;
    this.payloadMaskOffset = 0;
    this.isControl = false;
    this.opcode = 0;
    this.fin = false;
  }

  private enqueue(ev: ParserEvent): void {
    this.events.push(ev);
    if ('payload' in ev) this.eventBytes += ev.payload.length;
  }
}

function isValidCloseCode(code: number): boolean {
  if (code >= 1000 && code <= 1011) {
    // 1004/1005/1016 are reserved and never appear on the wire.
    return code !== 1004 && code !== 1005 && code !== 1006 && code !== 1015;
  }
  return code >= 3000 && code <= 4999;
}

// --------------------------------------------------------------- utilities

export interface EncodeOptions {
  fin?: boolean;
  opcode?: number;
  mask?: boolean | number; // true = random key; number = explicit key
}

/** Encode one frame (helper for tests and simple clients). */
export function encodeFrame(payload: Uint8Array, options: EncodeOptions = {}): Uint8Array {
  const fin = options.fin ?? true;
  const opcode = options.opcode ?? OPCODE_BINARY;
  const maskOpt = options.mask ?? false;
  const len = payload.length;
  const maskKey =
    maskOpt === true
      ? ((crypto.getRandomValues(new Uint32Array(1))[0]!) | 0)
      : typeof maskOpt === 'number'
        ? maskOpt
        : 0;
  const masked = maskOpt !== false;

  let ext = 0;
  if (len >= 1 << 16) ext = 8;
  else if (len > 125) ext = 2;

  const header = new Uint8Array(2 + ext + (masked ? 4 : 0));
  header[0] = (fin ? 0x80 : 0) | (opcode & 0x0f);
  const out = new Uint8Array(header.length + len);

  if (ext === 0) {
    header[1] = len;
  } else if (ext === 2) {
    header[1] = 126;
    header[2] = (len >>> 8) & 0xff;
    header[3] = len & 0xff;
  } else {
    header[1] = 127;
    const big = BigInt(len);
    for (let i = 0; i < 8; i++) header[2 + i] = Number((big >> BigInt((7 - i) * 8)) & 0xffn);
  }

  if (masked) {
    header[1] |= 0x80;
    const p = 2 + ext;
    header[p] = (maskKey >>> 24) & 0xff;
    header[p + 1] = (maskKey >>> 16) & 0xff;
    header[p + 2] = (maskKey >>> 8) & 0xff;
    header[p + 3] = maskKey & 0xff;
  }

  out.set(header, 0);
  for (let i = 0; i < len; i++) {
    out[header.length + i] = masked ? payload[i] ^ ((maskKey >>> ((3 - (i & 3)) * 8)) & 0xff) : payload[i];
  }
  return out;
}

/**
 * Decode exactly one complete frame from the head of `input`.
 * Returns null when more bytes are needed. Throws {@link FrameProtocolError}
 * on malformed header data (the streaming parser should be preferred for
 * untrusted input).
 */
export function decodeFrame(input: Uint8Array): ParsedFrame | null {
  if (input.length < 2) return null;
  const b0 = input[0]!;
  const b1 = input[1]!;
  if (b0 & 0x70) throw new FrameProtocolError('reserved_bits');
  const code = b1 & 0x7f;
  let headerLen = 2;
  let length: number;
  if (code < 126) {
    length = code;
  } else if (code === 126) {
    if (input.length < 4) return null;
    length = (input[2]! << 8) | input[3]!;
    headerLen = 4;
  } else {
    if (input.length < 10) return null;
    if (input[2]! & 0x80) throw new FrameProtocolError('length_overflow');
    let big = 0n;
    for (let i = 0; i < 8; i++) big = (big << 8n) | BigInt(input[2 + i]!);
    if (big > BigInt(Number.MAX_SAFE_INTEGER)) throw new FrameProtocolError('length_overflow');
    length = Number(big);
    headerLen = 10;
  }
  const masked = !!(b1 & 0x80);
  let maskKey: number | null = null;
  if (masked) {
    if (input.length < headerLen + 4) return null;
    maskKey =
      (input[headerLen]! << 24) |
      (input[headerLen + 1]! << 16) |
      (input[headerLen + 2]! << 8) |
      input[headerLen + 3]!;
    headerLen += 4;
  }
  if (input.length < headerLen + length) return null;
  const payload = new Uint8Array(length);
  for (let i = 0; i < length; i++) {
    payload[i] = masked
      ? input[headerLen + i]! ^ ((maskKey! >>> ((3 - (i & 3)) * 8)) & 0xff)
      : input[headerLen + i]!;
  }
  return { fin: !!(b0 & 0x80), opcode: b0 & 0x0f, masked, maskKey, payload, consumed: headerLen + length };
}

/** Minimal connection lifecycle tracker, kept for backwards compatibility. */
export class ConnectionState {
  state: 'open' | 'closing' | 'closed' = 'open';
  close(): void {
    if (this.state === 'open') this.state = 'closing';
  }
  receiveClose(): void {
    this.state = 'closed';
  }
}
