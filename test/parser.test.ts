import { describe, expect, it, vi } from 'vitest';
import {
  FrameParser,
  FrameProtocolError,
  encodeFrame,
  OPCODE_BINARY,
  OPCODE_CONTINUATION,
  OPCODE_CLOSE,
  OPCODE_PING,
  OPCODE_PONG,
  OPCODE_TEXT,
  type ParserEvent,
} from '../src/index.js';

const enc = new TextEncoder();
const dec = new TextDecoder();

// ------------------------------------------------------------- helpers

function frame(
  payload: Uint8Array | string,
  opts: Parameters<typeof encodeFrame>[1] = {},
): Uint8Array {
  const body = typeof payload === 'string' ? enc.encode(payload) : payload;
  return encodeFrame(body, { mask: 0x11223344, ...opts });
}

function concat(parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}

/** Deterministic pseudo-random chunk sizes. */
function chunkSeeds(seed: number): number[] {
  const sizes: number[] = [];
  let s = seed >>> 0;
  for (let i = 0; i < 4096; i++) {
    s = (s * 1664525 + 1013904223) >>> 0;
    sizes.push(1 + (s % 7));
  }
  return sizes;
}

function cutUp(data: Uint8Array, seed: number): Uint8Array[] {
  const out: Uint8Array[] = [];
  const sizes = chunkSeeds(seed);
  let i = 0;
  let k = 0;
  while (i < data.length) {
    const n = Math.min(sizes[k++ % sizes.length]!, data.length - i);
    out.push(data.subarray(i, i + n));
    i += n;
  }
  return out;
}

async function feedChunks(parser: FrameParser, chunks: Uint8Array[], drain = false): Promise<void> {
  for (const c of chunks) {
    const r = parser.write(c);
    if (drain && !r.ok) await parser.drain;
  }
}

/** Event shape independent of payload fragment boundaries. */
type CanonicalEvent =
  | { kind: 'messageStart'; opcode: number }
  | { kind: 'messageData'; data: number[] }
  | { kind: 'messageEnd' }
  | { kind: 'ping' | 'pong'; data: number[] }
  | { kind: 'close'; code: number | null; reason: string };

/** Preserves *event* order but merges fragment payloads into per-frame data groups. */
function canonicalize(events: ParserEvent[]): CanonicalEvent[] {
  const out: CanonicalEvent[] = [];
  for (const e of events) {
    if (e.type === 'messageStart') {
      out.push({ kind: 'messageStart', opcode: e.opcode });
    } else if (e.type === 'messageFragment') {
      const last = out[out.length - 1];
      const data = last && last.kind === 'messageData' ? last.data : null;
      if (data) for (const b of e.payload) data.push(b);
      else out.push({ kind: 'messageData', data: [...e.payload] });
    } else if (e.type === 'messageEnd') {
      out.push({ kind: 'messageEnd' });
    } else if (e.type === 'ping' || e.type === 'pong') {
      out.push({ kind: e.type, data: [...e.payload] });
    } else if (e.type === 'close') {
      out.push({ kind: 'close', code: e.code, reason: e.reason });
    }
  }
  return out;
}

const msg = (opcode: number, data: number[]): CanonicalEvent[] =>
  data.length
    ? [
        { kind: 'messageStart', opcode },
        { kind: 'messageData', data },
        { kind: 'messageEnd' },
      ]
    : [{ kind: 'messageStart', opcode }, { kind: 'messageEnd' }];

async function collect(parser: FrameParser): Promise<ParserEvent[]> {
  const out: ParserEvent[] = [];
  for await (const ev of parser) out.push(ev);
  return out;
}

async function expectError(code: string, wire: Uint8Array, role: 'client' | 'server' = 'server') {
  const parser = new FrameParser({ role });
  const events = collect(parser);
  parser.write(wire);
  parser.end();
  await expect(events).rejects.toMatchObject({ code });
}

// ---------------------------------------------------------------- tests

describe('basic assembly across arbitrary chunking', () => {
  const wire = concat([
    frame('Hello, WebSocket!', { opcode: OPCODE_TEXT }),
    frame(Uint8Array.from([0, 1, 2, 250, 255]), { opcode: OPCODE_BINARY }),
    frame('', { opcode: OPCODE_TEXT }),
  ]);

  const expected: CanonicalEvent[] = [
    ...msg(OPCODE_TEXT, [...enc.encode('Hello, WebSocket!')]),
    ...msg(OPCODE_BINARY, [0, 1, 2, 250, 255]),
    ...msg(OPCODE_TEXT, []),
  ];

  for (const seed of [1, 2, 7, 42, 99, 12345]) {
    it(`produces identical events for every chunk split (seed=${seed}, incl. byte-wise)`, async () => {
      const parser = new FrameParser();
      const done = collect(parser);
      const chunks = seed === 1 ? [...wire].map((b) => Uint8Array.from([b])) : cutUp(wire, seed);
      await feedChunks(parser, chunks, true);
      parser.end();
      const events = await done;
      expect(canonicalize(events)).toEqual(expected);
      // Fragment event boundaries may differ, but start/end/control count is fixed.
      expect(events.filter((e) => e.type === 'messageStart')).toHaveLength(3);
      expect(events.filter((e) => e.type === 'messageEnd')).toHaveLength(3);
    });
  }
});

describe('fragmented messages with interleaved control frames', () => {
  const big = new Uint8Array(10000);
  for (let i = 0; i < big.length; i++) big[i] = i & 0xff;

  const closeBody = new Uint8Array(2 + enc.encode('bye').length);
  closeBody[0] = 0x03;
  closeBody[1] = 0xe8;
  closeBody.set(enc.encode('bye'), 2);

  // Legal wire order: control frames may appear between data fragments, but
  // Close is the final frame of the connection.
  const wire = concat([
    frame(big.subarray(0, 10), { fin: false, opcode: OPCODE_BINARY }),
    frame('ping-1', { opcode: OPCODE_PING }),
    frame(big.subarray(10, 5000), { fin: false, opcode: OPCODE_CONTINUATION }),
    frame(Uint8Array.from([9]), { opcode: OPCODE_PONG }),
    frame(big.subarray(5000), { fin: true, opcode: OPCODE_CONTINUATION }),
    frame(closeBody, { opcode: OPCODE_CLOSE, mask: 0x99887766 }),
  ]);

  it('reassembles data and delivers each control frame immediately, in order', async () => {
    const parser = new FrameParser({ highWaterMark: 64 });
    const events: ParserEvent[] = [];
    const done = (async () => {
      for await (const ev of parser) events.push(ev);
    })();
    for (const c of cutUp(wire, 5)) {
      const r = parser.write(c);
      if (!r.ok) await parser.drain;
    }
    await done;

    const types = events.map((e) => e.type);
    expect(types.indexOf('ping')).toBeGreaterThanOrEqual(0);
    expect(types.indexOf('ping')).toBeLessThan(types.indexOf('messageEnd'));
    expect(types.indexOf('pong')).toBeLessThan(types.indexOf('messageEnd'));
    expect(types.indexOf('messageEnd')).toBeLessThan(types.indexOf('close'));

    const canon = canonicalize(events);
    const dataGroups = canon.filter((c) => c.kind === 'messageData') as { data: number[] }[];
    expect(dataGroups.flatMap((g) => g.data)).toEqual([...big]);
    expect(canon).toEqual([
      { kind: 'messageStart', opcode: OPCODE_BINARY },
      { kind: 'messageData', data: [...big.subarray(0, 10)] },
      { kind: 'ping', data: [...enc.encode('ping-1')] },
      { kind: 'messageData', data: [...big.subarray(10, 5000)] },
      { kind: 'pong', data: [9] },
      { kind: 'messageData', data: [...big.subarray(5000)] },
      { kind: 'messageEnd' },
      { kind: 'close', code: 1000, reason: 'bye' },
    ]);
  });

  it('every fragment respects the consumption window', async () => {
    const parser = new FrameParser({ highWaterMark: 64 });
    const seen: number[] = [];
    const done = (async () => {
      for await (const ev of parser) {
        if (ev.type === 'messageFragment') {
          seen.push(ev.payload.length);
          expect(ev.payload.length).toBeLessThanOrEqual(64);
          await new Promise((r) => setTimeout(r, 0));
        }
      }
    })();
    // byte-wise feeding, respecting backpressure
    for (const b of wire) {
      const r = parser.write(Uint8Array.from([b]));
      if (!r.ok) await parser.drain;
    }
    await done;
    expect(Math.max(...seen)).toBe(64);
    expect(seen.reduce((a, b) => a + b, 0)).toBe(10000);
  });

  it('ping inside an open message is delivered before more data even without a consumer', async () => {
    // Synchronous push: parser must retain the ping event ahead of data bytes.
    const parser = new FrameParser({ highWaterMark: 4096 });
    const order: string[] = [];
    const consumed = (async () => {
      for await (const ev of parser) order.push(ev.type);
    })();
    parser.write(frame('x', { fin: false, opcode: OPCODE_TEXT }));
    parser.write(frame('q', { opcode: OPCODE_PING }));
    parser.write(frame('y', { fin: true, opcode: OPCODE_CONTINUATION }));
    await consumed;
    expect(order).toEqual([
      'messageStart',
      'messageFragment',
      'ping',
      'messageFragment',
      'messageEnd',
    ]);
  });
});

describe('mask key spanning chunks and mask direction', () => {
  it('unmasks correctly when the 4 mask key bytes arrive one at a time', async () => {
    const wire = frame('masked text', { opcode: OPCODE_TEXT, mask: 0xaabbccdd });
    const parser = new FrameParser({ role: 'server' });
    const done = collect(parser);
    for (const b of wire) parser.write(Uint8Array.from([b]));
    parser.end();
    const canon = canonicalize(await done);
    expect(dec.decode(Uint8Array.from(canon[0]!.data))).toBe('masked text');
  });

  it('server role rejects unmasked client frames', () =>
    expectError('mask_required', encodeFrame(enc.encode('x'), { opcode: OPCODE_TEXT })));

  it('client role rejects masked server frames', () =>
    expectError(
      'mask_forbidden',
      encodeFrame(enc.encode('x'), { opcode: OPCODE_TEXT, mask: 0x01020304 }),
      'client',
    ));
});

describe('64-bit extended length', () => {
  it('parses a large (>64KiB) message delivered in random chunks', async () => {
    const size = 100_000;
    const data = new Uint8Array(size);
    for (let i = 0; i < size; i++) data[i] = (i * 31) & 0xff;
    const wire = frame(data, { opcode: OPCODE_BINARY });
    expect(wire[1] & 0x7f).toBe(127);

    const parser = new FrameParser({ highWaterMark: 4096 });
    let off = 0;
    const consumer = (async () => {
      for await (const ev of parser) {
        if (ev.type !== 'messageFragment') continue;
        for (let i = 0; i < ev.payload.length; i++) {
          if (ev.payload[i] !== data[off + i]) throw new Error(`byte mismatch at ${off + i}`);
        }
        off += ev.payload.length;
      }
    })();
    await feedChunks(parser, cutUp(wire, 77), true);
    parser.end();
    await consumer;
    expect(off).toBe(size);
  });

  it('rejects 64-bit length with the reserved high bit set', async () => {
    // masked binary frame, 64-bit length with the forbidden high bit
    const wire = Uint8Array.from([0x82, 0xff, 0x80, 0, 0, 0, 0, 0, 0, 0, 0x11, 0x22, 0x33, 0x44]);
    await expectError('length_overflow', wire);
  });

  it('rejects lengths beyond MAX_SAFE_INTEGER', async () => {
    const wire = Uint8Array.from([
      0x82, 0xff, 0x00, 0x20, 0, 0, 0, 0, 0, 0, 0x11, 0x22, 0x33, 0x44,
    ]);
    await expectError('length_overflow', wire);
  });
});

describe('control frame rules', () => {
  it('control frames must not be fragmented', async () => {
    await expectError(
      'fragmented_control',
      frame('x', { fin: false, opcode: OPCODE_PING }),
    );
  });

  it('control payload is limited to 125 bytes and must not use extended length', async () => {
    // 126 bytes forces the 16-bit extended length encoding, illegal for control
    const masked = frame(new Uint8Array(126), { opcode: OPCODE_PING });
    expect(masked[1] & 0x7f).toBe(126);
    await expectError('bad_length_encoding', masked);
  });

  it('accepts a 125-byte ping interleaved in a fragmented message', async () => {
    const wire = concat([
      frame('a', { fin: false, opcode: OPCODE_TEXT }),
      frame(new Uint8Array(125).fill(0x7a), { opcode: OPCODE_PING }),
      frame('b', { fin: true, opcode: OPCODE_CONTINUATION }),
    ]);
    const parser = new FrameParser();
    const done = collect(parser);
    await feedChunks(parser, cutUp(wire, 3), true);
    parser.end();
    const canon = canonicalize(await done);
    expect(canon).toEqual([
      { kind: 'messageStart', opcode: OPCODE_TEXT },
      { kind: 'messageData', data: [97] },
      { kind: 'ping', data: new Array(125).fill(0x7a) },
      { kind: 'messageData', data: [98] },
      { kind: 'messageEnd' },
    ]);
  });
});

describe('close frame validation', () => {
  it('accepts empty close and closes the parser', async () => {
    const parser = new FrameParser();
    const done = collect(parser);
    parser.write(frame('', { opcode: OPCODE_CLOSE }));
    const events = await done;
    expect(events).toEqual([{ type: 'close', code: null, reason: '', payload: new Uint8Array(0) }]);
    expect(parser.done).toBe(true);
    expect(() => parser.write(new Uint8Array([0x88, 0]))).toThrow(FrameProtocolError);
  });

  it('parses close code and UTF-8 reason', async () => {
    const body = new Uint8Array(2 + enc.encode('done').length);
    body[0] = 0x03;
    body[1] = 0xe8;
    body.set(enc.encode('done'), 2);
    const parser = new FrameParser();
    const done = collect(parser);
    parser.write(frame(body, { opcode: OPCODE_CLOSE }));
    const ev = (await done)[0] as Extract<ParserEvent, { type: 'close' }>;
    expect(ev.code).toBe(1000);
    expect(ev.reason).toBe('done');
  });

  it('rejects 1-byte close payload', () =>
    expectError('invalid_close_payload', frame(Uint8Array.from([3]), { opcode: OPCODE_CLOSE })));

  it('rejects reserved close codes', async () => {
    for (const code of [1004, 1005, 1006, 1015]) {
      const body = Uint8Array.from([code >> 8, code & 0xff]);
      await expectError('invalid_close_code', frame(body, { opcode: OPCODE_CLOSE }));
    }
  });

  it('rejects invalid UTF-8 close reason', () => {
    const body = Uint8Array.from([0x03, 0xe8, 0xff, 0xff]);
    return expectError('invalid_close_payload', frame(body, { opcode: OPCODE_CLOSE }));
  });
});

describe('illegal opcodes and continuation state', () => {
  // Masked, zero-payload frames (valid client framing) so mask checks pass.
  const bad = (op: number, fin = true) =>
    Uint8Array.from([(fin ? 0x80 : 0) | op, 0x80, 0x11, 0x22, 0x33, 0x44]);

  it('rejects undefined data opcodes 3-7', async () => {
    for (const op of [3, 4, 5, 6, 7]) await expectError('invalid_opcode', bad(op));
  });

  it('rejects undefined control opcodes 11-15', async () => {
    for (const op of [11, 12, 13, 14, 15]) await expectError('invalid_opcode', bad(op));
  });

  it('rejects continuation without an open message', () =>
    expectError('unexpected_continuation', bad(OPCODE_CONTINUATION)));

  it('rejects a new start frame while a message is open', async () => {
    const wire = concat([
      frame('a', { fin: false, opcode: OPCODE_TEXT }),
      frame('b', { fin: true, opcode: OPCODE_BINARY }),
    ]);
    await expectError('expected_continuation', wire);
  });

  it('rejects non-zero RSV bits', () =>
    expectError('reserved_bits', Uint8Array.from([0xf1, 0x80, 0, 0, 0, 0])));
});

describe('UTF-8 validation of text messages', () => {
  it('accepts a multibyte sequence split across fragments and chunks', async () => {
    const emoji = enc.encode('a🙂b');
    const wire = concat([
      frame(emoji.subarray(0, 2), { fin: false, opcode: OPCODE_TEXT }),
      frame(emoji.subarray(2), { fin: true, opcode: OPCODE_CONTINUATION }),
    ]);
    const parser = new FrameParser();
    const done = collect(parser);
    await feedChunks(parser, cutUp(wire, 11), true);
    parser.end();
    const canon = canonicalize(await done);
    const data = (canon.find((c) => c.kind === 'messageData') as { data: number[] }).data;
    expect(dec.decode(Uint8Array.from(data))).toBe('a🙂b');
  });

  it('rejects a definite invalid sequence mid-message', () =>
    expectError('invalid_utf8', frame(Uint8Array.from([0x61, 0xff, 0x62]), { opcode: OPCODE_TEXT })));

  it('rejects a dangling multibyte sequence at message end', () =>
    expectError('invalid_utf8', frame(Uint8Array.from([0x61, 0xf0]), { opcode: OPCODE_TEXT })));
});

describe('premature EOF', () => {
  const full = frame('abcdef', { opcode: OPCODE_TEXT });
  const cases: [string, number][] = [
    ['first byte only', 1],
    ['header only', 2],
    ['partial 16-bit extended length', 3],
    ['partial mask key', 5],
    ['partial payload', full.length - 1],
  ];
  for (const [name, n] of cases) {
    it(`on ${name}`, async () => {
      const parser = new FrameParser();
      const done = collect(parser);
      parser.write(full.subarray(0, n));
      parser.end();
      await expect(done).rejects.toMatchObject({ code: 'premature_eof' });
    });
  }

  it('on empty input (clean connection close with no frames)', async () => {
    const parser = new FrameParser();
    const done = collect(parser);
    parser.end();
    expect(await done).toEqual([]);
  });

  it('on an unfinished fragmented message', async () => {
    const parser = new FrameParser();
    const done = collect(parser);
    parser.write(frame('part', { fin: false, opcode: OPCODE_TEXT }));
    parser.end();
    await expect(done).rejects.toMatchObject({ code: 'premature_eof' });
  });

  it('clean end after a complete frame simply finishes', async () => {
    const parser = new FrameParser();
    const done = collect(parser);
    parser.write(frame('ok', { opcode: OPCODE_TEXT }));
    parser.end();
    expect(canonicalize(await done)).toEqual([
      { kind: 'messageStart', opcode: OPCODE_TEXT },
      { kind: 'messageData', data: [0x6f, 0x6b] },
      { kind: 'messageEnd' },
    ]);
  });
});

describe('oversized messages', () => {
  it('rejects a single frame over maxMessageSize', async () => {
    const parser = new FrameParser({ maxMessageSize: 100 });
    const done = collect(parser);
    parser.write(frame(new Uint8Array(101), { opcode: OPCODE_BINARY }));
    await expect(done).rejects.toMatchObject({ code: 'message_too_large' });
  });

  it('rejects fragments whose assembled size exceeds the limit', async () => {
    const wire = concat([
      frame(new Uint8Array(60), { fin: false, opcode: OPCODE_BINARY }),
      frame(new Uint8Array(60), { fin: true, opcode: OPCODE_CONTINUATION }),
    ]);
    const parser = new FrameParser({ maxMessageSize: 100, highWaterMark: 16 });
    const done = collect(parser);
    for (const c of cutUp(wire, 9)) {
      if (parser.protocolError) break;
      const r = parser.write(c);
      if (!r.ok) await parser.drain.catch(() => {});
    }
    await expect(done).rejects.toMatchObject({ code: 'message_too_large' });
  });

  it('resets the allowance for a following message', async () => {
    const parser = new FrameParser({ maxMessageSize: 10 });
    const done = collect(parser);
    parser.write(frame(new Uint8Array(10), { opcode: OPCODE_BINARY }));
    parser.write(frame(new Uint8Array(10), { opcode: OPCODE_BINARY }));
    parser.end();
    const canon = canonicalize(await done);
    expect(canon).toHaveLength(2);
  });
});

describe('cancellation and backpressure bounds', () => {
  it('breaking out of the iterator cancels parsing and rejects pending drains', async () => {
    const parser = new FrameParser({ highWaterMark: 8 });
    const wire = frame(new Uint8Array(1000), { opcode: OPCODE_BINARY });

    // Concurrent producer; stops on its own once cancellation fails the drain.
    const producer = (async () => {
      for (const b of wire) {
        const r = parser.write(Uint8Array.from([b]));
        if (!r.ok) await parser.drain;
      }
    })();

    let stopped = false;
    for await (const ev of parser) {
      if (ev.type === 'messageFragment') {
        stopped = true;
        break;
      }
    }
    expect(stopped).toBe(true);
    expect(parser.protocolError?.code).toBe('cancelled');
    await expect(producer).rejects.toMatchObject({ code: 'cancelled' });
    await expect(parser.drain).resolves.toBeUndefined();
    expect(() => parser.write(new Uint8Array([0]))).toThrow(FrameProtocolError);
  });

  it('explicit cancel terminates the iterator with no throw', async () => {
    const parser = new FrameParser();
    let waited = false;
    const done = (async () => {
      const events: ParserEvent[] = [];
      for await (const ev of parser) {
        events.push(ev);
        waited = true;
      }
      return events;
    })();
    // Let the consumer park on whenReadable, then cancel.
    await new Promise((r) => setTimeout(r, 0));
    parser.cancel('go away');
    expect(await done).toEqual([]);
    expect(waited).toBe(false);
    expect(parser.protocolError?.code).toBe('cancelled');
  });

  it('retained bytes stay bounded by the consumption window (+1 in-flight byte)', async () => {
    const hwm = 8;
    const parser = new FrameParser({ highWaterMark: hwm });
    const wire = frame(new Uint8Array(5000), { opcode: OPCODE_BINARY });

    // Slow consumer: pauses several ticks after each fragment.
    const consumer = (async () => {
      let count = 0;
      for await (const ev of parser) {
        if (ev.type !== 'messageFragment') continue;
        count++;
        if (count % 3 === 0) await new Promise((r) => setTimeout(r, 2));
      }
    })();

    // Producer runs concurrently and must observe backpressure while the
    // consumer is paused.
    let maxObserved = 0;
    let pressureEvents = 0;
    const producer = (async () => {
      for (const b of wire) {
        const r = parser.write(Uint8Array.from([b]));
        maxObserved = Math.max(maxObserved, r.bufferedAmount);
        if (!r.ok) {
          pressureEvents++;
          await parser.drain;
        }
      }
      parser.end();
    })();

    await producer;
    await consumer;
    expect(pressureEvents).toBeGreaterThan(0);
    expect(maxObserved).toBeLessThanOrEqual(hwm + 1);
  });

  it('cancel rejects a drain promise that was already pending', async () => {
    const parser = new FrameParser({ highWaterMark: 4 });
    const wire = frame(new Uint8Array(100), { opcode: OPCODE_BINARY });

    // Parked consumer (never drains its first fragment).
    const consumer = (async () => {
      for await (const ev of parser) {
        if (ev.type === 'messageFragment') await new Promise(() => {});
      }
    })();

    // Producer that parks on drain once the window is full.
    const producer = (async () => {
      for (const b of wire) {
        const r = parser.write(Uint8Array.from([b]));
        if (!r.ok) await parser.drain;
      }
    })();

    // Let the producer fill the window and block.
    await vi.waitFor(() => expect(parser.bufferedAmount).toBeGreaterThan(4));
    const drain = parser.drain;
    expect(drain).toBeInstanceOf(Promise);
    parser.cancel();
    await expect(drain).rejects.toMatchObject({ code: 'cancelled' });
    await expect(producer).rejects.toMatchObject({ code: 'cancelled' });
    await consumer.catch(() => {});
  });

  it('write() backpressure flips back to ok after the consumer drains', async () => {
    const parser = new FrameParser({ highWaterMark: 16 });
    const wire = frame(new Uint8Array(200), { opcode: OPCODE_BINARY });
    let redCount = 0;
    const consumer = (async () => {
      for await (const ev of parser) {
        if (ev.type === 'messageFragment') await new Promise((r) => setTimeout(r, 0));
      }
    })();
    for (const c of cutUp(wire, 3)) {
      const r = parser.write(c);
      if (!r.ok) {
        redCount++;
        await parser.drain;
      }
    }
    expect(redCount).toBeGreaterThan(0);
    parser.end();
    await consumer;
  });
});
