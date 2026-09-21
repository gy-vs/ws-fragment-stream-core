import { describe, expect, it } from 'vitest';
import {
  decodeFrame,
  encodeFrame,
  FrameProtocolError,
  ConnectionState,
  OPCODE_BINARY,
  OPCODE_PING,
  OPCODE_TEXT,
} from '../src/index.js';

it('decodes a simple text frame', () => {
  expect(decodeFrame(Uint8Array.from([0x81, 1, 65]))?.payload[0]).toBe(65);
});

describe('decodeFrame', () => {
  it('unmasks client payloads', () => {
    const f = decodeFrame(encodeFrame(new TextEncoder().encode('Hello'), { opcode: OPCODE_TEXT, mask: 0x37fa213d }));
    expect(new TextDecoder().decode(f!.payload)).toBe('Hello');
    expect(f!.masked).toBe(true);
  });

  it('handles 16-bit and 64-bit lengths', () => {
    for (const size of [200, 70000]) {
      const data = new Uint8Array(size).fill(7);
      const f = decodeFrame(encodeFrame(data, { opcode: OPCODE_BINARY }));
      expect(f!.payload.length).toBe(size);
      expect(f!.consumed).toBe(size + (size >= 1 << 16 ? 10 : 4));
    }
  });

  it('returns null on partial input and reports consumed bytes', () => {
    const wire = encodeFrame(new Uint8Array([1, 2, 3]), { opcode: OPCODE_PING, mask: 1 });
    expect(decodeFrame(wire.subarray(0, 3))).toBeNull();
    expect(decodeFrame(wire)!.consumed).toBe(wire.length);
  });

  it('rejects reserved length high bit', () => {
    const bad = new Uint8Array(10).fill(0);
    bad[1] = 127;
    bad[2] = 0x80;
    expect(() => decodeFrame(bad)).toThrow(FrameProtocolError);
  });
});

describe('ConnectionState', () => {
  it('tracks open -> closing -> closed', () => {
    const c = new ConnectionState();
    c.close();
    expect(c.state).toBe('closing');
    c.close();
    expect(c.state).toBe('closing');
    c.receiveClose();
    expect(c.state).toBe('closed');
  });
});
