import './pretest.js'; // first
import {getGQP} from './gpq.js';

import init from './gpq.wasm?init';
import {describe, expect, test} from 'vitest';

describe('gpq', () => {
  test('getGPQ()', async () => {
    expect(getGQP).toBeDefined();
  });

  test('wasm', async () => {
    const instance = await init();
    expect(instance).toBeDefined();
  });
});
