import { afterEach, describe, expect, test } from 'bun:test';

import { WorkmuxStatusPlugin } from '../resources/opencode/plugins/workmux-status';
import WorkmuxDirectoryPlugin from '../resources/opencode/index.js';

class EventQueue {
  private values: unknown[] = [];
  private waiters: Array<(result: IteratorResult<unknown>) => void> = [];

  push(value: unknown) {
    const waiter = this.waiters.shift();
    if (waiter) waiter({ value, done: false });
    else this.values.push(value);
  }

  subscribe(signal: AbortSignal): AsyncIterable<unknown> {
    return {
      [Symbol.asyncIterator]: () => ({
        next: () => {
          if (signal.aborted) return Promise.resolve({ value: undefined, done: true });
          const value = this.values.shift();
          if (value !== undefined) return Promise.resolve({ value, done: false });
          return new Promise<IteratorResult<unknown>>((resolve) => {
            const onAbort = () => resolve({ value: undefined, done: true });
            signal.addEventListener('abort', onAbort, { once: true });
            this.waiters.push((result) => {
              signal.removeEventListener('abort', onAbort);
              resolve(result);
            });
          });
        },
      }),
    };
  }
}

const cleanups: Array<() => Promise<void>> = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0)) await cleanup();
});

async function createHarness({ failRegistration = false } = {}) {
  const statuses: string[] = [];
  const commands: string[] = [];
  const queue = new EventQueue();
  const originalSpawn = Bun.spawn;
  Bun.spawn = ((args: string[]) => {
    const command = args.join(' ');
    commands.push(command);
    if (command === 'workmux register-agent' && failRegistration) {
      return { exited: Promise.reject(new Error('registration failed')) } as never;
    }
    const status = args[2];
    if (status !== undefined) statuses.push(status);
    return { exited: Promise.resolve(0) } as never;
  }) as never;
  const cleanup = await WorkmuxStatusPlugin.setup({
    event: { subscribe: ({ signal }: { signal: AbortSignal }) => queue.subscribe(signal) },
  } as never);
  cleanups.push(async () => {
    cleanup?.();
    await new Promise((resolve) => setTimeout(resolve, 0));
    Bun.spawn = originalSpawn;
  });

  return {
    commands,
    statuses,
    emit: async (event: unknown) => {
      queue.push(event);
      await new Promise((resolve) => setTimeout(resolve, 0));
    },
  };
}

const sessionStatus = (sessionID: string, type: 'busy' | 'idle') => ({
  type: 'session.status',
  properties: { sessionID, status: { type } },
});

const userMessage = (sessionID: string) => ({
  type: 'message.updated',
  properties: { sessionID, info: { role: 'user', sessionID } },
});

describe('WorkmuxStatusPlugin', () => {
  test('exports a stable V2 plugin definition', () => {
    expect(WorkmuxStatusPlugin.id).toBe('workmux-status');
    expect(typeof WorkmuxStatusPlugin.setup).toBe('function');
  });

  test('directory entrypoint exposes the OpenCode V2 plugin', () => {
    expect(WorkmuxDirectoryPlugin.id).toBe('workmux-status');
    expect(typeof WorkmuxDirectoryPlugin.setup).toBe('function');
  });

  test('awaits registration during initialization before status handling', async () => {
    let finishRegistration!: () => void;
    const registration = new Promise<void>((resolve) => {
      finishRegistration = resolve;
    });
    let initialized = false;
    const originalSpawn = Bun.spawn;
    Bun.spawn = (() => ({ exited: registration })) as never;
    const initialization = WorkmuxStatusPlugin.setup({
      event: { subscribe: () => new EventQueue().subscribe(new AbortController().signal) },
    } as never).then((cleanup) => {
      initialized = true;
      cleanups.push(async () => {
        cleanup?.();
        Bun.spawn = originalSpawn;
      });
      return cleanup;
    });
    await Promise.resolve();
    expect(initialized).toBe(false);

    finishRegistration();
    await initialization;
    expect(initialized).toBe(true);
  });

  test('registers before reporting status', async () => {
    const harness = await createHarness();
    await harness.emit(sessionStatus('parent', 'busy'));

    expect(harness.commands).toEqual([
      'workmux register-agent',
      'workmux set-window-status working',
    ]);
  });

  test('continues status tracking when registration fails', async () => {
    const harness = await createHarness({ failRegistration: true });
    await harness.emit(sessionStatus('parent', 'busy'));

    expect(harness.commands).toEqual([
      'workmux register-agent',
      'workmux set-window-status working',
    ]);
    expect(harness.statuses).toEqual(['working']);
  });

  test('continues consuming events when starting a status command throws', async () => {
    const originalSpawn = Bun.spawn;
    let failFirstStatus = true;
    const statuses: string[] = [];
    Bun.spawn = ((args: string[]) => {
      if (args[1] === 'register-agent') return { exited: Promise.resolve(0) } as never;
      if (failFirstStatus) {
        failFirstStatus = false;
        throw new Error('workmux executable unavailable');
      }
      statuses.push(args[2]);
      return { exited: Promise.resolve(0) } as never;
    }) as never;
    const queue = new EventQueue();
    const cleanup = await WorkmuxStatusPlugin.setup({
      event: { subscribe: ({ signal }: { signal: AbortSignal }) => queue.subscribe(signal) },
    } as never);
    cleanups.push(async () => {
      cleanup?.();
      await new Promise((resolve) => setTimeout(resolve, 0));
      Bun.spawn = originalSpawn;
    });

    queue.push(sessionStatus('first', 'busy'));
    await new Promise((resolve) => setTimeout(resolve, 0));
    queue.push(sessionStatus('first', 'idle'));
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(statuses).toEqual(['done']);
  });

  test('serializes status writes when event callbacks overlap', async () => {
    const commands: string[] = [];
    const applied: string[] = [];
    const completions: Array<() => void> = [];
    const originalSpawn = Bun.spawn;
    Bun.spawn = ((args: string[]) => {
      if (args[1] === 'register-agent') return { exited: Promise.resolve(0) } as never;
      const status = args[2];
      commands.push(args.join(' '));
      return {
        exited: new Promise<void>((resolve) => {
          completions.push(() => {
            if (status !== undefined) applied.push(status);
            resolve();
          });
        }),
      } as never;
    }) as never;
    const queue = new EventQueue();
    const cleanup = await WorkmuxStatusPlugin.setup({
      event: { subscribe: ({ signal }: { signal: AbortSignal }) => queue.subscribe(signal) },
    } as never);
    cleanups.push(async () => {
      cleanup?.();
      await new Promise((resolve) => setTimeout(resolve, 0));
      Bun.spawn = originalSpawn;
    });

    queue.push(sessionStatus('parent', 'busy'));
    await new Promise((resolve) => setTimeout(resolve, 0));
    queue.push(sessionStatus('parent', 'idle'));
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(commands).toEqual(['workmux set-window-status working']);

    completions.shift()?.();
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(commands).toEqual([
      'workmux set-window-status working',
      'workmux set-window-status done',
    ]);
    expect(applied).toEqual(['working']);

    completions.shift()?.();
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(applied).toEqual(['working', 'done']);
  });

  test('stays working when a child session finishes before its parent', async () => {
    const harness = await createHarness();

    await harness.emit(sessionStatus('parent', 'busy'));
    await harness.emit(sessionStatus('child', 'busy'));
    await harness.emit(sessionStatus('child', 'idle'));

    expect(harness.statuses).toEqual(['working']);

    await harness.emit(sessionStatus('parent', 'idle'));
    expect(harness.statuses).toEqual(['working', 'done']);
  });

  test('stays working when a parent session idles before its child', async () => {
    const harness = await createHarness();

    await harness.emit(sessionStatus('parent', 'busy'));
    await harness.emit(sessionStatus('child', 'busy'));
    await harness.emit(sessionStatus('parent', 'idle'));

    expect(harness.statuses).toEqual(['working']);

    await harness.emit(sessionStatus('child', 'idle'));
    expect(harness.statuses).toEqual(['working', 'done']);
  });

  test('forgets an active session when OpenCode deletes it', async () => {
    const harness = await createHarness();

    await harness.emit(sessionStatus('parent', 'busy'));
    await harness.emit(sessionStatus('child', 'busy'));
    await harness.emit(sessionStatus('parent', 'idle'));
    await harness.emit({
      type: 'session.deleted',
      properties: { info: { id: 'child' } },
    });
    await harness.emit(sessionStatus('child', 'busy'));

    expect(harness.statuses).toEqual(['working', 'done']);
  });

  test('ignores deletion of an untracked session', async () => {
    const harness = await createHarness();

    await harness.emit({
      type: 'session.deleted',
      properties: { info: { id: 'historical' } },
    });

    expect(harness.statuses).toEqual([]);
  });

  test('ignores idle status from an untracked session', async () => {
    const harness = await createHarness();

    await harness.emit(sessionStatus('parent', 'idle'));

    expect(harness.statuses).toEqual([]);
  });

  test('ignores stale busy events until a new user message', async () => {
    const harness = await createHarness();

    await harness.emit(sessionStatus('parent', 'busy'));
    await harness.emit(sessionStatus('parent', 'idle'));
    await harness.emit(sessionStatus('parent', 'busy'));
    expect(harness.statuses).toEqual(['working', 'done']);

    await harness.emit(userMessage('parent'));
    await harness.emit(sessionStatus('parent', 'busy'));
    expect(harness.statuses).toEqual(['working', 'done', 'working']);
  });

  test('reports waiting while another session is working', async () => {
    const harness = await createHarness();

    await harness.emit(sessionStatus('parent', 'busy'));
    await harness.emit({
      type: 'question.asked',
      properties: { sessionID: 'child' },
    });
    await harness.emit(sessionStatus('parent', 'idle'));
    expect(harness.statuses).toEqual(['working', 'waiting']);

    await harness.emit({
      type: 'question.replied',
      properties: { sessionID: 'child' },
    });
    expect(harness.statuses).toEqual(['working', 'waiting', 'working']);
  });

  test('reports waiting for permission requests and working after reply', async () => {
    const harness = await createHarness();

    await harness.emit(sessionStatus('parent', 'busy'));
    await harness.emit({
      type: 'permission.asked',
      properties: { sessionID: 'child' },
    });
    await harness.emit({
      type: 'permission.replied',
      properties: { sessionID: 'child' },
    });

    expect(harness.statuses).toEqual(['working', 'waiting', 'working']);
  });
});
