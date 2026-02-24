/**
 * Container runtime adapter.
 *
 * Purpose:
 * - Centralize shell commands for the container engine used by NanoClaw.
 * - Keep engine-specific command strings in one place.
 * - Provide startup checks and orphan cleanup before message processing begins.
 *
 * Current runtime: Docker CLI (`docker`).
 */
import { execSync } from 'child_process';

import { logger } from './logger.js';

/**
 * CLI binary used to manage agent containers.
 *
 * Notes:
 * - Default is `docker`.
 * - The `/convert-to-apple-container` flow can update this implementation
 *   to target Apple Container runtime semantics.
 */
export const CONTAINER_RUNTIME_BIN = 'docker';

/**
 * Build mount args for a read-only bind mount.
 *
 * Docker form: `-v /host/path:/container/path:ro`
 * The `:ro` suffix prevents writes from inside the container.
 */
export function readonlyMountArgs(hostPath: string, containerPath: string): string[] {
  return ['-v', `${hostPath}:${containerPath}:ro`];
}

/**
 * Build a stop command string for a specific container name.
 */
export function stopContainer(name: string): string {
  return `${CONTAINER_RUNTIME_BIN} stop ${name}`;
}

/**
 * Verify that the container runtime is reachable.
 *
 * We run `${CONTAINER_RUNTIME_BIN} info` with a short timeout.
 * If this fails, startup is aborted because agents cannot execute without
 * a running container engine.
 */
export function ensureContainerRuntimeRunning(): void {
  try {
    execSync(`${CONTAINER_RUNTIME_BIN} info`, { stdio: 'pipe', timeout: 10000 });
    logger.debug('Container runtime already running');
  } catch (err) {
    logger.error({ err }, 'Failed to reach container runtime');
    console.error(
      '\n╔════════════════════════════════════════════════════════════════╗',
    );
    console.error(
      '║  FATAL: Container runtime failed to start                      ║',
    );
    console.error(
      '║                                                                ║',
    );
    console.error(
      '║  Agents cannot run without a container runtime. To fix:        ║',
    );
    console.error(
      '║  1. Ensure Docker is installed and running                     ║',
    );
    console.error(
      '║  2. Run: docker info                                           ║',
    );
    console.error(
      '║  3. Restart NanoClaw                                           ║',
    );
    console.error(
      '║  ╚════════════════════════════════════════════════════════════════╝\n',
    );
    throw new Error('Container runtime is required but failed to start');
  }
}

/**
 * Stop leftover NanoClaw containers from previous runs.
 *
 * Why this exists:
 * - If the Node process crashes, child containers may keep running.
 * - Those stale containers can hold resources or conflict with new runs.
 *
 * Strategy:
 * 1) list running containers matching `nanoclaw-*`
 * 2) stop each one best-effort
 */
export function cleanupOrphans(): void {
  try {
    const output = execSync(
      `${CONTAINER_RUNTIME_BIN} ps --filter name=nanoclaw- --format '{{.Names}}'`,
      { stdio: ['pipe', 'pipe', 'pipe'], encoding: 'utf-8' },
    );
    const orphans = output.trim().split('\n').filter(Boolean);

    for (const name of orphans) {
      try {
        execSync(stopContainer(name), { stdio: 'pipe' });
      } catch { /* already stopped */ }
    }

    if (orphans.length > 0) {
      logger.info({ count: orphans.length, names: orphans }, 'Stopped orphaned containers');
    }
  } catch (err) {
    logger.warn({ err }, 'Failed to clean up orphaned containers');
  }
}
