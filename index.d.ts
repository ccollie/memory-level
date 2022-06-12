import {
  AbstractLevel,
  AbstractDatabaseOptions,
  AbstractOpenOptions,
  NodeCallback
} from 'abstract-level'
import {
  Redis,
  RedisOptions,
} from 'ioredis';

/**
 * Redis based {@link AbstractLevel} database for Node.js.
 *
 * @template KDefault The default type of keys if not overridden on operations.
 * @template VDefault The default type of values if not overridden on operations.
 */
export class RedisLevel<KDefault = string, VDefault = string>
  extends AbstractLevel<Buffer | Uint8Array | string, KDefault, VDefault> {
  /**
   * Database constructor.
   * @param location The data location.
   * @param options Options, of which some will be forwarded to {@link open}.
   */
  constructor (location: string, options?: DatabaseOptions<KDefault, VDefault> | undefined)

  open (): Promise<void>
  open (options: OpenOptions): Promise<void>
  open (callback: NodeCallback<void>): void
  open (options: OpenOptions, callback: NodeCallback<void>): void
}

/**
 * Options for the {@link RedisLevel} constructor.
 */
export interface DatabaseOptions<K, V> extends AbstractDatabaseOptions<K, V> {
  /**
   Redis connection options. If not provided, it will use db 0 on localhost:6379
   */
  connection?: string | RedisOptions | Redis

  /**
   * number of values to fetch in one redis call for iteration. Defaults to 256.
   */
  highWaterMark?: number

  /**
   * An {@link AbstractLevel} option that has no effect on {@link RedisLevel}.
   */
  createIfMissing?: boolean

  /**
   * An {@link AbstractLevel} option that has no effect on {@link RedisLevel}.
   */
  errorIfExists?: boolean

  /**
   * Set to true to disable sharing of the connection between multiple RedisLevel instances.
   */
  ownClient?: boolean
}

/**
 * Options for the {@link RedisLevel.open} method.
 */
export interface OpenOptions extends AbstractOpenOptions {
  /**
   * clearOnOpen: If true, the database will be destroyed on open.
   */
  clearOnOpen?: boolean

  /**
   Redis connection options. If not provided, it will use db 0 on localhost:6379
   */
  connection?: string | RedisOptions | Redis

  /**
   * Set to true to disable sharing of the connection between multiple RedisLevel instances.
   */
  ownClient?: boolean
}
