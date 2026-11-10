import { resolve } from 'path';

import {
  appConfig,
  authConfig,
  dbConfig,
  emailConfig,
  fileWriteConfig,
  migrationConfig,
  oidcRuntimeConfig,
  podcastConfig,
  storageConfig,
} from './config';

const ORIGINAL_ENV = process.env;

function resetEnv(): void {
  process.env = { ...ORIGINAL_ENV };
  delete process.env.NODE_ENV;
  delete process.env.HOST;
  delete process.env.APP_URL;
  delete process.env.APP_VERSION;
  delete process.env.OIDC_ALLOW_LOCAL_ISSUERS;
  delete process.env.SWAGGER_ENABLED;
  for (const key of [
    'BOOK_SEARCH_ENABLED',
    'BOOK_SEARCH_INDEX_READY',
    'BOOK_SEARCH_RESTART_PROOF_PATH',
    'BOOK_SEARCH_ENDPOINT',
    'BOOK_SEARCH_INDEX',
    'BOOK_SEARCH_USERNAME',
    'BOOK_SEARCH_PASSWORD',
    'BOOK_SEARCH_TIMEOUT_MS',
    'BOOK_SEARCH_MAX_RESULT_WINDOW',
    'BOOK_SEARCH_MATCH_MODE',
    'BOOK_SEARCH_WORKER_ENABLED',
    'BOOK_SEARCH_WORKER_INTERVAL_MS',
    'BOOK_SEARCH_WORKER_BATCH_SIZE',
  ])
    delete process.env[key];
  delete process.env.KOBO_CLOUDSCRAPER_PYTHON;
  delete process.env.KOREADER_PLUGIN_PATH;
  delete process.env.METADATA_PROVIDER_DIRECTORY_URL;
  delete process.env.DATABASE_URL;
  delete process.env.JWT_SECRET;
  delete process.env.JWT_EXPIRES_IN;
  delete process.env.JWT_REFRESH_EXPIRES_IN;
  delete process.env.SETUP_BOOTSTRAP_TOKEN;
  delete process.env.DISABLE_LOCAL_AUTH;
  delete process.env.APP_DATA_PATH;
  delete process.env.BOOK_DOCK_PATH;
  delete process.env.LIBRARY_BROWSE_ROOT;
  delete process.env.FILE_WRITE_DEBOUNCE_MS;
  delete process.env.FILE_WRITE_MAX_CONCURRENT_WRITES;
  delete process.env.EMAIL_ENCRYPTION_KEY;
  delete process.env.MIGRATION_ENCRYPTION_KEY;
  delete process.env.MIGRATION_IMPORT_ROOT;
  delete process.env.PODCAST_ENCRYPTION_KEY;
  delete process.env.PODCAST_MAX_FEED_BYTES;
  delete process.env.PODCAST_MAX_EPISODE_BYTES;
  delete process.env.PODCAST_MAX_CONCURRENT_DOWNLOADS;
  delete process.env.PODCAST_REQUEST_TIMEOUT_MS;
  delete process.env.PODCAST_MAX_DOWNLOAD_DURATION_MS;
  delete process.env.OIDC_STATE_TTL_SECS;
  delete process.env.OIDC_DISCOVERY_CACHE_TTL_SECS;
  delete process.env.OIDC_JWKS_CACHE_TTL_SECS;
  delete process.env.OIDC_CLOCK_TOLERANCE_SECS;
  delete process.env.OIDC_TOKEN_EXCHANGE_TIMEOUT_MS;
}

describe('config', () => {
  beforeEach(() => {
    resetEnv();
  });

  afterAll(() => {
    process.env = ORIGINAL_ENV;
  });

  it('uses app defaults, including local-build fallback version', () => {
    expect(appConfig()).toEqual({
      nodeEnv: 'development',
      host: '0.0.0.0',
      appUrl: 'http://localhost:6263',
      nativeRedirectUri: 'bookorbit://oauth2-callback',
      version: 'Local build',
      githubReleasesRepo: 'bookorbit/bookorbit',
      githubReleasesToken: undefined,
      metadataProviderDirectoryUrl: 'https://raw.githubusercontent.com/bookorbit/bookorbit-metadata-providers/main/directory.json',
      oidcAllowLocalIssuers: false,
      swaggerEnabled: false,
      bookSearch: {
        enabled: false,
        indexReady: false,
        restartProofPath: undefined,
        endpoint: 'http://127.0.0.1:4085',
        index: 'books_metadata_gse_v1',
        username: '',
        password: '',
        timeoutMs: 1500,
        maxResultWindow: 1000,
        matchMode: 'contains',
        workerEnabled: false,
        workerIntervalMs: 100,
        workerBatchSize: 100,
      },
      koboCloudscraperPython: undefined,
      koreaderPluginSourcePath: undefined,
    });
  });

  it('selects explicit fulltext matching and defaults to native contains behavior', () => {
    expect(appConfig().bookSearch.matchMode).toBe('contains');
    process.env.BOOK_SEARCH_MATCH_MODE = ' fulltext ';
    expect(appConfig().bookSearch.matchMode).toBe('fulltext');
  });

  it('requires an explicit restart proof path and trims an empty setting back to ordinary reconciliation', () => {
    expect(appConfig().bookSearch.restartProofPath).toBeUndefined();
    process.env.BOOK_SEARCH_RESTART_PROOF_PATH = ' /private/search-restart.json ';
    expect(appConfig().bookSearch.restartProofPath).toBe('/private/search-restart.json');
    process.env.BOOK_SEARCH_RESTART_PROOF_PATH = ' ';
    expect(appConfig().bookSearch.restartProofPath).toBeUndefined();
  });

  it('keeps the worker opt-in and bounds the interval', () => {
    process.env.BOOK_SEARCH_WORKER_ENABLED = 'true';
    process.env.BOOK_SEARCH_WORKER_INTERVAL_MS = '250';
    expect(appConfig().bookSearch).toMatchObject({ workerEnabled: true, workerIntervalMs: 250 });
    for (const value of ['0', '9', '60001', '1.5', 'NaN']) {
      process.env.BOOK_SEARCH_WORKER_INTERVAL_MS = value;
      expect(() => appConfig()).toThrow('BOOK_SEARCH_WORKER_INTERVAL_MS');
    }
  });

  it('bounds optional index batches while retaining the existing default', () => {
    expect(appConfig().bookSearch.workerBatchSize).toBe(100);
    process.env.BOOK_SEARCH_WORKER_BATCH_SIZE = '500';
    expect(appConfig().bookSearch.workerBatchSize).toBe(500);
    for (const value of ['0', '501', 'NaN', '1.5']) {
      process.env.BOOK_SEARCH_WORKER_BATCH_SIZE = value;
      expect(() => appConfig()).toThrow('BOOK_SEARCH_WORKER_BATCH_SIZE');
    }
  });

  it('rejects an unknown search mode rather than silently changing matching behavior', () => {
    process.env.BOOK_SEARCH_MATCH_MODE = 'unsupported';
    expect(() => appConfig()).toThrow('BOOK_SEARCH_MATCH_MODE');
  });

  it('reads app values from environment when provided', () => {
    process.env.NODE_ENV = 'production';
    process.env.HOST = '127.0.0.1';
    process.env.APP_URL = 'https://bookorbit.local';
    process.env.APP_VERSION = 'v2.3.4';
    process.env.OIDC_ALLOW_LOCAL_ISSUERS = 'true';
    process.env.SWAGGER_ENABLED = 'true';
    process.env.KOBO_CLOUDSCRAPER_PYTHON = '/opt/bookorbit-python/bin/python';
    process.env.KOREADER_PLUGIN_PATH = '/opt/koreader/bookorbit.koplugin';
    process.env.GITHUB_RELEASES_REPO = 'acme/app';
    process.env.GITHUB_RELEASES_TOKEN = 'ghp_example';
    process.env.NATIVE_REDIRECT_URI = 'myfork://oauth2-callback';
    process.env.METADATA_PROVIDER_DIRECTORY_URL = ' OFF ';
    process.env.BOOK_SEARCH_ENABLED = 'true';
    process.env.BOOK_SEARCH_INDEX_READY = 'true';
    process.env.BOOK_SEARCH_ENDPOINT = ' http://search:4080 ';
    process.env.BOOK_SEARCH_INDEX = ' library_v2 ';
    process.env.BOOK_SEARCH_USERNAME = 'fixture-user';
    process.env.BOOK_SEARCH_PASSWORD = 'fixture-password';
    process.env.BOOK_SEARCH_TIMEOUT_MS = '900';
    process.env.BOOK_SEARCH_MAX_RESULT_WINDOW = '2000';

    expect(appConfig()).toEqual({
      nodeEnv: 'production',
      host: '127.0.0.1',
      appUrl: 'https://bookorbit.local',
      nativeRedirectUri: 'myfork://oauth2-callback',
      version: 'v2.3.4',
      githubReleasesRepo: 'acme/app',
      githubReleasesToken: 'ghp_example',
      metadataProviderDirectoryUrl: null,
      oidcAllowLocalIssuers: true,
      swaggerEnabled: true,
      bookSearch: {
        enabled: true,
        indexReady: true,
        endpoint: 'http://search:4080',
        index: 'library_v2',
        username: 'fixture-user',
        password: 'fixture-password',
        timeoutMs: 900,
        maxResultWindow: 2000,
        matchMode: 'contains',
        workerEnabled: false,
        workerIntervalMs: 100,
        workerBatchSize: 100,
      },
      koboCloudscraperPython: '/opt/bookorbit-python/bin/python',
      koreaderPluginSourcePath: '/opt/koreader/bookorbit.koplugin',
    });
  });

  it.each(['', '   '])('preserves wildcard binding for blank HOST %j', (host) => {
    process.env.HOST = host;
    expect(appConfig().host).toBe('0.0.0.0');
  });

  it.each(['127.0.0.1', '192.0.2.10', '::1', '::'])('reads and trims bind address %s', (host) => {
    process.env.HOST = ` ${host} `;
    expect(appConfig().host).toBe(host);
  });

  it('falls back to false when OIDC_ALLOW_LOCAL_ISSUERS is invalid', () => {
    process.env.OIDC_ALLOW_LOCAL_ISSUERS = 'maybe';
    expect(appConfig().oidcAllowLocalIssuers).toBe(false);
  });

  it('uses defaults for database, auth, email, and migration config', () => {
    expect(dbConfig().url).toBe('postgres://bookorbit:bookorbit@localhost:5432/bookorbit');
    expect(authConfig()).toEqual({
      jwtSecret: 'change-me-in-production',
      jwtExpiresIn: '15m',
      jwtRefreshExpiresIn: '7d',
      setupBootstrapToken: '',
      refreshRotationGraceMs: 30_000,
      passwordLoginEnabled: true,
    });
    expect(emailConfig().encryptionKey).toBe('');
    expect(migrationConfig()).toEqual({ encryptionKey: '', importRoot: undefined });
  });

  it('disables password login only for an enabled DISABLE_LOCAL_AUTH flag', () => {
    for (const value of ['true', '1', 'yes', 'on']) {
      process.env.DISABLE_LOCAL_AUTH = value;
      expect(authConfig().passwordLoginEnabled).toBe(false);
    }
    for (const value of ['false', '0', 'no', 'off', 'invalid']) {
      process.env.DISABLE_LOCAL_AUTH = value;
      expect(authConfig().passwordLoginEnabled).toBe(true);
    }
  });

  it('resolves the configured migration import root', () => {
    process.env.MIGRATION_IMPORT_ROOT = './imports/audiobookshelf';

    expect(migrationConfig().importRoot).toBe(resolve('./imports/audiobookshelf'));
  });

  it('treats a blank migration import root as unset', () => {
    process.env.MIGRATION_IMPORT_ROOT = '   ';

    expect(migrationConfig().importRoot).toBeUndefined();
  });

  it('resolves storage path from APP_DATA_PATH', () => {
    process.env.APP_DATA_PATH = './tmp/bookorbit-data';
    expect(storageConfig().appDataPath).toBe(resolve('./tmp/bookorbit-data'));
    expect(storageConfig().bookDockPath).toBe(resolve('./tmp/bookorbit-data/book-dock'));
    expect(storageConfig().libraryBrowseRoot).toBe(resolve('/'));
  });

  it('falls back storage path to /data when APP_DATA_PATH is not set', () => {
    expect(storageConfig().appDataPath).toBe(resolve('/data'));
    expect(storageConfig().bookDockPath).toBe(resolve('/data/book-dock'));
    expect(storageConfig().libraryBrowseRoot).toBe(resolve('/'));
  });

  it('uses BOOK_DOCK_PATH as the Book Dock storage path when provided', () => {
    process.env.APP_DATA_PATH = '/data';
    process.env.BOOK_DOCK_PATH = '/books/bookdrop';

    expect(storageConfig().bookDockPath).toBe(resolve('/books/bookdrop'));
  });

  it('uses LIBRARY_BROWSE_ROOT as the library folder picker root when provided', () => {
    process.env.LIBRARY_BROWSE_ROOT = '/books';

    expect(storageConfig().libraryBrowseRoot).toBe(resolve('/books'));
  });

  it('ignores blank LIBRARY_BROWSE_ROOT values from compose defaults', () => {
    process.env.LIBRARY_BROWSE_ROOT = '';

    expect(storageConfig().libraryBrowseRoot).toBe(resolve('/'));
  });

  it('ignores blank BOOK_DOCK_PATH values from compose defaults', () => {
    process.env.APP_DATA_PATH = '/custom/data';
    process.env.BOOK_DOCK_PATH = '';

    expect(storageConfig().bookDockPath).toBe(resolve('/custom/data/book-dock'));
  });

  it('parses positive integers for file-write config and floors decimal values', () => {
    process.env.FILE_WRITE_DEBOUNCE_MS = '1234.99';
    process.env.FILE_WRITE_MAX_CONCURRENT_WRITES = '4.2';

    expect(fileWriteConfig()).toEqual({
      debounceMs: 1234,
      maxConcurrentWrites: 4,
    });
  });

  it('uses file-write fallbacks for zero, negatives, NaN, and Infinity', () => {
    process.env.FILE_WRITE_DEBOUNCE_MS = '0';
    process.env.FILE_WRITE_MAX_CONCURRENT_WRITES = '-7';
    expect(fileWriteConfig()).toEqual({
      debounceMs: 3000,
      maxConcurrentWrites: 2,
    });

    process.env.FILE_WRITE_DEBOUNCE_MS = 'nope';
    process.env.FILE_WRITE_MAX_CONCURRENT_WRITES = 'Infinity';
    expect(fileWriteConfig()).toEqual({
      debounceMs: 3000,
      maxConcurrentWrites: 2,
    });
  });

  it('uses stable podcast defaults and falls back to the JWT secret in development', () => {
    process.env.JWT_SECRET = 'jwt-secret-for-podcasts';

    expect(podcastConfig()).toEqual({
      encryptionKey: 'jwt-secret-for-podcasts',
      maxFeedBytes: 10 * 1024 * 1024,
      maxEpisodeBytes: 2 * 1024 * 1024 * 1024,
      maxConcurrentDownloads: 2,
      requestTimeoutMs: 30_000,
      maxDownloadDurationMs: 6 * 60 * 60_000,
    });
  });

  it('reads dedicated podcast security and resource settings', () => {
    process.env.PODCAST_ENCRYPTION_KEY = ' dedicated-podcast-key ';
    process.env.PODCAST_MAX_FEED_BYTES = '2048';
    process.env.PODCAST_MAX_EPISODE_BYTES = '4096';
    process.env.PODCAST_MAX_CONCURRENT_DOWNLOADS = '4';
    process.env.PODCAST_REQUEST_TIMEOUT_MS = '15000';

    expect(podcastConfig()).toEqual({
      encryptionKey: 'dedicated-podcast-key',
      maxFeedBytes: 2048,
      maxEpisodeBytes: 4096,
      maxConcurrentDownloads: 4,
      requestTimeoutMs: 15_000,
      maxDownloadDurationMs: 6 * 60 * 60_000,
    });
  });

  it('computes OIDC runtime values from seconds with millisecond conversion', () => {
    process.env.OIDC_STATE_TTL_SECS = '11';
    process.env.OIDC_DISCOVERY_CACHE_TTL_SECS = '22';
    process.env.OIDC_JWKS_CACHE_TTL_SECS = '33';
    process.env.OIDC_CLOCK_TOLERANCE_SECS = '44';
    process.env.OIDC_TOKEN_EXCHANGE_TIMEOUT_MS = '5555';

    expect(oidcRuntimeConfig()).toEqual({
      stateTtlMs: 11000,
      discoveryCacheTtlMs: 22000,
      jwksCacheTtlMs: 33000,
      clockToleranceSecs: 44,
      tokenExchangeTimeoutMs: 5555,
    });
  });

  it('uses OIDC defaults when values are missing or invalid', () => {
    process.env.OIDC_STATE_TTL_SECS = '';
    process.env.OIDC_DISCOVERY_CACHE_TTL_SECS = '0';
    process.env.OIDC_JWKS_CACHE_TTL_SECS = '-1';
    process.env.OIDC_CLOCK_TOLERANCE_SECS = 'abc';
    process.env.OIDC_TOKEN_EXCHANGE_TIMEOUT_MS = 'Infinity';

    expect(oidcRuntimeConfig()).toEqual({
      stateTtlMs: 300000,
      discoveryCacheTtlMs: 3600000,
      jwksCacheTtlMs: 21600000,
      clockToleranceSecs: 30,
      tokenExchangeTimeoutMs: 10000,
    });
  });
});
