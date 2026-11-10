import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { resolveBookSearchMigrationsFolder } from './book-search-migrations';

describe('book search migration assets', () => {
  let directory: string;
  beforeEach(async () => {
    directory = await mkdtemp(join(tmpdir(), 'book-search-assets-'));
  });
  afterEach(async () => {
    await rm(directory, { recursive: true, force: true });
  });
  async function bundle(path: string) {
    await mkdir(join(path, 'meta'), { recursive: true });
    await writeFile(join(path, 'meta', '_journal.json'), '{}');
  }

  it('resolves a packaged external directory without relying on the current working directory', async () => {
    const folder = join(directory, 'search-migrations');
    await bundle(folder);
    expect(resolveBookSearchMigrationsFolder(join(directory, 'dist', 'scripts'), join(directory, 'elsewhere'))).toBe(folder);
  });

  it.each([
    { scripts: 'dist/scripts', assets: 'dist/db' },
    { scripts: 'src/scripts', assets: 'src/db' },
    { scripts: 'dist/scripts', assets: 'dist/src/db' },
  ])('resolves $assets generated assets for $scripts', async ({ scripts, assets }) => {
    const folder = join(directory, assets, 'search-migrations');
    await bundle(folder);
    expect(resolveBookSearchMigrationsFolder(join(directory, scripts), join(directory, 'elsewhere'))).toBe(folder);
  });

  it('rejects a directory whose migration journal was omitted from the package', async () => {
    await mkdir(join(directory, 'search-migrations'), { recursive: true });
    expect(() => resolveBookSearchMigrationsFolder(join(directory, 'dist', 'scripts'), directory)).toThrow('assets are missing');
  });
});
