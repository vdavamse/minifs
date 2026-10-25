# Releasing

Releases go to [PyPI](https://pypi.org/project/minifs/) from `.github/workflows/wheels.yml`, through Trusted
Publishing: no API token, PyPI trusts that workflow running in the `pypi` GitHub environment. A version can never be
uploaded twice, even after it's deleted, so check it before you tag.

## One-time setup

1. On PyPI, under Your account > Publishing, add a trusted publisher (a *pending* publisher before the first upload):
   project `minifs`, owner `vdavamse`, repository `minifs`, workflow `wheels.yml`, environment `pypi`.
2. On GitHub, under Settings > Environments, create `pypi`. Add yourself as a required reviewer, so every upload waits
   for your approval. Limit its deployment tags to `v*` as well.

## Cutting a release

1. Set the version in `crates/minifs-regex/Cargo.toml`. It is the package's version too. Use Cargo's spelling: maturin
   turns `0.2.0-alpha.1` into PEP 440's `0.2.0a1`, `-beta.N` into `bN` and `-rc.N` into `rcN`. pip skips pre-releases
   unless asked (`pip install --pre minifs`).
2. Run `cargo metadata --format-version 1 > /dev/null` to update `Cargo.lock`, then open a PR and merge it.
3. Tag the commit that merged the bump, by its SHA, not whatever `main` points to by then, and push the tag:

   ```sh
   git fetch origin && git log --oneline origin/main   # find the bump's merge commit
   git tag v0.2.0a1 <sha> && git push origin v0.2.0a1
   ```

4. The workflow builds the wheels and the sdist and smoke-tests each one. Next, `check-tag` checks that the tagged
   commit is on `main` and that every file carries the tag's version. Only then does `publish` wait for approval on the
   `pypi` environment. Approve it to upload.
5. Check <https://pypi.org/project/minifs/>, then install the release in a clean environment and load the extension:
   `uv run --no-project --with "minifs[binary]==0.2.0a1" python -c "import minifs, minifs._regex as r; print(minifs.__version__); assert not r.HAS_ORACLE"`.

If `publish` fails for a setup reason (a missing environment, or a mismatched trusted publisher), fix the setting and
rerun the job. A rerun uploads the files already built from the tagged commit, so a problem in the code or metadata
needs a new commit: delete the tag (`git push origin :v0.2.0a1 && git tag -d v0.2.0a1`), merge the fix, and tag again.
That works only while nothing has reached PyPI. If a broken version was uploaded, yank it on PyPI and release the next
version, since the number can't be reused.
