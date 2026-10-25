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
3. Tag the merge commit on `main` with the PEP 440 version and push the tag:

   ```sh
   git switch main && git pull
   git tag v0.2.0a1 && git push origin v0.2.0a1
   ```

4. The workflow builds the wheels and the sdist, and smoke-tests each one. Then `publish` checks that every file
   carries the tag's version and waits for approval on the `pypi` environment. Approve it to upload.
5. Check <https://pypi.org/project/minifs/>, then install from a clean environment:
   `uv run --no-project --with "minifs==0.2.0a1" python -c "import minifs; print(minifs.__version__)"`.

If `publish` fails before the upload, fix the problem and rerun the job; nothing reached PyPI. If a broken version was
uploaded, yank it on PyPI and release the next version. The number can't be reused.
