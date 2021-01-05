## Push to PyPI

1. Update version number in sdf/_version.py

2. Run bumpversion

```bash
bumpversion patch
```

3. Push to GitHub
```
git push && git push --tags
```

4. Create a new release on GitHub
