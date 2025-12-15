# Badger Release Process

This document outlines the steps needed to build and push a new release of Badger.

1. Have a team member "at-the-ready" with github `writer` access (you'll need them to approve PRs).
1. Create a new branch (prepare-for-release-vXX.X.X, for instance).
1. Update dependencies in `go.mod` for Ristretto, if required.
1. Update the CHANGELOG.md. Sonnet 4.5 does a great job of doing this. Example prompt:
   `I'm releasing vXX.X.X off the main branch, add a new entry for this release. Conform to the "Keep a Changelog" format, use past entries as a formatting guide. Run the trunk linter on your changes.`.
1. Validate the version does not have storage incompatibilities with the previous version. If so,
   add a warning to the CHANGELOG.md that export/import of data will need to be run as part of the
   upgrade process.
1. Commit and push your changes. Create a PR and have a team member approve it.
1. Once your "prepare for release branch" is merged into main, on the github
   [releases](https://github.com/dgraph-io/badger/releases) page, create a new draft release.
1. Start the deployment workflow from
   [here](https://github.com/dgraph-io/badger/actions/workflows/cd-badger.yml).

   The CD workflow handles the building and copying of release artifacts to the releases area.

1. For all major and minor releases (non-patches), create a release branch. In order to easily
   backport fixes to the release branch, create a release branch from the tag head. For instance, if
   we're releasing v4.9.0, create a branch called `release/v4.9` from the tag head (ensure you're on
   the main branch from which you created the tag)
   ```sh
   git checkout main
   git pull origin main
   git checkout -b release/v4.9
   git push origin release/v4.9
   ```
1. Splash the "go index" cache to ensure that the latest release is available to the public with

```sh
go list -m github.com/dgraph-io/badger@vX.X.X
```

1. If needed, create a new announcement thread in the
   [Discussions](https://github.com/orgs/dgraph-io/discussions) forum for the release.
