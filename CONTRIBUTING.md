# Contribution Guide

- [Contribution Guide](#contribution-guide)
  - [Before you get started](#before-you-get-started)
    - [Code of Conduct](#code-of-conduct)
  - [Your First Contribution](#your-first-contribution)
    - [Code style](#code-style)
    - [License Header](#license-header)
    - [Find a good first topic](#find-a-good-first-topic)
  - [Setting up your development environment](#setting-up-your-development-environment)
    - [Fork the project](#fork-the-project)
    - [Clone the project](#clone-the-project)
    - [New branch for a new code](#new-branch-for-a-new-code)
    - [Test](#test)
    - [Commit and push](#commit-and-push)
    - [Create a Pull Request](#create-a-pull-request)
    - [Sign the CLA](#sign-the-cla)
    - [Get a code review](#get-a-code-review)

## Before you get started

### Code of Conduct

Please make sure to read and observe our [Code of Conduct](./CODE_OF_CONDUCT.md).

## Your First Contribution

### Code style

- We follow [Go Code Review](https://github.com/golang/go/wiki/CodeReviewComments)
- At a minimum, use `go fmt` to format your code before committing. Ideally you should use `trunk`
  as our CI will run `trunk` on your code
- If you see _any code_ which clearly violates the style guide, please fix it and send a pull
  request. No need to ask for permission
- Avoid unnecessary vertical spaces. Use your judgment or follow the code review comments
- Wrap your code and comments to 120 characters, unless doing so makes the code less legible

### License Header

Every new source file must begin with a license header.

Most of Dgraph, Badger, and the Dgraph clients (dgo, dgraph-js, pydgraph and dgraph4j) are licensed
under the Apache 2.0 license:

```sh
/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */
```

### Find a good first topic

You can start by finding an existing issue with the
[good first issue](https://github.com/dgraph-io/badger/labels/good%20first%20issue) or
[help wanted](https://github.com/dgraph-io/badger/labels/help%20wanted) labels. These issues are
well suited for new contributors.

## Setting up your development environment

- [Install Go 1.25.0 or above](https://golang.org/doc/install).
- Install
  [trunk](https://docs.trunk.io/code-quality/overview/getting-started/install#install-the-launcher).
  Our CI uses trunk to lint and check code, having it installed locally will save you time.

### Fork the project

- Visit https://github.com/dgraph-io/badger
- Click the `Fork` button (top right) to create a fork of the repository

### Clone the project

```sh
git clone https://github.com/$GITHUB_USER/badger
cd badger
git remote add upstream git@github.com:dgraph-io/badger.git

# Never push to the upstream main
git remote set-url --push upstream no_push
```

### New branch for a new code

Get your local main up to date:

```sh
git fetch upstream
git checkout main
git rebase upstream/main
```

Create a new branch from the main:

```sh
git checkout -b my_new_feature
```

And now you can finally add your changes to project.

### Test

Build and run all tests:

```sh
./test.sh
```

### Commit and push

Commit your changes:

```sh
git commit
```

When the changes are ready to review:

```sh
git push origin my_new_feature
```

### Create a Pull Request

Just open `https://github.com/$GITHUB_USER/badger/pull/new/my_new_feature` and fill the PR
description.

### Sign the CLA

Click the **Sign in with Github to agree** button to sign the CLA.
[An example](https://cla-assistant.io/dgraph-io/badger?pullRequest=1377).

### Get a code review

If your pull request (PR) is opened, it will be assigned to one or more reviewers. Those reviewers
will do a code review.

To address review comments, you should commit the changes to the same branch of the PR on your fork.
