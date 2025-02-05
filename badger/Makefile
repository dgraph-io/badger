#
# SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
# SPDX-License-Identifier: Apache-2.0
#

GOOS          ?= $(shell go env GOOS)
GOARCH        ?= $(shell go env GOARCH)

.PHONY: badger

all: badger

badger: 
# 	build badger binary
	@go build --tags=jemalloc -o badger-$(GOOS)-$(GOARCH) .
