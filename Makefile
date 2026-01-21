#
# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0
#

USER_ID      = $(shell id -u)

# Set TMPDIR, jemalloc will be built here
ifdef TMPDIR
	# no-op
else
	TMPDIR := /tmp
endif

# jemalloc stuff
JEMALLOC_VERSION = 5.3.0
JEMALLOC_BUILD = $(TMPDIR)/jemalloc-$(JEMALLOC_VERSION)-u$(USER_ID)/build
JEMALLOC_TARGET = $(TMPDIR)/jemalloc-$(JEMALLOC_VERSION)-u$(USER_ID)/target
JEMALLOC_INCLUDE = $(JEMALLOC_TARGET)/include
HAS_JEMALLOC = $(shell test -f ${JEMALLOC_TARGET}/lib/libjemalloc.a && echo "jemalloc")
JEMALLOC_URL = "https://github.com/jemalloc/jemalloc/releases/download/$(JEMALLOC_VERSION)/jemalloc-$(JEMALLOC_VERSION).tar.bz2"
export CGO_CFLAGS = -I$(JEMALLOC_INCLUDE)
export CGO_LDFLAGS = $(JEMALLOC_TARGET)/lib/libjemalloc.a -ldl

.PHONY: all badger test jemalloc dependency

badger: jemalloc
	@echo "Compiling Badger binary..."
	@$(MAKE) -C badger badger
	@echo "Badger binary located in badger directory."

test: jemalloc
	@echo "Running Badger tests..."
	@./test.sh

jemalloc:
	@if [ -z "$(HAS_JEMALLOC)" ] ; then \
		mkdir -p ${JEMALLOC_BUILD} && cd ${JEMALLOC_BUILD} ; \
		echo "Downloading jemalloc..." ; \
		curl -s -L ${JEMALLOC_URL} -o jemalloc.tar.bz2 ; \
		tar xjf ./jemalloc.tar.bz2 ; \
		cd jemalloc-$(JEMALLOC_VERSION) ; \
		./configure --prefix=${JEMALLOC_TARGET} --with-jemalloc-prefix='je_' --with-malloc-conf='background_thread:true,metadata_thp:auto'; \
		make install ; \
	fi

dependency:
	@echo "Installing dependencies..."
	@sudo apt-get update
	@sudo apt-get -y install \
    	ca-certificates \
    	curl \
    	gnupg \
    	lsb-release \
    	build-essential \
    	protobuf-compiler \
