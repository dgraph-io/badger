/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
	Package debug encapsulates info and events logging for easy toggling.

	The purpose of this package is to allow flexibility to performance configurations,
	while keeping the main source undisturbed. When using this package for logging,
	the Go build tool will produce a binary with logging disabled by default. To toggle
	logging you must use build tags. See: go help build

	Build tags:
		- debugcompat: enables info logging (log) and event logging (x/net/trace.EventLog). This is
			the compatible behavior following traditional builds.
		- debugevents: enables only event logging.

	Examples:

		go build -tags debugcompat

		go build -tags debugevents

	Note: Use one or none tags when building.
*/
package debug
