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
	Package debug abstracts info and events logging for easy toggling.

	The purpose of this package is to allow flexibility to performance configurations,
	while keeping the main source undisturbed.

	To attach a new standard logger, it must implement the StdLogger interface. This will be
	used with typical info outputs.

	Example using LogRUs:

		log := logrus.New()
		debug.Use(log)

	To attach a new events logger, it must implement the EventLogger interface. This is used for
	long-running operations that potentially generate lots of output.

	Example using x/net/trace:

		elog := trace.NewEventLog("Dgraph", "Parse")
		debug.Use(elog)

	For convenience, the func `UseCompatStdLogger()` and `UseCompatEventLogger()` will setup the
	Go standard logging and trace event logging.

*/
package debug
