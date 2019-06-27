# Serialization Versioning: Semantic Versioning for databases

Semantic Versioning, commonly known as SemVer, is a great idea that has been very widely adopted as a way to decide how to name software versions. The whole concept is very well summarized on semver.org with the following lines:

> Given a version number MAJOR.MINOR.PATCH, increment the:
> 
> 1. MAJOR version when you make incompatible API changes,
> 2. MINOR version when you add functionality in a backwards-compatible manner, and
> 3. PATCH version when you make backwards-compatible bug fixes.
> 
> Additional labels for pre-release and build metadata are available as extensions to the MAJOR.MINOR.PATCH format.

I have used and benefitted from this clear naming schema for many years, but recently I did find a small concern which drove me to propose a slightly different naming schema, specifically designed for libraries that provide encoding and decoding mechanisms which are not part of their API.

Let’s imagine that you’re the maintainer of a little library that handles a TODO list. The API for version `v1.0.0` is the following (I went with Go, but hopefully you get what it’s doing).

```go
package todo

type TaskManager struct {
   Path string
}

type Task struct {
   ID int
   Text string
   Done bool
}

func (tm *TaskManager) Get(id int) (*Task, error)
func (tm *TaskManager) Add(text string) (*Task, error)
func (tm *TaskManager) Save(id int, task *Task) error
func (tm *TaskManager) Remove(id int) error
```

These tasks are transparently stored in a file with the path given to the TaskManager.

A new version appears where, for performance sake, the format in which the tasks are stored on disk changes from XML to JSON. The API has not changed at all, has it? So you release a new version, and following the Semantic Versioning naming schema, you call it `v1.0.1`.

Now imagine you have a user with a couple of billion tasks already stored on their huge disks, they see that a new patch version has been released, and automatically update to the next version. Surprise, all of their systems are down!
What happened? Well, the new version expects all files to contain JSON, but the ones on disk are still in XML so they won’t be parsed.

## Is SemVer flawed or am I holding it incorrectly?

There are two different points of view on this issue: either semantic versioning is a flawed naming strategy, or we’re missing something out.

The easy way is to blame others, so let’s say that Semantic Versioning is wrong. How is it wrong? Well, it fails to identify that “major” changes are not necessarily major or even visible on a library’s API.
But there’s a second way of seeing this, maybe our API *should* include the way data is serialized — regardless of whether the data is going to be sent through the network, or stored in disks, there’s a clear impact of that format changing over time.

Do you know what kind of libraries suffer from this? Key value stores like BadgerDB definitely do.

But there’s one more aspect in which Semantic Versioning doesn’t necessarily match what databases do most of the time. It turns out that a backward-incompatible change on the API, e.g. renaming the Done field in Task to Accomplished, might be actually a minor change — as it can be easily automated with little risk. Why should we call say our library is now v2 just because a field name has changed?

## Serialization Versioning

This is why I’m proposing a new naming mechanism, heavily inspired by Semantic Versioning, but optimized for database libraries and similar: Serialization Versioning.

Serialization Versioning also uses 3 numbers and also calls them MAJOR.MINOR.PATCH, but the semantics of the numbers are slightly modified:

Given a version number MAJOR.MINOR.PATCH, increment the:

- MAJOR version when you make changes that require a transformation of the dataset before it can be used again.
- MINOR version when old datasets are still readable but the API might have changed in backward-compatible or incompatible ways.
- PATCH version when you make backward-compatible bug fixes.

Additional labels for pre-release and build metadata are available as extensions to the MAJOR.MINOR.PATCH format.

Following this, renaming the Done field would lead us to v1.1.0 which implies a minor migration cost. On the other hand, migrating from XML to JSON as our encoding format would directly move the package to v2.0.0 as the cost of this migration could be quite large depending on the dataset size.
