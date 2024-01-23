# zikade

[![ProbeLab](https://img.shields.io/badge/made%20by-ProbeLab-blue.svg)](https://probelab.io)
[![Libp2p](https://img.shields.io/badge/project-libp2p-yellow.svg)](https://libp2p.io)
[![Build status](https://img.shields.io/github/actions/workflow/status/probe-lab/zikade/go-test.yml?branch=main)](https://github.com/probe-lab/zikade/actions)
[![GoDoc](https://pkg.go.dev/badge/github.com/probe-lab/zikade)](https://pkg.go.dev/github.com/probe-lab/zikade)
[![Discourse posts](https://img.shields.io/discourse/https/discuss.libp2p.io/posts.svg)](https://discuss.libp2p.io)

> A Go implementation of the [libp2p Kademlia DHT specification](https://github.com/libp2p/specs/tree/master/kad-dht).

Zikade is a new implementation of the go-libp2p Kademlia DHT, designed to be a successor to the existing implementation, [go-libp2p-kad-dht](https://github.com/libp2p/go-libp2p-kad-dht), which is now over [nine years old](https://github.com/libp2p/go-libp2p-kad-dht/commit/71d5f6fc8d16c458ae3d37b50f8477eff53e5390). 

It uses a state machine-oriented execution model, with a single worker coordinator to achieve more predictable behaviour. The new model allows for cleaner resource bounding, effective task prioritisation, and improved performance management. It also enables debugging and testing to be more deterministic and simplifies simulation of new behaviours. 

## Table of Contents

- [Status](#status)
- [Background](#background)
  - [Motivation](#motivation)
  - [Compatibility](#compatibility)
- [Install](#install)
- [Maintainers](#maintainers)
- [Contributing](#contributing)
- [License](#license)

## Status

This project is under active development and has not reached feature completeness. You may experience unexpected behaviour in its current state. We've not yet made optimisation a priority, so some aspects of the software may run slower or consume more resources than expected. Community input and contributions are welcomed as we continue to iterate and improve.

## Background

### Motivation

The [go-libp2p-kad-dht](https://github.com/libp2p/go-libp2p-kad-dht) Kademlia implementation has reached a stage where adding significant features or improving algorithms has become increasingly difficult. It presents several core challenges:

 - Years of feature additions and core functionality iterations have increased performance but, as a side-effect, also added substantially to the complexity. It's now a daunting task for newcomers to understand the codebase and contribute meaningfully.
 - Due to the extensive parallelisation in different sections, writing stable unit tests has turned into a considerable challenge. Unreliable tests due to concurrency issues have become a bottleneck in the development pipeline.
 - A scarcity of reliable unit tests has made it difficult to run meaningful performance evaluations. Ambiguous results, like those seen with [Bitswap's Provider Search delay](https://github.com/ipfs/kubo/pull/9530), further cloud the picture.
 - The current architecture has accumulated a significant amount of technical debt. For instance, Kademlia should internally only handle Kademlia identifiers (256-bit bitstrings), but it currently uses strings, which is not only a deviation from the original design but also an unnecessary complexity.

In light of these challenges, Zikade aims to:

 - Make the code easier to understand and more straightforward to modify. 
 - Eliminate the flakiness associated with unconstrained concurrency in unit tests.
 - Provide a modular and extensible foundation for future DHT enhancements.
 - Allow the code to be easily tuned for performance optimisations.
 - Give more control over allocation and use of resources.
 - Prioritise execution of tasks such that 
   - local work is prioritized over incoming work
   - ongoing work is progressed before initating new work
   - work is bounded and backpressure is applied

You can read more about the motivation for revising the DHT implementation on the [IPFS Blog](https://blog.ipfs.tech/2023-09-amino-refactoring/).

### Compatibility

Where possible, Zikade will remain compatible with [go-libp2p-kad-dht](https://github.com/libp2p/go-libp2p-kad-dht). There are no breaking protocol changes planned, and we adhere to the standard [routing.Routing](https://pkg.go.dev/github.com/libp2p/go-libp2p/core/routing#Routing) interface used by Kubo and other application. The existing libp2p Kademlia implementation has been battle tested through many years of use, and we want to take advantage of the learnings from that real-world usage while improving the ergonomics and clarity of the code. However, we will be taking this opportunity to look closely at the current code interfaces and to propose improved or new ones.

## Install

```sh
go get github.com/probe-lab/zikade
```

## Maintainers

 - [Dennis Trautwein](https://github.com/dennis-tra)
 - [Guillaume Michel](https://github.com/guillaumemichel)
 - [Ian Davis](https://github.com/iand)

See [CODEOWNERS](./CODEOWNERS).

## Contributing

Contributions are welcome! 

Please take a look at [the issues](https://github.com/probe-lab/zikade/issues).

This repository is part of the IPFS project and therefore governed by our [contributing guidelines](https://github.com/ipfs/community/blob/master/CONTRIBUTING.md).

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
