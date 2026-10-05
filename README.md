# Day 12 — Cache and Persist

## Overview

This project demonstrates how Apache Spark uses caching and persistence to avoid recomputing an RDD when the same dataset is reused by multiple actions.

The practical scenario is a cleaned transaction dataset that is reused to generate multiple reports.

## Objectives

- Understand `cache()`
- Understand `persist()`
- Compare `cache()` and `persist()`
- Experiment with different Spark storage levels
- Understand when caching can improve performance
- Understand when caching can hurt performance
- Reuse a cleaned RDD across multiple actions

## Scenario

Imagine a transaction-processing system where raw transactions are cleaned once and the cleaned dataset is used for multiple reports.

Instead of recomputing the cleaning transformations for every report, Spark can keep the RDD in memory or memory and disk.

## Concepts

### cache()

`cache()` stores an RDD using Spark's default storage level so that subsequent actions can reuse the computed data.

### persist()

`persist()` allows us to explicitly choose a storage level for the RDD.

For example:

```scala
rdd.persist(StorageLevel.MEMORY_AND_DISK)
