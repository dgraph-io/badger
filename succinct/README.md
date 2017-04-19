**On this page I will keep notes on implementing Succinct store.**

## Overview

Succinct is a data store that enables efficient queries directly on a compressed representation of flat files. To achieve that Succinct uses various compression techniques.

### References

Main paper:
https://www.usenix.org/system/files/conference/nsdi15/nsdi15-paper-agarwal.pdf

More detailed technical report (appendix contains very useful information about data structures):
https://people.eecs.berkeley.edu/~anuragk/succinct-techreport.pdf

Blog:
http://succinct.cs.berkeley.edu/wp/wordpress/

Video:
https://www.usenix.org/node/188995

###Interface
```
f = compress(file)
append(f, buffer)
buffer = extract(f, offset, len)
cnt = count(f, str)
[offset1, ...] = search(f, str)
[offset1, ...] = rangesearch(f, str1, str2)
[[offset1, len1], ...] = wildcardsearch(f, prefix, suffix, di
```

### Data structures

Generally, Succinct uses 4 data structures to represent the file:

**Array of Suffixes (AoS)** - https://en.wikipedia.org/wiki/Suffix_array
**Input2AoS** - mapping from input to AoS;
**AoS2Input** - mapping from AoS to input;
**NextCharIdx** - for each suffix in AoS, the position of next suffix of the input file in the AoS.

These structures allow efficient (roughly at most logarithmic) implementations of all the operations. However, they use lots of memory (n^2 for AoS itself), so the whole idea of the paper is to compress the information in the files, so that we don't loose the speed of the operations.


## Compressing AoS

First, we observe that we need only the first char for each suffix. If we want to retrieve next char, we can just use `NextCharIdx` table to retrieve the next position in the AoS that contains the next char and so on.

Second, now we look at the first chars we have in the suffix array. The array is sorted by the first char, so it has many duplicates like in `[a, a, a, b, b, n]`. So instead of keeping the whole array we just need to track on which position the next letter begins. More precisely, we store this information as 2 arrays: one contains sorted alphabet (called `characters`) and the second one at the position `i` contains the index where char `characters[i]` begins in the suffix array.

## Compressing `AoS2Input`

**Note 1** We can represent a permutation either by target positions or an order.

Let's say we have a permutation `p` (1-1 function from some elements `E` to `0..(|E| - 1)`). The most natural way we can encode it is to use an array `a` in which `a[i] = p(i)`.

We could also define the permutation by introducing next (`->`) relation such that for `x, y in E` we say `x -> y` iff `p(x) + 1 = p(y)`. In other words, we defined order of elements in `E` such that `p` is determined by that order. We can store `->` as an array `n` such that `n[i] = j if i -> j for some j else -1 if there does not exist j s. t. i -> j`.

**Note 2** We can use order representation of permutation to fill missing values in target representation.

Now, we observe that if we have only some values in the array `a` we can use `n` to fill the missing values. Precisely, if `a[i]` is missing, we check the next element after `i`: `n[i]` and  `a[i] = a[n[i]] - 1`. If `a[n[i]]` is missing too, we check `a[n[n[i]]]` and so on.


Now, suffixes of a word `w` is the set `E` (order is defined lexicographically). Permutation `p` orders first lettters of `E` to obtain the innput word `w`. `AoS2Input` is `p` stored in the natural way and `NextCharIdx` defines it using the order representation of `p`.

So, as long as we have `NextCharIdx`, we can store only some values in the `AoS2Input` array and that's what we are going to do.

### Side note: value sampling compression

The easiest way to compress `AoS2Input` is by using every `k`-th value. This is good, but Succinct does a bit better. It stores only `k`-th value, but chooses the values to store not by index but by value. So stores only values that are a multiple of `k`. As a result they need lower number of bits for each value, because they substitute each value `v` with `v/k`.

Note: `k` is called `α` in the paper.

## Compressing `NextCharIdx`

TODO
