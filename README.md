# Simple database on go (WIP)

## B tree

`copy-on-write` model

- to change node it is being copied (convenient for snapshot isolation and simpier implementation)

`page format`:

```
| type | nkeys | pointers   | offsets    | key-values
|  2B  |   2B  | nkeys * 8B | nkeys * 2B |     ...

| klen | vlen | key | val |
|  2B  |  2B  | ... | ... |
```

## KV store

### Free list

free list is used for recycling and reusing pages

it is and unrolled linked list, that means that each page contains multiple pages, items are appended to the tail and consumed from the head
