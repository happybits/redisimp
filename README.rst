Redis Import Tool
=================

This project allows you to quickly and safely import data from other redis hosts
 into a running redis instance or cluster.

```
redisimp -s 127.0.0.1:6379 -d 127.0.0.1:6380
```

The script will take all the keys from the source `127.0.0.1:6379` and copy
them into the destination `127.0.0.1:6380`.



It also allows you to load data stored
in RDB files by loading the rdb file into a temporarily embedded redis instance
via redislite.

```
redisimp -s ./dump.rdb -d 127.0.0.1:6380
```

There are other tools out there that do this but usually it involves parsing
an rdb file directly and often mistakes are made in character encodings for
keys or values. In contrast, *redisimp* uses the redis commands of dump and
restore to copy the data safely between instances.

I experimented with the MIGRATE command but I found it to be less flexible and
with no great speed improvements. I wanted to create a tool that would not
change the source database and could load data over the top of existing data
in a destination database.






