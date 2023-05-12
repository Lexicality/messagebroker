# Hypothetical Redis Queue Message Broker

While no one sane would create an entire queueing system from scratch utilising a
database that already has a queueing system for a stack that's running in AWS (which
also has a queueing system) it's fun to imagine that such a person might exist.

And if that person did exist, their frame of mind might suggest that any broker they
built for this system would not have been constructed in a particularly good or easy to
understand way, and probably would not be particularly fault tolerant either.

As a fun challenge, I've decided to pretend all of this is true and write out what such
a system would look like, and then build my own message broker in Rust that implements
it - but nicely!
