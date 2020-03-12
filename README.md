egress
======

![Egress](egress.svg)
Because the world needs another Python Postgress driver... :P

But why??
---------

The main difference is this tries to use the binary protocol as much as possible, instead of relying on text parsing.

Additionally, it uses the `PQexecParams` interface to pass the query and its parameters separately to the server.
