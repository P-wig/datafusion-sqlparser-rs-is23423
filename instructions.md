# Project

In this project, you will provide a database(s) as a service.  This is
NOT a group project, but an individual effort.

We covered graph databases (and talk about Neo4j, which is one
implementation), but if you need more resources check this wiki
article: https://en.wikipedia.org/wiki/Graph_database and this Neo4j
page: https://neo4j.com/docs/getting-started. You can also find free
books on the topic.


## You

Your UT EID plays a role when deciding what language to use and what
features to implement. It is very important to do this computation
properly, as we will grade only those that have done computation
properly.

```
hash(to_lower(eid)) => your group
```

hash function is the following:

```
hash(eid) = ascii(eid[0]) + ascii(eid[1]) + ... ascii(eid[n-1])
```

For example:
```
hash(sb43278) = 477
```
My Project Requirements:
```
hash(is23423) = ascii('i') + ascii('s') + ascii('2') + ascii('3') + ascii('4') + ascii('2') + ascii('3')
              = 105 + 115 + 50 + 51 + 52 + 50 + 51 = 474
Go web framework: 474 % 4 = 2 → Echo framework
Database: 474 % 2 = 0 → SQLite
Table type: 474 % 2 = 0 → table that stores a list of columns

In the following section, we will compute problem that you should
solve by the following formula:

```
your group % N => your item
```

N will be defined in each section below, so pay attention.


## High level

You will develop a client-server application that can process user
queries (Queries), but data will be kept in a relational database.


## Components

This section describes components of the system. The design is modular
and you can easily replace any part of the system with a more robust
implementation.

```
 --------------------------                           -----------------
| client (textbox: cypher) |--->(1) check cypher --->| graph db (neoj) |
 --------------------------                           -----------------
   ^
   | REST: /query/my_cypher
   v                                 
 ------------------      ---------------------------------------------------
| db service       |--->| query transformer/transform cypher into sql query |
 ------------------      ---------------------------------------------------
   ^
   | send SQL to relational db
   v
 ------------------------
| db   Mysql or SQLite   |
 ------------------------
```

* client - client application that has API for sending
  queries/requests to server

* db service - web server serving requests/queries

* query transformer - transforms Cypher into SQL

* graph db - actual graph database

* db - actual relational database

In the following subsection we describe each of the components in more
detail.


### Client

Client application is responsible for:
* accepting user input (a text box and a button to send a request)
* checking correctness of the query
* sending the request to server
* accepting the response (json)
* storing and printing the results

Client application should be written in Smalltalk.

Input by the user will be a Cypher query; the query might be valid or
invalid. You can decide in what way to accept the input, but we
suggest that you use a dedicated textbox (and a button to send the
request).  To check if the query is valid, you should use a local
Neo4j instance to let you know if the query is valid.

You should prepare a request for the server and send it (json).

Upon receiving a response (json), you should store the response into
an instance of an in-memory Table inside Smalltalk (similar to the one
we developed in class). We define N=2 in this case: 0-table that
stores a list of columns; 1-table that stores a list of rows.

The response should be printed on the Transcript (reasonably)
formatted as a table.


### db service

Your server should be written in Go.  You should use (0) standard
packages, (1) Gin, (2) Echo, (3) Chi (we define N=4).  The server will
accept requests and serve them.  If the request has any error,
appropriate message will be sent to the client.  If the request is
valid, it will be sent to the transformer and then to the database,
results will be accepted, packed, and sent to the user.

We leave to you to design communication protocol between the server
and database, e.g., log files, inter-process messages.

Client and server should communicate using REST. You have freedom to
define end points and arguments.


### Query transformer

Once server is happy with the request, it will call a local tool
written in Rust.  The tool will convert Cypher into SQL query.  You
have to use https://github.com/apache/datafusion-sqlparser-rs.  You
have to change this create to add parsing for cypher queries that you
want to support and then modify that same crate to desugar your
constructs into existing constructs.

Grammar for Cypher is here
(https://s3.amazonaws.com/artifacts.opencypher.org/M23/Cypher.g4) in
case you need to reference it.  We of course do not expect that you
will translate every possible Cypher into SQL, but we want to see
reasonable mapping decisions and several solid examples.


### Database

You should set up and use Neo4j as your actual graph db.  As for the
relational database, you should use (0) SQLite or (1) MySQL (we define
N=2).


## Testing

You should have tests for each part of your code; without tests, code
will be considered non-existent.


## Benchmarking

Graduate version only.

Write a Bash script(s) that will collect benchmarking data for 100
queries. Each query should be run 100 times and averages should be
computed.


## Software

Your implementation should work under the following configuration:
* Linux (any recent distribution)
* Oracle Java 25
* Neo4j v5.x (cloud Graph Database Self-Managed community edition https://neo4j.com/deployment-center)
* Smalltalk Pharo 13 (https://pharo.org/download)
* Go 1.21+
* Rust 1.90.0
* Python 3.8+
* gcc 9.4.0+
* OCaml 4.08.1+
* If you pick a language not in the list, please contact us for the version number
* CMake 3.16+
* SQLite 3.31+

If you create a Docker image with required software and demo
everything using it, you will receive extra points.  Keep Smalltalk
outside.


## Repository and Steps

Keep all your code in the same repo as defined in the syllabus.pdf (in
the `project`) subdirectory.  Note that you should fork
https://github.com/apache/datafusion-sqlparser-rs and share also that
with us (suffix the fork with your ut id).  You might also need a
separate repo for Smalltalk (suffix your repo name with "smalltalk").
To illustrate.  Let's say your default repo is pp-abc334.  Then you
would have also datafusion-sqlparser-rs-abc334 and
pp-abc334-smalltalk.

We will split the project in three parts.

*Part 1*
Due October 19.

Smalltalk client.

Expected to deliver:

* Your CI is up and running (although you do not need to have Smalltalk in the CI)
* Your Smalltalk client is complete

Do not include unnecessary binaries into your repository.

*Part 2*
Due November 14.

Initial service code and initial transformer.

*Part 3*
Due December 1.

Everything else.

Points will be distributed (approximately) according the the following:

* bash/ci 5
* client 15
* neo 20
* service 20
* transformer 20
* testing 10
* benchmarking 10
