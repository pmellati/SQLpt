[![Build Status](https://travis-ci.org/pmellati/SQLpt.svg?branch=master)](https://travis-ci.org/pmellati/SQLpt)

# SQLpt

SQLpt will let you construct gigantic SQL expressions in a readable manner. With SQLpt, you will achieve readability by:

* Not writing your entire SQL as a single gigantic expression: break your SQL down into thoughtfully named `val`s.
* Using functions to inspect and manipulate your SQL: package common patterns into thoughtfully named `def`s.
* Type safety: let your compiler (and IDE) tell you what's what.

The final output of the library will simply be a piece of SQL string.

Future ambitions?

* Support for testing
  * Unit test portions of your larger SQL
  * Output to `List` based computations (rather than a SQL string), for fast execution of tests, so that you can use ScalaCheck, for instance.
