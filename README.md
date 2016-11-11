[![Build Status](https://travis-ci.org/pmellati/SQLpt.svg?branch=master)](https://travis-ci.org/pmellati/SQLpt)

# SQLpt

SQLpt (pronounced as "sculpt") is a Scala DSL for generating SQL in a type-safe and composable manner.

## Type safety

SQLpt is (highly) type-safe, which means fewer run-time failures. This is especially important when working on long running queries. The last thing you want is to fix each typing issue by running your query for an hour. With SQLpt, bad code hopefully won't compile.

Moreover, it goes without saying that type-safety will dramatically improve the comprehensibility of your code, especially in an IDE, where you can hover over a value or variable to see its type.

Last but not least, types can direct and assist with development. The simplest example of this would be typing `.` after a variable and having your IDE tell you what operations are available on it.

## Composability

The biggest impediment to readability for normal SQL is that you cannot break it down (easily and fully). As a result, most SQL tends to heavily rely on nesting of queries and shameless duplication.

With SQLpt, on the other hand, you are encouraged to break your SQL down into its constituents. Why not simply put each constituent in a `val`, and while we're at it, give it a descriptive name too?

SQLpt also favors defining reusable functions instead of blatantly duplicating your logic everywhere.

For instance you can define a function `bestSellingBooksWhere` that accepts a predicate. You can then use the function with different predicates.

Another good candidate for a function would be that timestamp manipulation logic you keep copy-pasting everywhere.

## Audience

If you find your-self working with extremely large and complex SQL queries that take forever to run, then SQLpt is for you.

SQLpt will especially come in handy if you do non-real-time [ETL-like](https://en.wikipedia.org/wiki/Extract,_transform,_load) work at an organization where you have business experts churning out SQL that you will then have to make maintainable (make readable, test, ...).

Of course, if you *are* one of those business experts, but you also know a little bit of Scala, then you may also find SQLpt useful (and power to you).

Non-technically minded business experts will always favor SQL over anything else, because it is easy to learn, very concise and great for incremental development. Therefore, the task of translating SQL into something more maintainable is unlikely to go away anytime soon.

In order to reduce confusion when carrying out this translation task, SQLpt minimizes the impedance mismatch between it and SQL by closely resembling SQL's syntax.

## Who shouldn't use it

Note that SQLpt is neither an ORM, nor a full fledged database access library. For instance, there is no concurrency support, or any concept of transactions.

So if you are building a web application or web service and need to talk to your database in real-time, then SQLpt is not for you.
