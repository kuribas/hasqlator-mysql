hasqlator-mysql
====================

Hasqlator is a SQL generation library for haskell.  This package is
the mysql version of the package.

## Why yes another database library?

There are already many database libraries, postgresql-simple,
mysql-simple, ..., selda, beam, esqueleto, opaleye, squeal, ...

Why would we need another one?  One one side, many database libraries
are pretty sofisticated, and provide a lot of safety, but for the
expense of complexity.  On the other side are the simple libraries,
which are not much more than a wrapper over sql.  However these simple
libraries make constructing queries dynamically inconvenient.  What I
missed was a library that is easy enough to use, but still has
composable queries, type safe conversion, looks like SQL and supports
most of SQL features.  Furthermore I wanted it to work with existing
databases, and have an easy and predictable mapping between SQL
datatypes and haskell datatypes.

Hasqlator does this using a two layered approach.  The bottom layer
provides less type safety, and is as simple as using a string builder.
The upper layer adds on top of that the ability to verify that queries
conform to the schema.  This has more type level complexity, but
doesn't go as far as trying to prove the whole query correct.

## Layer 1

Layer one allows you to compose query clauses using a monoid, like
building strings: 

```haskell
select (liftA2 (,) (strSel "name") (intSel "age")) $
from "user" <>
where_ ["age" >. arg 18]
```

Here arg injects a value from haskell into the sql.  Each clause is
combined using mappend (`<>`).  Other MySQL clauses are also supported: `innerJoin`, `leftJoin`, `limit`,
etc.

Select takes a `Selector`, which basically expresses how to convert a
sql return type to a haskell value.  It has an `Applicative`
interface, so you can return any datatype you want.

```haskell

userSelector :: Selector (Text, Int)
userSelector = liftA2 (,) (strSel "name") (intSel "age")
executeQuery conn qry $ select userSelector $ from "user"
```

This creates a select clause:

    SELECT `name`, `age`
    
and marshalls the return values from `name` and `age` into the correct
haskell type, in this case by combining them into a tuple.

## SQL output

The SQL output should match the code!  .  This way, you know
exactly what SQL code would be generated, and learning the library
should be relatively straightforward, as you can just map the SQL
directly to the haskell code.

The end goal is to support all SQL syntax that is supported by the
backend database.

```haskell
select (liftA2 (,) (strSel "name") (intSel "age") (strSel "d.name")) $
from "user" <>
innerJoin ["department" `as` "d"]
["user.department_id" =. "d.id"] <>
where_ ["age" >. arg 18]
```

SQL output matches the code.  No exceptions! (well, there are
some exceptions where an invalid query would be produced, for example
when passing the empty list to `in`, in that case the library tries to
gracefully resolve this with a valid query)

```sql
SELECT name, age
FROM user
INNER JOIN department as d ON user.department_id =. d.id
WHERE age > ?
```

## Predictable datatype mapping

Each SQL datatype matches one haskell datatype, and vice versa.  Makes
it easy to use when the databases already exist.

| SQL Type             | Haskell Type |
|:---------------------|:-------------|
| TINYINT              | Int8         |
| DOUBLE               | Double       |
| FLOAT                | Float        |
| DECIMAL              | Scientific   |
| DATETIME/TIMESTAMP   | LocalTime    |
| TIME (specific time) | TimeOfDay    |
| TIME (time duration) | DiffTime     |
| DATE                 | Day          |
| ByteString           | BLOB         |
| TEXT                 | Text         |

## Layer 2: verifying queries against the schema

THe second layer allows you to verify if your query matches the
schema.  To do this you must express the schema as haskell values:

```haskell
userTbl :: Table "user" MyDB 
userTbl = Table (Just "MySchema") "user"
 
name :: Field "user" MyDB NotNull Text
name = Field "user" "name"

age :: Field "user" MyDB NotNull Int
age = Field "user" "age"
```

This can be also done automatically frmo the information schema using
Template Haskell:

```haskell
$(makeFields defaultProperties ''NSQC tableSchema tables)
```

Then you can write your queries using a monadic builder:

```haskell
userQuery :: Query MyDB (Selector (Text, Int))
do u <- from user_tbl
   where_ $ u age =. arg 18
   pure $ liftA2 (,) (strSel $ u name") (intSel $ u age)
```

The Monadic builder *builds* the query.  `from`, `innerJoin`, ... return
a freshly created alias that can be applied to a field.  THeir type
includes the table, so you can only apply the right fields to it.
Finally you return an Applicative `Selector` Functor that will do the
convertion between SQL and haskell.

## TODO

- More documentation and examples
- Port to other databases (postgresql, sqlite, ...).  Currently only
  mysql is supported.  A stub for postgresql exists, but doesn't
  function yet.
- Add more SQL statements that the backend supports.
- Create a dependently typed Idris version
