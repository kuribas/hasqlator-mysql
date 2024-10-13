{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE KindSignatures #-}

{-|
Module      : Database.Hasqelator
Description : SQL generation
Copyright   : (c) Kristof Bastiaensen, 2020
License     : BSD-3
Maintainer  : kristof@resonata.be
Stability   : unstable
Portability : ghc


-}

module Database.MySQL.Hasqlator
  ( -- * Querying
    Query, Command, select, unionDistinct, unionAll, mergeSelect, replaceSelect,

    -- * Query Clauses
    QueryClauses, from, innerJoin, leftJoin, rightJoin, outerJoin, emptyJoins,
    where_, emptyWhere, groupBy_, having, emptyHaving, QueryOrdering(..),
    orderBy, limit, limitOffset,

    -- * Selectors
    Selector, as, forUpdate, forShare, shareMode, WaitLock,

    -- ** polymorphic selector
    sel,
    -- ** specialised selectors
    -- | The following are specialised versions of `sel`.  Using these
    -- may make refactoring easier, for example accidently swapping
    -- @`sel` "age"@ and @`sel` "name"@ would not give a type error,
    -- while @intSel "age"@ and @textSel "name"@ most likely would.
    intSel, integerSel, doubleSel, floatSel, scientificSel, 
    localTimeSel, timeOfDaySel, diffTimeSel, daySel, byteStringSel,
    textSel,
    -- ** other selectors
    rawValues, rawValues_,

    -- * Expressions
    subQuery,
    arg, fun, op, isNull, isNotNull, (>.), (<.), (>=.), (<=.), (+.), (-.), (*.),
    (/.), (=.), (++.), (/=.), (&&.), (||.), abs_, negate_, signum_, sum_,
    rawSql, substr, in_, false_, true_, notIn_, values,

    -- * Insertion
    Insertor, insertValues, insertUpdateValues, insertSelect, insertData,
    skipInsert, into, exprInto, Getter, lensInto, insertOne, ToSql, insertLess,

    -- * Updates
    update,

    -- * Deletes
    delete,
    
    -- * Rendering Queries
    renderStmt, renderPreparedStmt, SQLError(..), QueryBuilder,
    ToQueryBuilder(..), FromSql,

    -- * Executing Queries
    executeQuery, executeCommand
  )

where

import Database.MySQL.Base hiding (Query, Command)
import qualified Database.MySQL.Base as MySQL
import Prelude hiding (unwords)
import Control.Monad.State
import Control.Applicative
import Control.Monad.Except
import Data.Monoid hiding ((<>))
import Data.String hiding (unwords)
import Data.List (intersperse)
import qualified Data.DList as DList
import GHC.Generics hiding (Selector, from)
import qualified GHC.Generics as Generics (from)
  
import Data.DList (DList)
import Data.Scientific
import Data.Word
import Data.Int
import Data.Time
import qualified Data.ByteString as StrictBS
import qualified Data.ByteString.Lazy as LazyBS
import Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder
import qualified Data.Text.Encoding as Text
import qualified Data.Text as Text
import Data.Text (Text)
import Data.Binary.Put
import Data.Traversable
import Data.Functor.Contravariant
import qualified System.IO.Streams as Streams
import Control.Exception (throw, Exception)
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Text as Aeson
import qualified Data.Text.Lazy as LazyText
import Data.Maybe (mapMaybe)

class FromSql a where
  fromSql :: MySQLValue -> Either SQLError a

class ToSql a where
  toSqlValue :: a -> MySQLValue

instance FromSql a => IsString (Selector a) where
  fromString = sel . fromString

class ToQueryBuilder a where
  toQueryBuilder :: a -> QueryBuilder

renderStmt :: ToQueryBuilder a => a -> LazyBS.ByteString
renderStmt a = Builder.toLazyByteString stmt
  where
    QueryBuilder stmt _ _ = toQueryBuilder a

renderPreparedStmt :: ToQueryBuilder a => a -> (LazyBS.ByteString, [MySQLValue])
renderPreparedStmt a = (Builder.toLazyByteString pstmt, DList.toList args)
  where
    QueryBuilder _ pstmt args = toQueryBuilder a

-- | Execute a Query which returns a resultset.  May throw a
-- `SQLError` exception.  See the mysql-haskell package for other
-- exceptions it may throw.
executeQuery :: MySQLConn -> Query a -> IO [a]
executeQuery conn q =
  do let getSelector :: Query a -> Selector a
         getSelector (Query s _) = s
         getSelector (UnionAll q1 _) = getSelector q1
         getSelector (UnionDistinct q1 _) = getSelector q1
     is <- fmap snd $ MySQL.query_ conn $ MySQL.Query $ renderStmt q
     results <- Streams.toList is
     for results $ either throw pure . runSelector (getSelector q)
     
-- | Execute a Command which doesn't return a result-set. May throw a
-- `SQLError` exception.  See the mysql-haskell package for other
-- exceptions it may throw.
executeCommand :: MySQLConn -> Command -> IO OK
executeCommand conn c = MySQL.execute_ conn $ MySQL.Query $ renderStmt c

selectOne :: (MySQLValue -> Either SQLError a) -> QueryBuilder -> Selector a
selectOne f fieldName =
  Selector (DList.singleton fieldName) $ do
  results <- get
  case results of
    [] -> throwError ResultSetCountError
    (r1:rest) -> do
      put rest
      lift $ f r1

-- | The polymorphic selector.  The return type is determined by type
-- inference.
sel :: FromSql a => QueryBuilder -> Selector a
sel fieldName = selectOne fromSql fieldName

-- | an integer field (TINYINT.. BIGINT).  Any bounded haskell integer
-- type can be used here , for example `Int`, `Int32`, `Word32`.  An
-- `Overflow` ur `Underflow` error will be raised if the value doesn't
-- fit the type.
intSel :: (Show a, Bounded a, Integral a) => QueryBuilder -> Selector a
intSel e = selectOne intFromSql e

-- | Un unbounded integer field, either a bounded integer (TINYINT,
-- etc...) or DECIMAL in the database.  Will throw a type error if the
-- stored value is actually fractional.
--
-- /WARNING/: this function could potentially create huge integers with DECIMAL,
-- if the exponent is large, even fillup the space and crash your
-- program!  Only use this on trusted inputs, or use Scientific
-- instead.
integerSel :: QueryBuilder -> Selector Integer
integerSel = sel

-- | a DOUBLE field.
doubleSel :: QueryBuilder -> Selector Double
doubleSel = sel

-- | a FLOAT field.
floatSel :: QueryBuilder -> Selector Float
floatSel = sel

-- | A DECIMAL or NUMERIC field.
scientificSel :: QueryBuilder -> Selector Scientific
scientificSel = sel

-- | a DATETIME or a TIMESTAMP field.
localTimeSel :: QueryBuilder -> Selector LocalTime
localTimeSel = sel

-- | A TIME field taken as a specific time.
timeOfDaySel :: QueryBuilder -> Selector TimeOfDay
timeOfDaySel = sel

-- | a TIME field taken as a time duration.
diffTimeSel :: QueryBuilder -> Selector DiffTime
diffTimeSel = sel

-- | A DATE field.
daySel :: QueryBuilder -> Selector Day
daySel = sel

-- | A binary BLOB field.
byteStringSel :: QueryBuilder -> Selector StrictBS.ByteString
byteStringSel = sel

-- | a TEXT field.
textSel :: QueryBuilder -> Selector Text
textSel = sel


data SQLError = SQLError String
              | ResultSetCountError
              | TypeError MySQLValue String
              | ConversionError Text
              deriving Show

instance Exception SQLError
-- | Selectors contain the target fields or expressions in a SQL
-- SELECT statement, and perform the conversion to haskell.  Selectors
-- are instances of `Applicative`, so they can return the desired
-- haskell type.
data Selector a = Selector (DList QueryBuilder)
                  (StateT [MySQLValue] (Either SQLError) a)

runSelector :: Selector a -> [MySQLValue] -> Either SQLError a
runSelector (Selector _ run) = evalStateT run
                  
instance Functor Selector where
  fmap f (Selector cols cast) = Selector cols $ fmap f cast

instance Applicative Selector where
  Selector cols1 cast1 <*> Selector cols2 cast2 =
    Selector (cols1 <> cols2) (cast1 <*> cast2)
  pure x = Selector DList.empty $ pure x

instance Semigroup a => Semigroup (Selector a) where
  (<>) = liftA2 (<>)
    
instance Monoid a => Monoid (Selector a) where
  mempty = pure mempty

-- | `Query a` represents a query returning values of type `a`.
data Query a = Query (Selector a) QueryBody
             | UnionAll (Query a) (Query a)
             | UnionDistinct (Query a) (Query a)
-- | A command is a database query that doesn't return a value, but is
-- executed for the side effect (inserting, updating, deleteing).
data Command = Update [QueryBuilder] [(QueryBuilder, QueryBuilder)] QueryBody
             | InsertSelect QueryBuilder [QueryBuilder] [QueryBuilder] QueryBody
             | forall a.InsertValues QueryBuilder (Insertor a)
               (Maybe [(QueryBuilder, QueryBuilder)]) [a]
             | Delete QueryBuilder QueryClauses

-- | An @`Insertor` a@ provides a mapping of parts of values of type
-- @a@ to columns in the database.  Insertors can be combined using `<>`.
data Insertor a = Insertor [Text] (a -> [QueryBuilder])

data Join = Join JoinType [QueryBuilder] [QueryBuilder]
data JoinType = InnerJoin | LeftJoin | RightJoin | OuterJoin

pairQuery :: (QueryBuilder, QueryBuilder) -> QueryBuilder
pairQuery (a, b) = a <> " = " <> b

instance ToQueryBuilder Command where
  toQueryBuilder (Update tables setting body) =
    unwords
    [ "UPDATE", commaSep tables
    , "SET", commaSep $ map pairQuery setting
    , toQueryBuilder body
    ] 

  toQueryBuilder (InsertValues table _ _  []) =
    unwords ["INSERT INTO", table, "SELECT * FROM", table, "WHERE false"]
  toQueryBuilder (InsertValues table (Insertor cols convert) updates values__) =
    let valuesB = commaSep $
                  map (parentized . commaSep . convert)
                  values__
        insertStmt = [ "INSERT INTO", table
                  , parentized $ commaSep $
                    map rawSql cols
                  , "VALUES", valuesB]
    in unwords $ insertStmt ++
       foldMap (\setting ->
                   [ "ON DUPLICATE KEY UPDATE"
                   , commaSep (map pairQuery setting)])
       updates

  toQueryBuilder (InsertSelect table cols rows queryBody) =
    unwords
    [ "INSERT INTO", table
    , parentized $ commaSep cols
    , "SELECT", parentized $ commaSep rows
    , toQueryBuilder queryBody
    ]
  toQueryBuilder (Delete fields (QueryClauses query__)) =
    "DELETE " <> fields <> " " <> toQueryBuilder body
    where body = appEndo query__ emptyQueryBody

instance ToQueryBuilder QueryBody where
  toQueryBuilder body =
    unwords $ 
    fromB (_from body) <>
    (joinB <$> _joins body) <>
    renderPredicates "WHERE" (_where_ body) <>
    (groupByB $ _groupBy body) <>
    renderPredicates "HAVING" (_having body) <>
    orderByB (_orderBy body) <>
    limitB (_limit body) <>
    lockModeB (_lockMode body)
    where
      fromB Nothing = []
      fromB (Just table) = ["FROM", table]

      joinB (Join _ [] _) = error "list of join tables cannot be empty"
      joinB (Join joinType tables joinConditions) =
        unwords $ [toQueryBuilder joinType, renderList tables] ++
        renderPredicates "ON" joinConditions

      groupByB [] = []
      groupByB e = ["GROUP BY", commaSep e]

      orderByB [] = []
      orderByB e = ["ORDER BY", commaSep $ map toQueryBuilder e]

      limitB Nothing = []
      limitB (Just (count, Nothing)) = ["LIMIT", fromString (show count)]
      limitB (Just (count, Just offset)) =
        [ "LIMIT" , fromString (show count)
        , "OFFSET", fromString (show offset) ]

      lockModeB Nothing = []
      lockModeB (Just (ForUpdate tables waitlock)) =
        ["FOR UPDATE"] <> updateTablesB tables <> waitLockB waitlock
      lockModeB (Just (ForShare tables waitlock)) =
        ["FOR SHARE"] <> updateTablesB tables <> waitLockB waitlock
      lockModeB (Just ShareMode) = ["LOCK IN SHARE MODE"]

      waitLockB NoWaitLock = ["NOWAIT"]
      waitLockB WaitLock = []
      waitLockB SkipLocked = ["SKIP LOCKED"]

      updateTablesB [] = []
      updateTablesB t = ["OF", commaSep t]

instance ToQueryBuilder (Query a) where
  toQueryBuilder (Query (Selector dl _) body) =
    "SELECT " <> commaSep (DList.toList dl) <> " " <> toQueryBuilder body
  toQueryBuilder (UnionAll q1 q2) =
    parentized (toQueryBuilder q1) <>
    " UNION ALL " <>
    parentized (toQueryBuilder q2)
  toQueryBuilder (UnionDistinct q1 q2) =
    parentized (toQueryBuilder q1) <>
    " UNION " <>
    parentized (toQueryBuilder q2)

rawSql :: Text -> QueryBuilder
rawSql t = QueryBuilder builder builder DList.empty where
  builder = Builder.byteString (Text.encodeUtf8 t)
                                  
instance ToQueryBuilder JoinType where
  toQueryBuilder InnerJoin = "INNER JOIN"
  toQueryBuilder LeftJoin = "LEFT JOIN"
  toQueryBuilder RightJoin = "RIGHT JOIN"
  toQueryBuilder OuterJoin = "OUTER JOIN"

data WaitLock = NoWaitLock | WaitLock | SkipLocked

data LockMode =
  ForUpdate [QueryBuilder] WaitLock |
  ForShare [QueryBuilder] WaitLock |
  ShareMode

data QueryBody = QueryBody
  { _from :: Maybe QueryBuilder
  , _joins :: [Join]
  , _where_ :: [QueryBuilder]
  , _groupBy :: [QueryBuilder]
  , _having :: [QueryBuilder]
  , _orderBy :: [QueryOrdering]
  , _limit :: Maybe (Int, Maybe Int)
  , _lockMode :: Maybe LockMode
  }

data QueryOrdering = 
  Asc QueryBuilder | Desc QueryBuilder

instance ToQueryBuilder QueryOrdering where
  toQueryBuilder (Asc b) = b <> " ASC"
  toQueryBuilder (Desc b) = b <> " DESC"

data QueryBuilder = QueryBuilder Builder Builder (DList MySQLValue)

instance IsString QueryBuilder where
  fromString s = QueryBuilder b b DList.empty
    where b = Builder.string8 s

instance Semigroup QueryBuilder where
  QueryBuilder stmt1 prepStmt1 vals1 <> QueryBuilder stmt2 prepStmt2 vals2 =
    QueryBuilder (stmt1 <> stmt2) (prepStmt1 <> prepStmt2) (vals1 <> vals2)

instance Monoid QueryBuilder where
  mempty = QueryBuilder mempty mempty mempty

newtype QueryClauses = QueryClauses (Endo QueryBody)
  deriving (Semigroup, Monoid)

instance Semigroup (Insertor a) where
  Insertor fields1 conv1 <> Insertor fields2 conv2 =
    Insertor (fields1 <> fields2) (conv1 <> conv2)

instance Monoid (Insertor a) where
  mempty = Insertor mempty mempty

instance Contravariant Insertor where
  contramap f (Insertor x g) = Insertor x (g . f)

fromText :: Text -> QueryBuilder
fromText s = QueryBuilder b b DList.empty
  where b = Builder.byteString $ Text.encodeUtf8 s

sepBy :: Monoid a => a -> [a] -> a
sepBy sep builder = mconcat $ intersperse sep builder
{-# INLINE sepBy #-}

commaSep :: (IsString a, Monoid a) => [a] -> a
commaSep = sepBy ", "
{-# INLINE commaSep #-}

unwords :: (IsString a, Monoid a) => [a] -> a
unwords = sepBy " "
{-# INLINE unwords #-}

parentized :: (IsString a, Monoid a) => a -> a
parentized expr = "(" <> expr <> ")"
{-# INLINE parentized #-}

renderList :: [QueryBuilder] -> QueryBuilder
renderList [] = ""
renderList [e] = e
renderList es = parentized $ commaSep es

renderPredicates :: QueryBuilder -> [QueryBuilder] -> [QueryBuilder]
renderPredicates _ [] = []
renderPredicates keyword [e] = [keyword, e]
renderPredicates keyword es =
  keyword : intersperse "AND" (map parentized $ reverse es)

mysqlValueBuilder :: MySQLValue -> Builder
mysqlValueBuilder = Builder.lazyByteString . runPut . putTextField 

arg :: ToSql a => a -> QueryBuilder
arg a = QueryBuilder 
  (mysqlValueBuilder $ toSqlValue a)
  (Builder.lazyByteString "?")
  (DList.singleton $ toSqlValue a)

fun :: Text -> [QueryBuilder] -> QueryBuilder
fun name exprs = fromText name <> parentized (commaSep exprs)

op :: Text -> QueryBuilder -> QueryBuilder -> QueryBuilder
op name e1 e2 = parentized $ e1 <> " " <> fromText name <> " " <> e2

substr :: QueryBuilder -> QueryBuilder -> QueryBuilder -> QueryBuilder
substr field start end = fun "substr" [field, start, end]

infixr 3 &&., ||.
infix 4 <., >., >=., <=., =., /=.
infixr 5 ++.
infixl 6 +., -.
infixl 7 *., /.
  
(>.), (<.), (>=.), (<=.), (+.), (-.), (/.), (*.), (=.), (/=.), (++.), (&&.),
  (||.)
  :: QueryBuilder -> QueryBuilder -> QueryBuilder
(>.) = op ">"
(<.) = op "<"
(>=.) = op ">="
(<=.) = op "<="
(+.) = op "+"
(*.) = op "*"
(/.) = op "/"
(-.) = op "-"
(=.) = op "="
(/=.) = op "<>"
a ++. b = fun "concat" [a, b]
(&&.) = op "and"
(||.) = op "or"

abs_, signum_, negate_, sum_ :: QueryBuilder -> QueryBuilder
abs_ x = fun "abs" [x]
signum_ x = fun "sign" [x]
negate_ x = fun "-" [x]
sum_ x = fun "sum" [x]

false_, true_ :: QueryBuilder
-- | False
false_ = rawSql "false"
-- | True
true_ = rawSql "true"

-- | VALUES
values :: QueryBuilder -> QueryBuilder
values x = fun "values" [x]

-- | IS NULL
isNull :: QueryBuilder -> QueryBuilder
isNull e = parentized $ e <> " IS NULL"

-- | IS NOT NULL expression
isNotNull :: QueryBuilder -> QueryBuilder
isNotNull e = parentized $ e <> " IS NOT NULL"

-- | insert an SQL expression.  Takes a function that generates the
-- SQL expression from the input.
exprInto :: (a -> QueryBuilder) -> Text -> Insertor a
exprInto f s = Insertor [s] (\t -> [f t])

-- | insert a single value directly
insertOne :: ToSql a => Text -> Insertor a
insertOne s = arg `exprInto` s

-- | insert a datastructure
class InsertGeneric (fields :: *) (data_ :: *) where
  insertDataGeneric :: fields -> Insertor data_

genFst :: (a :*: b) () -> a ()
genFst (a :*: _) = a

genSnd :: (a :*: b) () -> b ()
genSnd (_ :*: b) = b

instance (InsertGeneric (a ()) (c ()),
          InsertGeneric (b ()) (d ())) =>
  InsertGeneric ((a :*: b) ()) ((c :*: d) ()) where
  insertDataGeneric (a :*: b) =
    contramap genFst (insertDataGeneric a) <>
    contramap genSnd (insertDataGeneric b)

instance InsertGeneric (a ()) (b ()) =>
  InsertGeneric (M1 m1 m2 a ()) (M1 m3 m4 b ()) where
  insertDataGeneric = contramap unM1 . insertDataGeneric . unM1

instance ToSql b => InsertGeneric (K1 r Text ()) (K1 r b ()) where
  insertDataGeneric = contramap unK1 . insertOne . unK1

instance InsertGeneric (K1 r (Insertor a) ()) (K1 r a ()) where
  insertDataGeneric = contramap unK1 . unK1

-- | `insertData` inserts a tuple or other product type into the given
-- fields.  It uses generics to match the input to the fields. For
-- example:
--
-- > insert "Person" (insertData ("name", "age"))
-- >   [Person "Bart Simpson" 10, Person "Lisa Simpson" 8]

insertData :: (Generic a, Generic b, InsertGeneric (Rep a ()) (Rep b ()))
           => a -> Insertor b
insertData = contramap from' . insertDataGeneric . from'
  where from' :: Generic a => a -> Rep a ()
        from' = Generics.from

-- | skipInsert is mempty specialized to an Insertor.  It can be used
-- to skip fields when using insertData.
skipInsert :: Insertor a
skipInsert = mempty

-- | `into` uses the given accessor function to map the part to a
-- field.  For example:
--
-- > insertValues "Person" (fst `into` "name" <> snd `into` "age")
-- >   [("Bart Simpson", 10), ("Lisa Simpson", 8)]
into :: ToSql b => (a -> b) -> Text -> Insertor a
into toVal = contramap toVal . insertOne

-- | A Getter type compatible with the lens library
type Getter s a = (a -> Const a a) -> s -> Const a s

-- | `lensInto` uses a lens to map the part to a field.  For example:
--
-- > insertValues "Person" (_1 `lensInto` "name" <> _2 `lensInto` "age")
-- >   [("Bart Simpson", 10), ("Lisa Simpson", 8)]

lensInto :: ToSql b => Getter a b -> Text -> Insertor a
lensInto lens = into (getConst . lens Const)

-- | (<subquery>)
subQuery :: ToQueryBuilder a => a -> QueryBuilder
subQuery = parentized . toQueryBuilder

-- | FROM table
from :: QueryBuilder -> QueryClauses
from table = QueryClauses $ Endo $ \qc -> qc {_from = Just table}

joinClause :: JoinType -> [QueryBuilder] -> [QueryBuilder] -> QueryClauses
joinClause tp tables conditions = QueryClauses $ Endo $ \qc ->
  qc { _joins = Join tp tables conditions : _joins qc }

-- | INNER JOIN table1, ... ON cond1, cond2, ...
innerJoin ::
  -- | tables
  [QueryBuilder] ->
  -- | on expressions, joined by AND
  [QueryBuilder] ->
  QueryClauses
innerJoin = joinClause InnerJoin

-- | LEFT JOIN
leftJoin ::
  -- | tables
  [QueryBuilder] ->
  -- | on expressions, joined by AND
  [QueryBuilder] ->
  QueryClauses
leftJoin = joinClause LeftJoin

-- | RIGHT JOIN
rightJoin ::
  -- | tables
  [QueryBuilder] ->
  -- | on expressions, joined by AND
  [QueryBuilder] ->
  QueryClauses
rightJoin = joinClause RightJoin

-- | OUTER JOIN
outerJoin ::
  -- | tables
  [QueryBuilder] ->
  -- | on expressions, joined by AND
  [QueryBuilder] ->
  QueryClauses
outerJoin = joinClause OuterJoin

-- | remove all existing joins
emptyJoins :: QueryClauses
emptyJoins = QueryClauses $ Endo $ \qc ->
  qc { _joins = [] }

-- | WHERE expression1, expression2, ...
where_ :: [QueryBuilder] -> QueryClauses
where_ conditions = QueryClauses $ Endo $ \qc ->
  qc { _where_ = reverse conditions ++ _where_ qc}

-- | remove all existing where expressions
emptyWhere :: QueryClauses
emptyWhere = QueryClauses $ Endo $ \qc ->
  qc { _where_ = [] }

-- | GROUP BY e1, e2, ...
groupBy_ :: [QueryBuilder] -> QueryClauses
groupBy_ columns = QueryClauses $ Endo $ \qc ->
  qc { _groupBy = columns }

-- | HAVING e1, e2, ...
having :: [QueryBuilder] -> QueryClauses
having conditions = QueryClauses $ Endo $ \qc ->
  qc { _having = reverse conditions ++ _having qc }

-- | remove having expression
emptyHaving :: QueryClauses
emptyHaving = QueryClauses $ Endo $ \qc ->
  qc { _having = [] }

-- | ORDER BY e1, e2, ...
orderBy :: [QueryOrdering] -> QueryClauses
orderBy ordering = QueryClauses $ Endo $ \qc ->
  qc { _orderBy = ordering }

-- | LIMIT n
limit :: Int -> QueryClauses
limit count = QueryClauses $ Endo $ \qc ->
  qc { _limit = Just (count, Nothing) }

-- | LIMIT count, offset
limitOffset ::
  -- | count
  Int ->
  -- | offset
  Int ->
  QueryClauses
limitOffset count offset = QueryClauses $ Endo $ \qc ->
  qc { _limit = Just (count, Just offset) }

emptyQueryBody :: QueryBody
emptyQueryBody = QueryBody Nothing [] [] [] [] [] Nothing Nothing

-- | SELECT
select :: Selector a -> QueryClauses -> Query a
select selector (QueryClauses clauses) =
  Query selector $ clauses `appEndo` emptyQueryBody

-- | qry1 UNION ALL qry2
unionAll :: Query a -> Query a -> Query a
unionAll = UnionAll

-- | UNION 
unionDistinct :: Query a -> Query a -> Query a
unionDistinct = UnionDistinct

-- | Merge a new @Selector@ in a query.
mergeSelect :: Query b -> (a -> b -> c) -> Selector a -> Query c
mergeSelect (Query selector2 body) f selector1 =
  Query (liftA2 f selector1 selector2) body
mergeSelect (UnionAll q1 q2) f s =
  UnionAll (mergeSelect q1 f s) (mergeSelect q2 f s)
mergeSelect (UnionDistinct q1 q2) f s =
  UnionDistinct (mergeSelect q1 f s) (mergeSelect q2 f s)

-- | Replace the @Selector@ from a Query.
replaceSelect :: Selector a -> Query b -> Query a
replaceSelect s (Query _ body) = Query s body
replaceSelect s (UnionAll q1 q2) =
  UnionAll (replaceSelect s q1) (replaceSelect s q2)
replaceSelect s (UnionDistinct q1 q2) =
  UnionDistinct (replaceSelect s q1) (replaceSelect s q2)

-- | insert values using the given insertor.
insertValues :: QueryBuilder -> Insertor a -> [a] -> Command
insertValues qb i = InsertValues qb i Nothing

-- | DELETE
delete :: QueryBuilder -> QueryClauses -> Command
delete = Delete

-- | INSERT UPDATE
insertUpdateValues :: QueryBuilder
                   -> Insertor a
                   -> [(QueryBuilder, QueryBuilder)]
                   -> [a]
                   -> Command
insertUpdateValues qb i u = InsertValues qb i (Just u)

insertSelect :: QueryBuilder -> [QueryBuilder] -> [QueryBuilder] -> QueryClauses
             -> Command
insertSelect table toColumns fromColumns (QueryClauses clauses) =
  InsertSelect table toColumns fromColumns $ appEndo clauses emptyQueryBody

update :: [QueryBuilder] -> [(QueryBuilder, QueryBuilder)] -> QueryClauses
       -> Command
update tables assignments (QueryClauses clauses) =
  Update tables assignments $ appEndo clauses emptyQueryBody

-- | combinator for aliasing columns.
as :: QueryBuilder -> QueryBuilder -> QueryBuilder
as e1 e2 = e1 <> " AS " <> e2

in_ :: QueryBuilder -> [QueryBuilder] -> QueryBuilder
in_ _ [] = false_
in_ e l = e <> " IN " <> parentized (commaSep l)

notIn_ :: QueryBuilder -> [QueryBuilder] -> QueryBuilder
notIn_ _ [] = true_
notIn_ e l = e <> " NOT IN " <> parentized (commaSep l)

-- | Read the columns directly as a `MySQLValue` type without conversion.
rawValues :: [QueryBuilder] -> Selector [MySQLValue]
rawValues cols = Selector (DList.fromList cols) $
              state $ splitAt (length cols)

-- | Ignore the content of the given columns
rawValues_ :: [QueryBuilder] -> Selector ()
rawValues_ cols = () <$ rawValues cols

forUpdate :: [QueryBuilder] -> WaitLock -> QueryClauses
forUpdate qb wl = QueryClauses $ Endo $ \qc ->
  qc { _lockMode = Just $ ForUpdate qb wl }

forShare :: [QueryBuilder] -> WaitLock -> QueryClauses
forShare qb wl = QueryClauses $ Endo $ \qc ->
  qc { _lockMode = Just $ ForShare qb wl }

shareMode :: QueryClauses
shareMode = QueryClauses $ Endo $ \qc -> qc { _lockMode = Just ShareMode }

-- selector for any bounded integer type
intFromSql :: forall a.(Show a, Bounded a, Integral a)
            => MySQLValue -> Either SQLError  a
intFromSql r = case r of
  MySQLInt8U u -> castFromWord $ fromIntegral u
  MySQLInt8 i -> castFromInt $ fromIntegral i
  MySQLInt16U u -> castFromWord $ fromIntegral u
  MySQLInt16 i -> castFromInt $ fromIntegral i
  MySQLInt32U u -> castFromWord $ fromIntegral u
  MySQLInt32 i -> castFromInt $ fromIntegral i
  MySQLInt64U u -> castFromWord $ fromIntegral u
  MySQLInt64 i -> castFromInt $ fromIntegral i
  MySQLYear y -> castFromWord $ fromIntegral y
  _ -> Left $ TypeError r $
       "Int (" <> show (minBound :: a) <> ", " <> show (maxBound :: a) <> ")"
  where castFromInt :: Int64 -> Either SQLError a
        castFromInt i
          | i < fromIntegral (minBound :: a) =
              throwError $ ConversionError "underflow"
          | i > fromIntegral (maxBound :: a) =
              throwError $ ConversionError "overflow"
          | otherwise = pure $ fromIntegral i
        castFromWord :: Word64 -> Either SQLError a
        castFromWord i
          | i > fromIntegral (maxBound :: a) = throwError $ ConversionError "overflow"
          | otherwise = pure $ fromIntegral i

integerFromSql :: MySQLValue -> Either SQLError Integer
integerFromSql (MySQLInt8U u) = pure $ fromIntegral u
integerFromSql (MySQLInt8 i) = pure $ fromIntegral i
integerFromSql (MySQLInt16U u) = pure $ fromIntegral u
integerFromSql (MySQLInt16 i) = pure $ fromIntegral i
integerFromSql (MySQLInt32U u) = pure $ fromIntegral u
integerFromSql (MySQLInt32 i) = pure $ fromIntegral i
integerFromSql (MySQLInt64U u) = pure $ fromIntegral u
integerFromSql (MySQLInt64 i) = pure $ fromIntegral i
integerFromSql (MySQLYear y) = pure $ fromIntegral y
integerFromSql (MySQLDecimal d) = case floatingOrInteger d of
  Left (_ :: Double) -> throwError $ TypeError (MySQLDecimal d) "Integer"
  Right i -> pure i
integerFromSql v = throwError $ TypeError v "Integer"

-- | Exclude fields to insert.
insertLess :: Insertor a -> [Text] -> Insertor a
insertLess (Insertor fields t) toRemove =
  Insertor (filterList fields) (filterList . t)
  where
    filterList = mapMaybe removeMaybe . zip removeIt
    removeIt = map (`elem` toRemove) fields
    removeMaybe (True, _) = Nothing
    removeMaybe (False, x) = Just x
    
instance FromSql Bool where
  fromSql (MySQLInt8U x) = pure $ x /= 0
  fromSql (MySQLInt8 x) = pure $ x /= 0
  fromSql v = throwError $ TypeError v "Bool"
  
instance FromSql Int where
  fromSql = intFromSql

instance FromSql Int8 where
  fromSql = intFromSql

instance FromSql Word8 where
  fromSql = intFromSql

instance FromSql Int16 where
  fromSql = intFromSql

instance FromSql Word16 where
  fromSql = intFromSql

instance FromSql Int32 where
  fromSql = intFromSql

instance FromSql Word32 where
  fromSql = intFromSql

instance FromSql Int64 where
  fromSql = intFromSql

instance FromSql Word64 where
  fromSql = intFromSql

instance FromSql Integer where
  fromSql = integerFromSql

instance FromSql Float where
  fromSql r = case r of
    MySQLFloat f -> pure f
    _ -> Left $ TypeError r "Float"

instance FromSql Double where
  fromSql r = case r of
    MySQLFloat f -> pure $ realToFrac f
    MySQLDouble f -> pure f
    _ -> Left $ TypeError r "Double"

instance FromSql Scientific where
  fromSql r = case r of
    MySQLDecimal f -> pure f
    _ -> Left $ TypeError r "Scientific"

instance FromSql LocalTime where
  fromSql r = case r of
    MySQLTimeStamp t -> pure t
    MySQLDateTime t -> pure t
    _ -> Left $ TypeError r "LocalTime"

instance FromSql TimeOfDay where
  fromSql r = case r of
    MySQLTime sign_ t | sign_ >= 0 -> pure t
                      | otherwise -> throwError $ ConversionError "overflow"
    _ -> Left $ TypeError r "TimeOfDay"

instance FromSql DiffTime where
  fromSql r = case r of
    MySQLTime sign_ t | sign_ == 1 -> pure $ negate $ timeOfDayToTime t
                      | otherwise -> pure $ timeOfDayToTime t
    _ -> Left $ TypeError r "DiffTime"
    
instance FromSql Day where
  fromSql r = case r of
    MySQLDate d -> pure d
    _ -> Left $ TypeError r "Day"

instance FromSql StrictBS.ByteString where
  fromSql r = case r of
    MySQLBytes b -> pure b
    _ -> Left $ TypeError r "ByteString"

instance FromSql Text where
  fromSql r = case r of
    MySQLText t -> pure t
    _ -> Left $ TypeError r "Text"

instance FromSql a => FromSql (Maybe a) where
  fromSql r = case r of
    MySQLNull -> pure Nothing
    _ -> Just <$> fromSql r

instance FromSql Aeson.Value where
  fromSql r = case r of
    MySQLBytes t -> case Aeson.eitherDecodeStrict t
                   of Right val -> Right val
                      Left err -> Left $ ConversionError $ Text.pack err
    _ -> Left $ TypeError r "Value"
  
instance ToSql Int where
  toSqlValue = MySQLInt64 . fromIntegral

instance ToSql Int8 where
  toSqlValue = MySQLInt8

instance ToSql Word8 where
  toSqlValue = MySQLInt8U

instance ToSql Int16 where
  toSqlValue = MySQLInt16

instance ToSql Word16 where
  toSqlValue = MySQLInt16U

instance ToSql Int32 where
  toSqlValue = MySQLInt32

instance ToSql Word32 where
  toSqlValue = MySQLInt32U

instance ToSql Int64 where
  toSqlValue = MySQLInt64

instance ToSql Word64 where
  toSqlValue = MySQLInt64U

instance ToSql Integer where
  toSqlValue = MySQLDecimal . fromIntegral

instance ToSql Float where
  toSqlValue = MySQLFloat

instance ToSql Double where
  toSqlValue = MySQLDouble

instance ToSql Scientific where
  toSqlValue = MySQLDecimal

instance ToSql LocalTime where
  toSqlValue = MySQLDateTime

instance ToSql TimeOfDay where
  toSqlValue = MySQLTime 0
  
instance ToSql DiffTime where
  toSqlValue dt | dt < 0 =  MySQLTime 1 $ timeToTimeOfDay $ negate dt
                | otherwise = MySQLTime 0 $ timeToTimeOfDay dt

instance ToSql Day where
  toSqlValue = MySQLDate

instance ToSql StrictBS.ByteString where
  toSqlValue = MySQLBytes

instance ToSql Text where
  toSqlValue = MySQLText

instance ToSql a => ToSql (Maybe a) where
  toSqlValue Nothing = MySQLNull
  toSqlValue (Just v) = toSqlValue v

instance ToSql Bool where
  toSqlValue = MySQLInt8U . fromIntegral . fromEnum

instance ToSql Aeson.Value where
  toSqlValue = MySQLBytes . LazyBS.toStrict . Aeson.encode
