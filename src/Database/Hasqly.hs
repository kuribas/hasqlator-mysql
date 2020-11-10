{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedStrings #-}

{-|
Module      : Database.Hasqly
Description : SQL generation
Copyright   : (c) Kristof Bastiaensen, 2020
License     : BSD-3
Maintainer  : kristof@resonata.be
Stability   : unstable
Portability : ghc


-}

module Database.Hasqly
  ( -- * Querying
    Query, Command, select, mergeSelect, replaceSelect,

    -- * Query Clauses
    QueryClauses, from, innerJoin, leftJoin, rightJoin, outerJoin, emptyJoins,
    where_, emptyWhere, groupBy_, having, emptyHaving, orderBy, limit, limitOffset,

    -- * Selectors
    Selector, as,

    -- ** polymorphic selector
    col,
    -- ** specialised selectors
    -- | The following are specialised versions of `col`.  Using these
    -- may make refactoring easier, for example accidently swapping
    -- @`col` "age"@ and @`col` "name"@ would not give a type error,
    -- while @intCol "age"@ and @textCol "name"@ most likely would.
    intCol, integerCol, doubleCol, floatCol, scientificCol, 
    localTimeCol, timeOfDayCol, diffTimeCol, dayCol, byteStringCol,
    textCol,
    -- ** other selectors
    values, values_,

    -- * Expressions
    subQuery,
    arg, fun, op, (.>), (.+), (.-),

    -- * Insertion
    Insertor, insertValues, insertSelect, into, lensInto,
    insert1, insert2, insert3, insert4, insert5,
    insert6, insert7, insert8, insert9, insert10,
    ToSql,
    
    -- * Rendering Queries
    renderStmt, renderPreparedStmt, SQLError(..), QueryBuilder, ToQueryBuilder, FromSql
  )

where

import Database.MySQL.Base hiding (Query, Command)
import Prelude hiding (unwords)
import Control.Monad.State
import Control.Applicative
import Control.Monad.Except
import Data.Monoid hiding ((<>))
import Data.String hiding (unwords)
import Data.List hiding (unwords)
import qualified Data.DList as DList
  
import Data.DList (DList)
import Data.Scientific
import Data.Word
import Data.Int
import Data.Time
import qualified Data.ByteString as StrictBS
import qualified Data.ByteString.Lazy as LazyBS
import Data.ByteString.Lazy.Builder (Builder)
import qualified Data.ByteString.Lazy.Builder as Builder
import qualified Data.Text.Encoding as Text
import Data.Text (Text)
import Data.Binary.Put

class FromSql a where
  fromSql :: MySQLValue -> Either SQLError a

class ToSql a where
  toSqlValue :: a -> MySQLValue

instance FromSql a => IsString (Selector a) where
  fromString = col . fromString

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
col :: FromSql a => QueryBuilder -> Selector a
col fieldName = selectOne fromSql fieldName

-- | an integer field (TINYINT.. BIGINT).  Any bounded haskell integer
-- type can be used here , for example `Int`, `Int32`, `Word32`.  An
-- `Overflow` ur `Underflow` error will be raised if the value doesn't
-- fit the type.
intCol :: (Show a, Bounded a, Integral a) => QueryBuilder -> Selector a
intCol e = selectOne intFromSql e

-- | Un unbounded integer field, either a bounded integer (TINYINT,
-- etc...) or DECIMAL in the database.  Will throw a type error if the
-- stored value is actually fractional.
--
-- /WARNING/: this function could potentially create huge integers with DECIMAL,
-- if the exponent is large, even fillup the space and crash your
-- program!  Only use this on trusted inputs, or use Scientific
-- instead.
integerCol :: QueryBuilder -> Selector Integer
integerCol = col

-- | a DOUBLE field.
doubleCol :: QueryBuilder -> Selector Double
doubleCol = col

-- | a FLOAT field.
floatCol :: QueryBuilder -> Selector Float
floatCol = col

-- | A DECIMAL or NUMERIC field.
scientificCol :: QueryBuilder -> Selector Scientific
scientificCol = col

-- | a DATETIME or a TIMESTAMP field.
localTimeCol :: QueryBuilder -> Selector LocalTime
localTimeCol = col

-- | A TIME field taken as a specific time.
timeOfDayCol :: QueryBuilder -> Selector TimeOfDay
timeOfDayCol = col

-- | a TIME field taken as a time duration.
diffTimeCol :: QueryBuilder -> Selector DiffTime
diffTimeCol = col

-- | A DATE field.
dayCol :: QueryBuilder -> Selector Day
dayCol = col

-- | A binary BLOB field.
byteStringCol :: QueryBuilder -> Selector StrictBS.ByteString
byteStringCol = col

-- | a TEXT field.
textCol :: QueryBuilder -> Selector Text
textCol = col

data SQLError = SQLError String
              | ResultSetCountError
              | TypeError MySQLValue String
              | Underflow
              | Overflow

-- | Selectors contain the target fields or expressions in a SQL
-- SELECT statement, and perform the conversion to haskell.  Selectors
-- are instances of `Applicative`, so they can return the desired
-- haskell type.
data Selector a = Selector (DList QueryBuilder)
                  (StateT [MySQLValue] (Either SQLError) a)
                  
                  
instance Functor Selector where
  fmap f (Selector cols cast) = Selector cols (fmap f cast)

instance Applicative Selector where
  Selector cols1 cast1 <*> Selector cols2 cast2 =
    Selector (cols1 <> cols2) (cast1 <*> cast2)
  pure x = Selector DList.empty (pure x)

instance Semigroup a => Semigroup (Selector a) where
  (<>) = liftA2 (<>)
    
instance Monoid a => Monoid (Selector a) where
  mempty = pure mempty

data Query a = Query (Selector a) QueryBody
data Command = Update QueryBuilder [(QueryBuilder, QueryBuilder)] QueryBody
             | InsertSelect QueryBuilder [QueryBuilder] [QueryBuilder] QueryBody
             | forall a.InsertValues QueryBuilder (Insertor a) [a]
             | forall a.Delete (Query a)

-- | An @`Insertor` a@ provides a mapping of parts of values of type
-- @a@ to columns in the database.  Insertors can be combined using `<>`.
data Insertor a = Insertor [Text] (a -> [MySQLValue])

data Join = Join JoinType [QueryBuilder] [QueryBuilder]
data JoinType = InnerJoin | LeftJoin | RightJoin | OuterJoin

instance ToQueryBuilder Command where
  toQueryBuilder (Update table setting body) =
    let pairQuery (a, b) = a <> " = " <> b
    in unwords $
       [ "UPDATE", table
       , "SET", commaSep $ map pairQuery setting
       , toQueryBuilder body
       ] 
    
  toQueryBuilder (InsertValues (QueryBuilder table _ _)
                  (Insertor cols convert) values__) =
    let builder, valuesB :: Builder
        valuesB = commaSep $
                  map (parentized . commaSep . map mysqlValueBuilder . convert)
                  values__
        builder = unwords [ "INSERT INTO", table
                          , parentized $ commaSep $
                            map (Builder.byteString . Text.encodeUtf8) cols
                          , "VALUES", valuesB]
    in QueryBuilder builder builder (DList.empty)
  toQueryBuilder (InsertSelect table cols rows queryBody) =
    unwords $
    [ "INSERT INTO", table
    , parentized $ commaSep cols
    , "SELECT", parentized $ commaSep rows
    , toQueryBuilder queryBody
    ]
  toQueryBuilder (Delete query__) =
    "DELETE " <> toQueryBuilder query__

instance ToQueryBuilder QueryBody where
  toQueryBuilder body =
    unwords $ 
    fromB (_from body) <>
    (joinB <$> _joins body) <>
    renderPredicates "WHERE" (_where_ body) <>
    (groupByB $ _groupBy body) <>
    renderPredicates "HAVING" (_having body) <>
    orderByB (_orderBy body) <>
    limitB (_limit body)
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

instance ToQueryBuilder (Query a) where
  toQueryBuilder (Query _ body) = toQueryBuilder body
        
instance ToQueryBuilder JoinType where
  toQueryBuilder InnerJoin = "INNER JOIN"
  toQueryBuilder LeftJoin = "LEFT JOIN"
  toQueryBuilder RightJoin = "RIGHT JOIN"
  toQueryBuilder OuterJoin = "OUTER JOIN"

data QueryBody = QueryBody
  { _from :: Maybe QueryBuilder
  , _joins :: [Join]
  , _where_ :: [QueryBuilder]
  , _groupBy :: [QueryBuilder]
  , _having :: [QueryBuilder]
  , _orderBy :: [QueryOrdering]
  , _limit :: Maybe (Int, Maybe Int)
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

class HasQueryClauses a where
  mergeClauses :: a -> QueryClauses -> a

instance HasQueryClauses (Query a) where
  mergeClauses (Query selector body) (QueryClauses clauses) =
    Query selector (clauses `appEndo` body)

instance HasQueryClauses Command where
  mergeClauses (Update table setting body) (QueryClauses clauses) =
    Update table setting (clauses `appEndo` body)
  mergeClauses (InsertSelect table toColumns fromColumns queryBody)
    (QueryClauses clauses) =
    InsertSelect table toColumns fromColumns (appEndo clauses queryBody)
  mergeClauses command__@(InsertValues _ _ _) _ =
    command__
  mergeClauses (Delete query__) clauses =
    Delete $ mergeClauses query__ clauses
  
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
op name e1 e2 = e1 <> " " <> fromText name <> " " <> e2

(.>), (.+), (.-) :: QueryBuilder -> QueryBuilder -> QueryBuilder
(.>) = op ">"
(.+) = op "+"
(.-) = op "-"

-- | insert a single value directly
insert1 :: ToSql a => Text -> Insertor a
insert1 s = Insertor [s] (\t -> [toSqlValue t])

-- | insert the values of a tuple
insert2 :: (ToSql a1, ToSql a2) => Text -> Text -> Insertor (a1, a2)
insert2 s1 s2 = Insertor [s1, s2] $
                   \(t1, t2) -> [toSqlValue t1, toSqlValue t2]

insert3 :: (ToSql a1, ToSql a2, ToSql a3)
        => Text -> Text -> Text -> Insertor (a1, a2, a3)
insert3 s1 s2 s3 = Insertor [s1, s2, s3] $
    \(t1, t2, t3) -> [toSqlValue t1, toSqlValue t2, toSqlValue t3]

insert4 :: (ToSql a1, ToSql a2, ToSql a3, ToSql a4)
        => Text -> Text -> Text -> Text -> Insertor (a1, a2, a3, a4)
insert4 s1 s2 s3 s4 = Insertor [s1, s2, s3, s4] $
    \(t1, t2, t3, t4) -> [toSqlValue t1, toSqlValue t2, toSqlValue t3,
                          toSqlValue t4]


insert5 :: (ToSql a1, ToSql a2, ToSql a3, ToSql a4, ToSql a5)
        => Text -> Text -> Text -> Text -> Text
        -> Insertor (a1, a2, a3, a4, a5)
insert5 s1 s2 s3 s4 s5 = Insertor [s1, s2, s3, s4, s5] $
    \(t1, t2, t3, t4, t5) -> [toSqlValue t1, toSqlValue t2, toSqlValue t3,
                          toSqlValue t4, toSqlValue t5]

insert6 :: (ToSql a1, ToSql a2, ToSql a3, ToSql a4, ToSql a5, ToSql a6)
        => Text -> Text -> Text  -> Text -> Text -> Text
        -> Insertor (a1, a2, a3, a4, a5, a6)
insert6 s1 s2 s3 s4 s5 s6 = Insertor [s1, s2, s3, s4, s5, s6] $
    \(t1, t2, t3, t4, t5, t6) -> [toSqlValue t1, toSqlValue t2, toSqlValue t3,
                          toSqlValue t4, toSqlValue t5, toSqlValue t6]

insert7 :: (ToSql a1, ToSql a2, ToSql a3, ToSql a4, ToSql a5, ToSql a6,
            ToSql a7)
        => Text -> Text  -> Text -> Text -> Text -> Text -> Text
        -> Insertor (a1, a2, a3, a4, a5, a6, a7)
insert7 s1 s2 s3 s4 s5 s6 s7 =
    Insertor [s1, s2, s3, s4, s5, s6, s7] $
    \(t1, t2, t3, t4, t5, t6, t7) ->
      [toSqlValue t1, toSqlValue t2, toSqlValue t3,
       toSqlValue t4, toSqlValue t5, toSqlValue t6,
       toSqlValue t7]

insert8 :: (ToSql a1, ToSql a2, ToSql a3, ToSql a4, ToSql a5, ToSql a6,
            ToSql a7, ToSql a8)
        => Text -> Text -> Text -> Text -> Text -> Text -> Text
        -> Text
        -> Insertor (a1, a2, a3, a4, a5, a6, a7, a8)
insert8 s1 s2 s3 s4 s5 s6 s7 s8 =
    Insertor [s1, s2, s3, s4, s5, s6, s7, s8] $
    \(t1, t2, t3, t4, t5, t6, t7, t8) ->
      [toSqlValue t1, toSqlValue t2, toSqlValue t3,
       toSqlValue t4, toSqlValue t5, toSqlValue t6,
       toSqlValue t7, toSqlValue t8]

insert9 :: (ToSql a1, ToSql a2, ToSql a3, ToSql a4, ToSql a5, ToSql a6,
            ToSql a7, ToSql a8, ToSql a9)
        => Text -> Text -> Text -> Text -> Text -> Text -> Text
        -> Text -> Text
        -> Insertor (a1, a2, a3, a4, a5, a6, a7, a8, a9)
insert9 s1 s2 s3 s4 s5 s6 s7 s8 s9 =
    Insertor [s1, s2, s3, s4, s5, s6, s7, s8, s9] $
    \(t1, t2, t3, t4, t5, t6, t7, t8, t9) ->
      [toSqlValue t1, toSqlValue t2, toSqlValue t3,
       toSqlValue t4, toSqlValue t5, toSqlValue t6,
       toSqlValue t7, toSqlValue t8, toSqlValue t9]
      
insert10 :: (ToSql a1, ToSql a2, ToSql a3, ToSql a4, ToSql a5, ToSql a6,
             ToSql a7, ToSql a8, ToSql a9, ToSql a10)
         => Text -> Text -> Text -> Text -> Text -> Text
         -> Text -> Text -> Text -> Text
         -> Insertor (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)
insert10 s1 s2 s3 s4 s5 s6 s7 s8 s9 s10 =
    Insertor [s1, s2, s3, s4, s5, s6, s7, s8, s9, s10] $
    \(t1, t2, t3, t4, t5, t6, t7, t8, t9, t10) ->
      [toSqlValue t1, toSqlValue t2, toSqlValue t3,
       toSqlValue t4, toSqlValue t5, toSqlValue t6,
       toSqlValue t7, toSqlValue t8, toSqlValue t9,
       toSqlValue t10]

-- | `into` uses the given extractor function to map the part to a
-- field.  For example:
--
-- > insertValues "Person" (fst `into` "name" <> snd `into` "age")
-- >   [("Bart Simpson", 10), ("Lisa Simpson", 8)]
into :: ToSql b => (a -> b) -> Text -> Insertor a
into toVal theField = Insertor [theField] ((:[]) . toSqlValue . toVal)

-- | `lensInto` uses a lens to map the part to a field.  For example:
--
-- > insertValues "Person" (_1 `lensInto` "name" <> _2 `lensInto` "age")
-- >   [("Bart Simpson", 10), ("Lisa Simpson", 8)]

lensInto :: ToSql b => ((b -> Const b b) -> (a -> Const b a)) -> Text
         -> Insertor a
lensInto lens theField = into (getConst . lens Const) theField

subQuery :: ToQueryBuilder a => a -> QueryBuilder
subQuery = parentized . toQueryBuilder
  
from :: QueryBuilder -> QueryClauses
from table = QueryClauses $ Endo $ \qc -> qc {_from = Just table}

joinClause :: JoinType -> [QueryBuilder] -> [QueryBuilder] -> QueryClauses
joinClause tp tables conditions = QueryClauses $ Endo $ \qc ->
  qc { _joins = Join tp tables conditions : _joins qc }

innerJoin :: [QueryBuilder] -> [QueryBuilder] -> QueryClauses
innerJoin = joinClause InnerJoin

leftJoin :: [QueryBuilder] -> [QueryBuilder] -> QueryClauses
leftJoin = joinClause LeftJoin

rightJoin :: [QueryBuilder] -> [QueryBuilder] -> QueryClauses
rightJoin = joinClause RightJoin

outerJoin :: [QueryBuilder] -> [QueryBuilder] -> QueryClauses
outerJoin = joinClause OuterJoin

emptyJoins :: QueryClauses
emptyJoins = QueryClauses $ Endo $ \qc ->
  qc { _joins = [] }

where_ :: [QueryBuilder] -> QueryClauses
where_ conditions = QueryClauses $ Endo $ \qc ->
  qc { _where_ = reverse conditions ++ _where_ qc}

emptyWhere :: QueryClauses
emptyWhere = QueryClauses $ Endo $ \qc ->
  qc { _where_ = [] }

groupBy_ :: [QueryBuilder] -> QueryClauses
groupBy_ columns = QueryClauses $ Endo $ \qc ->
  qc { _groupBy = columns }

having :: [QueryBuilder] -> QueryClauses
having conditions = QueryClauses $ Endo $ \qc ->
  qc { _having = reverse conditions ++ _having qc }

emptyHaving :: QueryClauses
emptyHaving = QueryClauses $ Endo $ \qc ->
  qc { _having = [] }

orderBy :: [QueryOrdering] -> QueryClauses
orderBy ordering = QueryClauses $ Endo $ \qc ->
  qc { _orderBy = ordering }

limit :: Int -> QueryClauses
limit count = QueryClauses $ Endo $ \qc ->
  qc { _limit = Just (count, Nothing) }

limitOffset :: Int -> Int -> QueryClauses
limitOffset count offset = QueryClauses $ Endo $ \qc ->
  qc { _limit = Just (count, Just offset) }

emptyQueryBody :: QueryBody
emptyQueryBody = QueryBody Nothing [] [] [] [] [] Nothing 

select :: Selector a -> QueryClauses -> Query a
select selector (QueryClauses clauses) =
  Query selector (clauses `appEndo` emptyQueryBody)

mergeSelect :: Query b -> (a -> b -> c) -> Selector a -> Query c
mergeSelect (Query selector2 body) f selector1 =
  Query (liftA2 f selector1 selector2) body

replaceSelect :: Selector a -> Query b -> Query a
replaceSelect s (Query _ body) = Query s body

insertValues :: QueryBuilder -> Insertor a -> [a] -> Command
insertValues = InsertValues

insertSelect :: QueryBuilder -> [QueryBuilder] -> [QueryBuilder] -> QueryClauses
             -> Command
insertSelect table toColumns fromColumns (QueryClauses clauses) =
  InsertSelect table (toColumns) (fromColumns) (appEndo clauses emptyQueryBody)

-- | combinator for aliasing columns.
as :: QueryBuilder -> QueryBuilder -> QueryBuilder
as e1 e2 = e1 <> " AS " <> e2

-- | Read the columns directly as a `MySQLValue` type without conversion.
values :: [QueryBuilder] -> Selector [MySQLValue]
values cols = Selector (DList.fromList cols) $
              state $ splitAt (length cols)

-- | Ignore the content of the given columns
values_ :: [QueryBuilder] -> Selector ()
values_ cols = () <$ values cols
  
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
          | i < fromIntegral (minBound :: a) = throwError Underflow
          | i > fromIntegral (maxBound :: a) = throwError Overflow
          | otherwise = pure $ fromIntegral i
        castFromWord :: Word64 -> Either SQLError a
        castFromWord i
          | i > fromIntegral (maxBound :: a) = throwError Overflow
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
                      | otherwise -> throwError Overflow
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
