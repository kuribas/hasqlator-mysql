{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveFunctor #-}

module Database.MySQL.Hasqlator.Typed
  ( -- * Database Types
    Table(..), Field(..), Tbl(..), (@@), Nullable (..),
    
    
    -- * Querying
    Query, 

    -- * Selectors
    H.Selector, sel, selMaybe,

    -- * Expressions
    Expression, SomeExpression, someExpr, Operator, 
    arg, argMaybe, nullable, cast, unsafeCast,
    op, fun1, fun2, fun3, (>.), (<.), (>=.), (<=.), (&&.), (||.),
    substr, true_, false_,

    -- * Clauses
    from, fromSubQuery, innerJoin, leftJoin, joinSubQuery, leftJoinSubQuery,
    where_, groupBy_, having, orderBy, limit, limitOffset,

    -- * Insertion
    Insertor, insertValues, insertSelect, insertData, skipInsert, into,
    lensInto, insertOne, exprInto, Into,
    
    -- * imported from Database.MySQL.Hasqlator
    H.Getter, H.ToSql, H.FromSql, subQueryExpr
  )
where
import Data.Text (Text)
import qualified Data.Text as Text
import Data.Coerce
import qualified Data.ByteString as StrictBS
import Data.Scientific
import Data.Word
import Data.Int
import Data.Time
import Data.String
import qualified Data.DList  as DList
import qualified Data.Map.Strict as Map
import Control.Monad.State
import Control.Monad.Reader
import GHC.Exts (Constraint)
import GHC.TypeLits as TL
import Data.Functor.Contravariant
import Control.Applicative
import qualified GHC.Generics as Generics (from, to)
import GHC.Generics hiding (from, Selector)
import qualified Database.MySQL.Hasqlator as H
import Data.Proxy

data Nullable = Nullable | NotNull
data JoinType = LeftJoined | InnerJoined

type family CheckInsertable (fieldNullable :: Nullable) fieldType a
            :: Constraint where
  CheckInsertable 'Nullable a (Maybe a) = ()
  CheckInsertable 'Nullable a a = ()
  CheckInsertable 'NotNull a a = ()
  CheckInsertable n t ft =
    TypeError ('TL.Text "Cannot insert value of type " ':<>:
               'ShowType t ':<>:
               'TL.Text " into " ':<>:
               'ShowType n ':<>:
               'TL.Text " field of type " ':<>:
               'ShowType ft)

type Insertable nullable field a =
  (CheckInsertable nullable field a, H.ToSql a)

-- | check if field can be used in nullable context
type family CheckExprNullable (expr :: Nullable) (context :: Nullable)
     :: Constraint
  where
    CheckExprNullable 'Nullable 'Nullable = ()
    CheckExprNullable 'Nullable 'NotNull =
      TypeError ('Text "A nullable expression can be only used in a nullable context")
    -- a NotNull expressions can be used in both contexts
    CheckExprNullable 'NotNull _ = ()

-- | check if a field is nullable after being joined
type family JoinNullable (leftJoined :: JoinType) (field :: Nullable)
     :: Nullable
  where
    JoinNullable 'InnerJoined nullable = nullable
    JoinNullable 'LeftJoined _ = 'Nullable

data Field (table :: Symbol) database (nullable :: Nullable) a =
  Field Text Text
 
newtype Expression (nullable :: Nullable) a =
  Expression {runExpression :: QueryInner H.QueryBuilder }

-- | An expression of any type
newtype SomeExpression =
  SomeExpression { runSomeExpression :: QueryInner H.QueryBuilder }

newtype Selector a = Selector (QueryInner (H.Selector a))

instance Functor Selector where
  fmap f (Selector s) = Selector (fmap f <$> s)
instance Applicative Selector where
  pure x = Selector $ pure $ pure x
  Selector a <*> Selector b = Selector $ liftA2 (<*>) a b
instance Semigroup a => Semigroup (Selector a) where
  Selector a <> Selector b = Selector $ liftA2 (<>) a b
instance Monoid a => Monoid (Selector a) where
  mempty = Selector $ pure mempty

-- | Remove types of an expression
someExpr :: Expression nullable a -> SomeExpression
someExpr = coerce

instance IsString (Expression nullable Text) where
  fromString = arg . fromString

instance Semigroup (Expression nullable Text) where
  (<>) = fun2 (H.++.)

instance Monoid (Expression nullable Text) where
  mempty = arg ""

instance (Num n, H.ToSql n) => Num (Expression nullable n) where
  (+) = op (H.+.)
  (-) = op (H.-.)
  (*) = op (H.*.)
  negate = fun1 H.negate_
  abs = fun1 H.abs_
  signum = fun1 H.signum_
  fromInteger = arg . fromInteger

instance (Fractional n, H.ToSql n)
         => Fractional (Expression nullable n) where
  (/) = op (H./.)
  fromRational = arg . fromRational
  
data Table (table :: Symbol) database = Table (Maybe Text) Text

-- | An table alias that can be used inside the Query.  The function
-- inside the newtype can also be applied directly to create an
-- expression from a field.
newtype Tbl table database (joinType :: JoinType) =
  Tbl { getTableAlias ::
          forall fieldNull exprNull a .
          CheckExprNullable (JoinNullable joinType fieldNull) exprNull =>
          Field table database fieldNull a ->
          Expression exprNull a }

newtype Insertor (table :: Symbol) database a = Insertor (H.Insertor a)
  deriving (Monoid, Semigroup, Contravariant)

data ClauseState = ClauseState
  { clausesBuild :: H.QueryClauses  -- clauses build so far
  , aliases :: Map.Map Text Int   -- map of aliases to times used
  }

emptyClauseState :: ClauseState
emptyClauseState = ClauseState mempty Map.empty

type QueryInner a = State ClauseState a

newtype Query database a = Query (QueryInner a)
  deriving (Functor, Applicative, Monad)

instance H.ToQueryBuilder (Query database (H.Selector a)) where
  toQueryBuilder (Query query) =
    let (selector, clauseState) = runState query emptyClauseState
    -- TODO: finalize query
    in H.toQueryBuilder $ H.select selector $ clausesBuild clauseState

type Operator a b c = forall nullable .
                      (Expression nullable a ->
                       Expression nullable b ->
                       Expression nullable c)

infixl 9 @@

-- | Create an expression from an aliased table and a field.
(@@) :: CheckExprNullable (JoinNullable joinType fieldNull) exprNull
     => Tbl table database (joinType :: JoinType)
     -> Field table database fieldNull a
     -> Expression exprNull a
(@@) = getTableAlias  
 
mkTableAlias :: Text -> Tbl table database leftJoined
mkTableAlias tableName = Tbl $ \(Field _ fieldName) ->
  Expression $ pure $ H.rawSql $ tableName <> "." <> fieldName

data QueryOrdering = Asc SomeExpression | Desc SomeExpression

-- | make a selector from a column
sel :: H.FromSql a
    => Expression 'NotNull a
    -> Selector a
sel (Expression expr) = Selector $ H.sel <$> expr

-- | make a selector from a column that can be null
selMaybe :: H.FromSql (Maybe a)
         => Expression 'Nullable a
         -> Selector (Maybe a)
selMaybe (Expression expr) = Selector $ H.sel <$> expr

-- | pass an argument
arg :: H.ToSql a => a -> Expression nullable a
arg x = Expression $ pure $ H.arg x

-- | pass an argument which can be null
argMaybe :: H.ToSql a => Maybe a -> Expression 'Nullable a
argMaybe x = Expression $ pure $ H.arg x

-- | create an operator
op :: (H.QueryBuilder -> H.QueryBuilder -> H.QueryBuilder)
   -> Operator a b c
op = fun2

fun1 :: (H.QueryBuilder -> H.QueryBuilder)
     -> Expression nullable a
     -> Expression nullable b
fun1 f (Expression x) = Expression $ f <$> x

fun2 :: (H.QueryBuilder -> H.QueryBuilder -> H.QueryBuilder)
     -> Expression nullable a
     -> Expression nullable b
     -> Expression nullable c
fun2 f (Expression x1) (Expression x2) = Expression $ liftA2 f x1 x2

fun3 :: (H.QueryBuilder -> H.QueryBuilder -> H.QueryBuilder -> H.QueryBuilder)
     -> Expression nullable a
     -> Expression nullable b
     -> Expression nullable c
     -> Expression nullable d
fun3 f (Expression x1) (Expression x2) (Expression x3) =
  Expression $ liftA3 f x1 x2 x3

substr :: Expression nullable Text -> Expression nullable Int
       -> Expression nullable Int
       -> Expression nullable Text
substr = fun3 H.substr

(>.), (<.), (>=.), (<=.) :: H.ToSql a => Operator a a Bool
(>.) = op (H.>.)
(<.) = op (H.<.)
(>=.) = op (H.>=.)
(<=.) = op (H.<=.)

(||.), (&&.) :: Operator Bool Bool Bool
(||.) = op (H.||.)
(&&.) = op (H.&&.)

true_, false_ :: Expression nullable Bool
true_ = Expression $ pure $ H.rawSql "true"
false_ = Expression $ pure $ H.rawSql "false"

-- | make expression nullable
nullable :: Expression nullable a -> Expression 'Nullable a
nullable = coerce

class Castable a where
  -- | Safe cast.  This uses the SQL CAST function to convert safely
  -- from one type to another.
  cast :: Expression nullable b
       -> Expression nullable a

castTo :: H.QueryBuilder
       -> Expression nullable b
       -> Expression nullable a
castTo tp (Expression e) = Expression $ do
  x <- e
  pure $ H.fun "cast" [x `H.as` tp]

instance Castable StrictBS.ByteString where
  cast = castTo "BINARY"

instance Castable Text where
  cast = castTo "CHAR UNICODE"

instance Castable Day where
  cast = castTo "DATE"

instance Castable LocalTime where
  cast = castTo "DATETIME"

instance Castable Scientific where
  cast = castTo "DECIMAL"

instance Castable Double where
  cast = castTo "FLOAT[53]"

instance Castable Int where
  cast = castTo "SIGNED"

instance Castable Int8 where
  cast = castTo "SIGNED"

instance Castable Int16 where
  cast = castTo "SIGNED"

instance Castable Int32 where
  cast = castTo "SIGNED"

instance Castable Int64 where
  cast = castTo "SIGNED"

instance Castable TimeOfDay where
  cast = castTo "TIME"

instance Castable DiffTime where
  cast = castTo "TIME"

instance Castable Word where
  cast = castTo "UNSIGNED"

instance Castable Word8 where
  cast = castTo "UNSIGNED"

instance Castable Word16 where
  cast = castTo "UNSIGNED"

instance Castable Word32 where
  cast = castTo "UNSIGNED"

instance Castable Word64 where
  cast = castTo "UNSIGNED"

-- | Cast the return type of an expression to any other type, without
-- changing the query. Since this library adds static typing on top of
-- SQL, you may sometimes want to use this to get back the lenient
-- behaviour of SQL.  This opens up more possibilies for runtime
-- errors, so it's up to the programmer to ensure type correctness.
unsafeCast :: Expression nullable a -> Expression nullable b
unsafeCast = coerce

fieldText :: Field table database nullable a -> Text
fieldText (Field table fieldName) = table <> "." <> fieldName

insertOne :: Insertable fieldNull fieldType a
          => Field table database fieldNull fieldType
          -> Insertor table database a
insertOne = Insertor . H.insertOne. fieldText

genFst :: (a :*: b) () -> a ()
genFst (a :*: _) = a

genSnd :: (a :*: b) () -> b ()
genSnd (_ :*: b) = b

class InsertGeneric table database (fields :: *) (data_ :: *) where
  insertDataGeneric :: fields -> Insertor table database data_

instance (InsertGeneric tbl db (a ()) (c ()),
          InsertGeneric tbl db (b ()) (d ())) =>
         InsertGeneric tbl db ((a :*: b) ()) ((c :*: d) ()) where
  insertDataGeneric (a :*: b) =
    contramap genFst (insertDataGeneric a) <>
    contramap genSnd (insertDataGeneric b)

instance InsertGeneric tbl db (a ()) (b ()) =>
         InsertGeneric tbl db (M1 m1 m2 a ()) (M1 m3 m4 b ()) where
  insertDataGeneric = contramap unM1 . insertDataGeneric . unM1

instance Insertable fieldNull a b =>
  InsertGeneric tbl db (K1 r (Field tbl db fieldNull a) ()) (K1 r b ()) where
  insertDataGeneric = contramap unK1 . insertOne . unK1

instance InsertGeneric tbl db (K1 r (Insertor tbl db a) ()) (K1 r a ()) where
  insertDataGeneric = contramap unK1 . unK1

insertData :: (Generic a, Generic b, InsertGeneric tbl db (Rep a ()) (Rep b ()))
           => a -> Insertor tbl db b
insertData = contramap from' . insertDataGeneric . from'
  where from' :: Generic a => a -> Rep a ()
        from' = Generics.from

skipInsert :: Insertor tbl db a
skipInsert = mempty

{-
personInsertor :: Insertor table database Person
personInsertor = insertData (name, age)
-}

into :: Insertable fieldNull fieldType b
        => (a -> b)
        -> Field table database fieldNull fieldType
        -> Insertor table database a
into f = Insertor . H.into f . fieldText

lensInto :: Insertable fieldNull fieldType b
         => H.Getter a b
         -> Field table database fieldNull fieldType
         -> Insertor table database a
lensInto lens a = Insertor $ H.lensInto lens $ fieldText a

insertValues :: Table table database
             -> Insertor table database a
             -> [a]
             -> H.Command
insertValues (Table _schema tableName) (Insertor i) =
  H.insertValues (H.rawSql tableName) i

newAlias :: Text -> QueryInner Text
newAlias prefix = do
  clsState <- get
  let newIndex = Map.findWithDefault 0 prefix (aliases clsState) + 1
  put $ clsState { aliases = Map.insert prefix newIndex $ aliases clsState}
  pure $ prefix <> Text.pack (show newIndex)

addClauses :: H.QueryClauses -> QueryInner ()
addClauses c = modify $ \clsState ->
  clsState { clausesBuild = clausesBuild clsState <> c }

from :: Table table database
     -> Query database (Tbl table database 'InnerJoined)
from (Table schema tableName) = Query $
  do alias <- newAlias (Text.take 1 tableName)
     addClauses $ H.from $
       H.rawSql (maybe mempty (<> ".") schema <> tableName) `H.as` H.rawSql alias
     pure $ mkTableAlias alias

innerJoin :: Table table database
          -> (Tbl table database 'InnerJoined ->
              Expression nullable Bool)
          -> Query database (Tbl table database 'InnerJoined)
innerJoin (Table schema tableName) joinCondition = Query $ do
  alias <- newAlias (Text.take 1 tableName)
  let tblAlias = mkTableAlias alias
  exprBuilder <- runExpression $ joinCondition tblAlias
  addClauses $
    H.innerJoin [H.rawSql (maybe mempty (<> ".") schema <> tableName) `H.as`
                 H.rawSql alias]
    [exprBuilder]
  pure tblAlias

leftJoin :: Table table database
         -> (Tbl table database 'LeftJoined ->
             Expression nullable Bool)
         -> Query database (Tbl table database 'LeftJoined)
leftJoin (Table schema tableName) joinCondition = Query $ do
  alias <- newAlias (Text.take 1 tableName)
  let tblAlias = mkTableAlias alias
  exprBuilder <- runExpression $ joinCondition tblAlias
  addClauses $
    H.leftJoin [H.rawSql (maybe mempty (<> ".") schema <> tableName) `H.as`
                H.rawSql alias]
    [exprBuilder]
  pure tblAlias


class SubQueryExpr (joinType :: JoinType) inExpr outExpr where
  -- The ReaderT monad takes the alias of the subquery table.
  -- The State monad uses the index of the last expression in the select clause.
  -- It generates SELECT clause expressions with aliases e1, e2, ...
  -- The outExpr contains just the output aliases.
  -- input and output should be product types, and have the same number of
  -- elements.
  subJoinGeneric :: Proxy joinType
                 -> inExpr
                 -> ReaderT Text (State Int)
                    (DList.DList SomeExpression, outExpr)

instance ( SubQueryExpr joinType (a ()) (c ())
         , SubQueryExpr joinType (b ()) (d ())) =>
         SubQueryExpr joinType ((a :*: b) ()) ((c :*: d) ()) where
  subJoinGeneric p (l :*: r) = do
    (lftBuilder, outLft) <- subJoinGeneric p l
    (rtBuilder, outRt) <- subJoinGeneric p r
    pure (lftBuilder <> rtBuilder, outLft :*: outRt)
          
instance SubQueryExpr joinType (a ()) (b ()) =>
         SubQueryExpr joinType (M1 m1 m2 a ()) (M1 m3 m4 b ()) where
  subJoinGeneric p (M1 x) = fmap M1 <$> subJoinGeneric p x
    
instance JoinNullable joinType nullable ~ nullable2 => 
         SubQueryExpr
         joinType
         (K1 r (Expression nullable a) ())
         (K1 r (Expression nullable2 b) ())
  where
  subJoinGeneric _ (K1 (Expression exprBuilder)) =
    ReaderT $ \alias -> state $ \i ->
    let name = Text.pack ('e': show i)
    in ( ( DList.singleton $ SomeExpression $
           (`H.as` H.rawSql name) <$> exprBuilder
         , K1 $ Expression $ pure $ H.rawSql $ alias <> "." <> name)
       , i+1
       )

-- update the aliases, but create and return new query clauses
runAsSubQuery :: Query database a -> QueryInner (H.QueryClauses, a)
runAsSubQuery (Query sq) =
  do ClauseState currentClauses currentAliases <- get
     let (subQueryRet, ClauseState subQueryBody newAliases) =
           runState sq (ClauseState mempty currentAliases)
     put $ ClauseState currentClauses newAliases
     pure (subQueryBody, subQueryRet)

subQueryExpr :: Query database (Expression nullable a) -> Expression nullable a
subQueryExpr sq = Expression $
  do (subQueryBody, Expression subQuerySelect) <- runAsSubQuery sq 
     selectBuilder <- subQuerySelect
     pure $ H.subQuery $ H.select (H.values_ [selectBuilder]) subQueryBody

-- 
subJoinBody :: (Generic inExprs,
              Generic outExprs,
              SubQueryExpr joinType (Rep inExprs ()) (Rep outExprs ()))
            => Proxy joinType
            -> Query database inExprs
            -> QueryInner (H.QueryBuilder, outExprs)
subJoinBody p sq = do
  sqAlias <- newAlias "sq"
  (subQueryBody, sqExprs) <- runAsSubQuery sq
  let from' :: Generic inExprs => inExprs -> Rep inExprs ()
      from' = Generics.from
      to' :: Generic outExpr => Rep outExpr () -> outExpr
      to' = Generics.to
      (selectExprs, outExprRep) = flip evalState 1 $
                                  flip runReaderT sqAlias $
                                  subJoinGeneric p $
                                  from' sqExprs
      outExpr = to' outExprRep
  selectBuilder <- DList.toList <$> traverse runSomeExpression selectExprs
  pure ( H.subQuery $ H.select (H.values_ selectBuilder) subQueryBody
       , outExpr)

joinSubQuery :: (Generic inExprs,
                 Generic outExprs,
                 SubQueryExpr 'InnerJoined (Rep inExprs ()) (Rep outExprs ()))
             => Query database inExprs
             -> (outExprs -> Expression nullable Bool)
             -> Query database outExprs
joinSubQuery sq condition = Query $ do
  (subQueryBody, outExpr) <- subJoinBody (Proxy :: Proxy 'InnerJoined) sq
  conditionBuilder <- runExpression $ condition outExpr
  addClauses $ H.innerJoin [subQueryBody] [conditionBuilder]
  pure outExpr

leftJoinSubQuery :: (Generic inExprs,
                     Generic outExprs,
                     SubQueryExpr 'LeftJoined (Rep inExprs ()) (Rep outExprs ()))
                 => Query database inExprs
                 -> (outExprs -> Expression nullable Bool)
                 -> Query database outExprs
leftJoinSubQuery sq condition = Query $ do
  (subQueryBody, outExpr) <- subJoinBody (Proxy :: Proxy 'LeftJoined) sq
  conditionBuilder <- runExpression $ condition outExpr
  addClauses $ H.leftJoin [subQueryBody] [conditionBuilder]
  pure outExpr

fromSubQuery :: (Generic inExprs,
                 Generic outExprs,
                 SubQueryExpr 'LeftJoined (Rep inExprs ()) (Rep outExprs ()))
             => Query database inExprs
             -> Query database outExprs
fromSubQuery sq = Query $ do              
  (subQueryBody, outExpr) <- subJoinBody (Proxy :: Proxy 'LeftJoined) sq 
  addClauses $ H.from subQueryBody
  pure outExpr

where_ :: Expression nullable Bool -> Query database ()
where_ expr = Query $ do
  exprBuilder <- runExpression expr
  addClauses $ H.where_ [exprBuilder]

groupBy_ :: [SomeExpression] -> Query database ()
groupBy_ columns = Query $ do
  columnBuilders <- traverse runSomeExpression columns
  addClauses $ H.groupBy_ columnBuilders

having :: Expression nullable Bool -> Query database ()
having expr = Query $ do
  exprBuilder <- runExpression expr
  addClauses $ H.having [exprBuilder]

orderBy :: [QueryOrdering] -> Query database ()
orderBy ordering = Query $
  do newOrdering <- traverse orderingToH ordering
     addClauses $ H.orderBy newOrdering
  where orderingToH (Asc x) = H.Asc <$> runSomeExpression x
        orderingToH (Desc x) = H.Desc <$> runSomeExpression x

limit :: Int -> Query database ()
limit count = Query $ addClauses $ H.limit count

limitOffset :: Int -> Int -> Query database ()
limitOffset count offset = Query $ addClauses $ H.limitOffset count offset

newtype Into database (table :: Symbol) =
  Into { runInto :: QueryInner (Text, H.QueryBuilder) }

exprInto :: CheckExprNullable exprNullable fieldNullable
         => Expression exprNullable a ->
            Field table database fieldNullable a ->
            Into database table
exprInto expr (Field _ fieldName) =
  Into $ (fieldName,) <$> runExpression expr

insertSelect :: Table table database
             -> Query database [Into database table]
             -> H.Command
insertSelect (Table schema tbl) (Query query) =
  H.insertSelect (H.rawSql (maybe mempty (<> ".") schema <> tbl))
  (map (H.rawSql . fst) intos)
  (map snd intos) clauses
  where (intos, ClauseState clauses _) =
          runState (query >>= traverse runInto) emptyClauseState

