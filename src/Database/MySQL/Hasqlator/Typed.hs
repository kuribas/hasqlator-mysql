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
{-# LANGUAGE DeriveFunctor #-}

module Database.MySQL.Hasqlator.Typed
  ( -- * Database Types
    Table(..), Field(..), Alias(..), (@@), Nullable (..), JoinType (..),
    
    -- * Querying
    Query, untypeQuery, executeQuery,

    -- * Selectors
    Selector, sel, selMaybe, forUpdate, forShare, shareMode,

    -- * Expressions
    Expression, SomeExpression, someExpr, Operator, 
    arg, argMaybe, isNull, isNotNull, nullable, notNull, orNull, unlessNull,
    cast, unsafeCast, op, fun1, fun2, fun3, (=.), (/=.), (>.), (<.), (>=.),
    (<=.), (&&.), (||.), substr, true_, false_, in_, notIn_,
    and_, or_, All_(..), Any_(..), all_, any_,
    
    -- * Clauses
    from, fromSubQuery, innerJoin, leftJoin, joinSubQuery, leftJoinSubQuery,
    where_, groupBy_, having, orderBy, limit, limitOffset,

    -- * Insertion
    Insertor, insertValues, insertUpdateValues, insertSelect, insertData,
    skipInsert, into,
    lensInto, maybeLensInto, opticInto, maybeOpticInto, insertOne, exprInto,
    Into,

    -- * Deletion
    delete,

    -- * Update
    Updator(..), update,

    -- * imported from Database.MySQL.Hasqlator
    H.Getter, H.ToSql, H.FromSql, subQueryExpr, H.executeCommand, H.Command,
    H.WaitLock
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
import GHC.TypeLits as TL
import Data.Functor.Contravariant
import Control.Applicative
import qualified GHC.Generics as Generics (from, to)
import GHC.Generics hiding (from, Selector)
import qualified Database.MySQL.Hasqlator as H
import Data.Proxy
import qualified Database.MySQL.Base as MySQL
import Optics.Core hiding (lens)

data Nullable = Nullable | NotNull
data JoinType = LeftJoined | InnerJoined

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
-- expression from a field.  For constructing records, applicativeDo
-- is the recommended way.  However note that this may fail due to a
-- bug in ghc, that breaks the polymorphism.  In that case as a
-- workaround you should use the Alias newtype directly and use the
-- `@@` operator to create an expression instead
newtype Alias table database (joinType :: JoinType) =
  Alias { getTableAlias ::
          forall fieldNull a .
          Field table database fieldNull a ->
          Expression (JoinNullable joinType fieldNull) a }

newtype Insertor (table :: Symbol) database a =
  Insertor (H.Insertor a)
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

type Operator a b c = forall nullable .
                      (Expression nullable a ->
                       Expression nullable b ->
                       Expression nullable c)

infixl 9 @@

untypeQuery :: Query database (Selector a) -> H.Query a
untypeQuery (Query query) =
  let (selector, clauseState) =
        runState (do (Selector sel) <- query; sel) emptyClauseState
  in H.select selector $ clausesBuild clauseState

executeQuery :: MySQL.MySQLConn -> Query database (Selector a) -> IO [a]
executeQuery conn query = H.executeQuery conn (untypeQuery query)
  
-- | Create an expression from an aliased table and a field.
(@@) :: Alias table database (joinType :: JoinType)
     -> Field table database fieldNull a
     -> Expression (JoinNullable joinType fieldNull) a
(@@) = getTableAlias  
 
mkTableAlias :: Text -> Alias table database leftJoined
mkTableAlias tableName = Alias $ \field ->
  Expression $ pure $ H.rawSql $ tableName <> "." <> fieldName field

emptyAlias :: Alias table database leftJoined
emptyAlias = Alias $ \field ->
  Expression $ pure $ H.rawSql $ fieldName field

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

infixr 3 &&., ||.
infix 4 <., >., >=., <=., =., /=.

(=.), (/=.), (>.), (<.), (>=.), (<=.) :: H.ToSql a => Operator a a Bool
(=.) = op (H.=.)
(/=.) = op (H./=.)
(>.) = op (H.>.)
(<.) = op (H.<.)
(>=.) = op (H.>=.)
(<=.) = op (H.<=.)

(||.), (&&.) :: Operator Bool Bool Bool
(||.) = op (H.||.)
(&&.) = op (H.&&.)

newtype All_ nullable = All_ { getAll_ :: Expression nullable Bool }

instance Semigroup (All_ nullable) where
  All_ x <> All_ y = All_ $ x &&.y
instance Monoid (All_ nullable) where
  mempty = All_ true_

and_ :: Foldable f => f (Expression nullable Bool) -> Expression nullable Bool
and_ = getAll_ . foldMap All_

all_ :: Foldable f => (a -> Expression nullable Bool) -> f a
     -> Expression nullable Bool
all_ f = getAll_ . foldMap (All_ . f)

newtype Any_ nullable = Any_ { getAny_ :: Expression nullable Bool }

instance Semigroup (Any_ nullable) where
  Any_ x <> Any_ y = Any_ $ x ||. y
instance Monoid (Any_ nullable) where
  mempty = Any_ false_

or_ :: Foldable f => f (Expression nullable Bool) -> Expression nullable Bool
or_ = getAny_ . foldMap Any_

any_ :: Foldable f => (a -> Expression nullable Bool) -> f a
     -> Expression nullable Bool
any_ f = getAny_ . foldMap (Any_ . f)
         
isNull :: Expression nullable a -> Expression 'NotNull Bool
isNull (Expression e) = Expression $ H.isNull <$> e

isNotNull :: Expression 'Nullable a -> Expression 'NotNull Bool
isNotNull (Expression e) = Expression $ H.isNotNull <$> e

true_, false_ :: Expression nullable Bool
true_ = Expression $ pure H.false_
false_ = Expression $ pure H.true_

in_ :: Expression nullable a -> [Expression nullable a]
    -> Expression nullable Bool
in_ e es = Expression $
           liftA2 H.in_ (runExpression e) (traverse runExpression es)

notIn_ :: Expression nullable a -> [Expression nullable a]
       -> Expression nullable Bool
notIn_ e es = Expression $
              liftA2 H.notIn_ (runExpression e) (traverse runExpression es)

-- | make expression nullable
nullable :: Expression nullable a -> Expression 'Nullable a
nullable = coerce

-- | ensure expression is not null
notNull :: Expression 'NotNull a -> Expression 'NotNull a
notNull = id

-- | Return a true expression if the given expression is NULL (using
-- the IS NULL sql test), or pass the expression (coerced to
-- 'NotNull) to the given test.
orNull :: Expression nullable a
       -> (Expression 'NotNull a -> Expression 'NotNull Bool)
       -> Expression 'NotNull Bool
orNull e f = isNull e ||. f (coerce e)

-- | Perform test if given expression is not NULL
unlessNull :: Expression nullable a
           -> (Expression 'NotNull a -> Expression 'NotNull Bool)
           -> Expression 'NotNull Bool
unlessNull e f = f (coerce e)

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

fieldName :: Field table database nullable a -> Text
fieldName (Field _ fn) = "`" <> fn <> "`"

insertOne :: H.ToSql a
          => Field table database 'NotNull fieldType
          -> Insertor table database a
insertOne = Insertor . H.insertOne . fieldName

insertOneMaybe :: H.ToSql a
               => Field table database 'Nullable fieldType
               -> Insertor table database (Maybe a)
insertOneMaybe = Insertor . H.insertOne . fieldName

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

instance H.ToSql b =>
         InsertGeneric tbl db (K1 r (Field tbl db 'NotNull a) ()) (K1 r b ())
  where insertDataGeneric = contramap unK1 . insertOne . unK1

instance H.ToSql b =>
         InsertGeneric tbl db (K1 r (Field tbl db 'Nullable a) ())
         (K1 r (Maybe b) ())
  where insertDataGeneric = contramap unK1 . insertOneMaybe . unK1

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

-- (a -> Expression) -> QueryInner (a -> QueryBuilder)
-- (a -> QueryInner QueryBuilder)
-- a -> queryState -> (queryState, result)
-- queryState -> a -> (query, result)
-- (a -> Expression) -> queryState -> a -> QueryBuilder
-- 

into :: (a -> Expression nullable b)
     -> Field table database nullable b
     -> Insertor table database a
into e f =
  Insertor $
  H.exprInto (\x -> evalState (runExpression $ e x) emptyClauseState)
  (fieldName f) 

lensInto :: H.ToSql b
         => H.Getter a b
         -> Field table database 'NotNull b
         -> Insertor table database a
lensInto lens a = Insertor $ H.lensInto lens $ fieldName a

maybeLensInto :: H.ToSql b
              => H.Getter a (Maybe b)
              -> Field table database 'Nullable b
              -> Insertor table database a
maybeLensInto lens a = Insertor $ H.lensInto lens $ fieldName a

opticInto :: (H.ToSql b , Is k A_Getter )
          => Optic' k is a b
          -> Field table database 'NotNull b
          -> Insertor table database a
opticInto getter field = (arg . view getter) `into` field

maybeOpticInto :: (H.ToSql b , Is k A_Getter)
               => Optic' k is a (Maybe b)
               -> Field table database 'Nullable b
               -> Insertor table database a
maybeOpticInto getter field = (argMaybe . view getter) `into` field

fullTableName :: Table table database -> Text
fullTableName (Table mbSchema tableName) =
  foldMap (\schema -> "`" <> schema <> "`.") mbSchema <>
  "`" <>
  tableName <>
  "`"
  
tableSql :: Table table database -> H.QueryBuilder
tableSql tbl = H.rawSql $ fullTableName tbl

insertValues :: Table table database
             -> Insertor table database a
             -> [a]
             -> H.Command
insertValues table (Insertor i) =
  H.insertValues (tableSql table) i

valuesAlias :: Alias table database leftJoined
valuesAlias = Alias $ \field ->
  Expression $ pure $ H.values $ H.rawSql $ fieldName field

insertUpdateValues :: Table table database
                   -> Insertor table database a
                   -> (Alias table database 'InnerJoined ->
                       Alias table database 'InnerJoined ->
                       [Updator table database])
                   -> [a]
                   -> H.Command
insertUpdateValues table (Insertor i) mkUpdators =
  H.insertUpdateValues (tableSql table) i updators
  where updators = flip evalState emptyClauseState $ 
                   traverse runUpdator $ mkUpdators emptyAlias valuesAlias
        runUpdator :: Updator table database
                   -> QueryInner (H.QueryBuilder, H.QueryBuilder)  
        runUpdator (field := Expression expr) = do
          (H.rawSql $ fieldName field, ) <$> expr

delete :: Query database (Alias table database 'InnerJoined) -> H.Command
delete (Query qry) = H.delete fields clauseBody
  where (Alias al, ClauseState clauseBody _) = runState qry emptyClauseState
        fields = flip evalState emptyClauseState $ runExpression $ al $ Field "" "*"
        
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
     -> Query database (Alias table database 'InnerJoined)
from table@(Table _ tableName) = Query $
  do alias <- newAlias (Text.take 1 tableName)
     addClauses $ H.from $ tableSql table `H.as` H.rawSql alias
     pure $ mkTableAlias alias

innerJoin :: Table table database
          -> (Alias table database 'InnerJoined ->
              Expression nullable Bool)
          -> Query database (Alias table database 'InnerJoined)
innerJoin table@(Table _ tableName) joinCondition = Query $ do
  alias <- newAlias $ Text.take 1 tableName
  let tblAlias = mkTableAlias alias
  exprBuilder <- runExpression $ joinCondition tblAlias
  addClauses $
    H.innerJoin [tableSql table `H.as` H.rawSql alias]
    [exprBuilder]
  pure tblAlias

leftJoin :: Table table database
         -> (Alias table database 'LeftJoined ->
             Expression nullable Bool)
         -> Query database (Alias table database 'LeftJoined)
leftJoin table@(Table _ tableName) joinCondition = Query $ do
  alias <- newAlias $ Text.take 1 tableName
  let tblAlias = mkTableAlias alias
  exprBuilder <- runExpression $ joinCondition tblAlias
  addClauses $
    H.leftJoin [tableSql table `H.as` H.rawSql alias]
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
     pure $ H.subQuery $ H.select (H.rawValues_ [selectBuilder]) subQueryBody

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
  pure ( H.subQuery $ H.select (H.rawValues_ selectBuilder) subQueryBody
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

where_ :: Expression 'NotNull Bool -> Query database ()
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

forUpdate :: [Table table database] -> H.WaitLock -> Query database ()
forUpdate tables waitLock = Query $ do
  addClauses $ H.forUpdate (map (H.rawSql . fullTableName) tables) waitLock

forShare :: [Table table database] -> H.WaitLock -> Query database ()
forShare tables waitLock = Query $ do
  addClauses $ H.forShare (map (H.rawSql . fullTableName) tables) waitLock

shareMode :: Query database ()
shareMode = Query $ addClauses $ H.shareMode

newtype Into database (table :: Symbol) =
  Into { runInto :: QueryInner (Text, H.QueryBuilder) }

exprInto :: Expression nullable a ->
            Field table database nullable a ->
            Into database table
exprInto expr field =
  Into $ (fieldName field,) <$> runExpression expr

insertSelect :: Table table database
             -> Query database [Into database table]
             -> H.Command
insertSelect table (Query query) =
  H.insertSelect (tableSql table)
  (map (H.rawSql . fst) intos)
  (map snd intos) clauses
  where (intos, ClauseState clauses _) =
          runState (query >>= traverse runInto) emptyClauseState

infix 0 :=

data Updator table database =
  forall nullable a.
  Field table database nullable a := Expression nullable a

update :: Table table database
       -> (Alias table database 'InnerJoined ->
           Query database [Updator table database])
       -> H.Command
update table query =
  H.update [tableSql table]
  updators clauses
  where Query runQuery = query emptyAlias
        (updators, ClauseState clauses _) =
          runState (runQuery >>= traverse runUpdator) emptyClauseState
        runUpdator :: Updator table database
                   -> QueryInner (H.QueryBuilder, H.QueryBuilder)  
        runUpdator (field := Expression expr) = do
          (H.rawSql $ fieldName field, ) <$> expr
          
          
        
          

{- TODO:

DML values:

values :: Insertable database a inExpres outExprs =>
  inExprs -> [a] -> Query database outExprs

specialized:

(Person -> Int, Person -> Maybe String) ->
  [Person] -> 
  Query database (Expression nullable Int, Expression 'Nullable String)

-}
