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

module Database.MySQL.Hasqlator.Typed
  ( -- * Database Types
    Table(..), Field(..), TableAlias, Nullable (..),
    
    
    -- * Querying
    Query, 

    -- * Selectors
    H.Selector, col, colMaybe,

    -- * Expressions
    Expression, SomeExpression, someExpr, Operator, 
    arg, argMaybe, (@@), nullable, cast, unsafeCast,
    op, fun1, fun2, fun3, (.>), (.<), (.>=), (.<=), (.&&), (.||),
    substr, true_, false_,

    -- * Clauses
    innerJoin, leftJoin, where_, groupBy_, having, orderBy, limit, limitOffset,

    -- * Insertion
    Insertor, insertValues, insertSelect, insertData, skipInsert, into,
    lensInto, insertOne, exprInto, Into,
    
    -- * imported from Database.MySQL.Hasqlator
    H.Getter, H.ToSql, H.FromSql
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
import qualified Data.Map.Strict as Map
import Control.Monad.State
import GHC.Exts (Constraint)
import GHC.TypeLits as TL
import Data.Functor.Contravariant
import GHC.Generics

import qualified Database.MySQL.Hasqlator as H

data Nullable = Nullable | NotNull
data JoinType = LeftJoined | InnerJoined

type family CheckInsertable (fieldNullable :: Nullable) fieldType a
            :: Constraint where
  CheckInsertable 'Nullable a (Maybe a) = ()
  CheckInsertable 'Nullable a a = ()
  CheckInsertable 'NotNull a a = ()
  CheckInsertable t n ft =
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

data Field table database (nullable :: Nullable) a =
  Field Text Text
 
newtype Expression (nullable :: Nullable) a =
  Expression { exprBuilder :: H.QueryBuilder }

-- | An expression of any type
newtype SomeExpression = SomeExpression  H.QueryBuilder

-- | Remove types of an expression
someExpr :: Expression nullable a -> SomeExpression
someExpr = coerce

instance IsString (Expression nullable Text) where
  fromString = arg . fromString

instance Semigroup (Expression nullable Text) where
  (<>) = fun2 (H..++)

instance Monoid (Expression nullable Text) where
  mempty = arg ""

instance (Num n, H.ToSql n) => Num (Expression nullable n) where
  (+) = op (H..+)
  (-) = op (H..-)
  (*) = op (H..*)
  negate = fun1 H.negate_
  abs = fun1 H.abs_
  signum = fun1 H.signum_
  fromInteger = arg . fromInteger

instance (Fractional n, H.ToSql n)
         => Fractional (Expression nullable n) where
  (/) = op (H../)
  fromRational = arg . fromRational
  
data Table table database = Table Text
data TableAlias table database (joinType :: JoinType) =
  TableAlias Text

newtype Insertor table database a = Insertor (H.Insertor a)
  deriving (Monoid, Semigroup, Contravariant)

data ClauseState database = ClauseState
  { clausesBuild :: H.QueryClauses  -- clauses build so far
  , aliases :: Map.Map Text Int   -- map of aliases to times used
  }

emptyClauseState :: ClauseState database
emptyClauseState = ClauseState mempty Map.empty

type QueryInner database a = State (ClauseState database) a

newtype Query database a = Query (QueryInner database a)
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

-- | reference a field from a joined table
(@@) :: CheckExprNullable (JoinNullable leftJoined fieldNull) exprNull
     => TableAlias table database leftJoined
     -> Field table database fieldNull a
     -> Expression exprNull a
TableAlias tableName @@ Field _ fieldName =
    Expression $ H.rawSql $ tableName <> "." <> fieldName

data QueryOrdering = Asc SomeExpression | Desc SomeExpression

-- | make a selector from a column
col :: H.FromSql a
    => Expression 'NotNull a
    -> H.Selector a
col = coerce $ H.col . exprBuilder

-- | make a selector from a column that can be null
colMaybe :: H.FromSql (Maybe a)
         => Expression 'Nullable a
         -> H.Selector (Maybe a)
colMaybe = coerce $ H.col . exprBuilder

-- | pass an argument
arg :: H.ToSql a => a -> Expression nullable a
arg = Expression . H.arg

-- | pass an argument which can be null
argMaybe :: H.ToSql a => Maybe a -> Expression 'Nullable a
argMaybe = Expression . H.arg

-- | create an operator
op :: (H.QueryBuilder -> H.QueryBuilder -> H.QueryBuilder)
   -> Operator a b c
op = coerce

fun1 :: (H.QueryBuilder -> H.QueryBuilder)
     -> Expression nullable a
     -> Expression nullable b
fun1 = coerce

fun2 :: (H.QueryBuilder -> H.QueryBuilder -> H.QueryBuilder)
     -> Expression nullable a
     -> Expression nullable b
     -> Expression nullable c
fun2 = coerce

fun3 :: (H.QueryBuilder -> H.QueryBuilder -> H.QueryBuilder -> H.QueryBuilder)
     -> Expression nullable a
     -> Expression nullable b
     -> Expression nullable c
     -> Expression nullable d
fun3 = coerce

substr :: Expression nullable Text -> Expression nullable Int
       -> Expression nullable Int
       -> Expression nullable Text
substr = fun3 H.substr

(.>), (.<), (.>=), (.<=) :: H.ToSql a => Operator a a Bool
(.>) = op (H..>)
(.<) = op (H..<)
(.>=) = op (H..>=)
(.<=) = op (H..<=)

(.||), (.&&) :: Operator Bool Bool Bool
(.||) = op (H..||)
(.&&) = op (H..&&)

true_, false_ :: Expression nullable Bool
true_ = Expression $ H.rawSql "true"
false_ = Expression $ H.rawSql "false"

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
castTo tp e = Expression $ H.fun "cast" [exprBuilder e `H.as` tp]

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
-- behaviour of SQL.  Obviously this opens up more possibilies for
-- runtime errors, so it's up to the programmer to ensure it's used
-- correctly.
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
        from' = from

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
insertValues (Table tableName) (Insertor i) =
  H.insertValues (H.rawSql tableName) i

newAlias :: Text -> QueryInner database Text
newAlias prefix = do
  clsState <- get
  let newIndex = Map.findWithDefault 0 prefix (aliases clsState) + 1
  put $ clsState { aliases = Map.insert prefix newIndex $ aliases clsState}
  pure $ prefix <> Text.pack (show newIndex)

addClauses :: H.QueryClauses -> QueryInner database ()
addClauses c = modify $ \clsState ->
  clsState { clausesBuild = clausesBuild clsState <> c }

innerJoin :: Table table database
          -> (TableAlias table database 'InnerJoined ->
              Expression nullable Bool)
          -> Query database (TableAlias table database 'InnerJoined)
innerJoin (Table tableName) expr = Query $ do
  alias <- newAlias (Text.take 1 tableName)
  addClauses $ H.innerJoin [H.rawSql alias]
    [exprBuilder $ expr $ TableAlias alias]
  pure $ TableAlias alias

leftJoin :: Table table database
         -> (TableAlias table database 'LeftJoined ->
             Expression nullable Bool)
         -> Query database (TableAlias table database 'LeftJoined)
leftJoin (Table tableName) expr = Query $ do
  alias <- newAlias (Text.take 1 tableName)
  addClauses $ H.leftJoin [H.rawSql alias]
    [exprBuilder $ expr $ TableAlias alias]
  pure $ TableAlias alias

where_ :: Expression nullable Bool -> Query database ()
where_ expr = Query $ addClauses $ H.where_ [exprBuilder expr]

groupBy_ :: [SomeExpression] -> Query database ()
groupBy_ columns = Query $ addClauses $ H.groupBy_ $ coerce columns

having :: Expression nullable Bool -> Query database ()
having expr = Query $ addClauses $ H.having [exprBuilder expr]

orderBy :: [QueryOrdering] -> Query database ()
orderBy ordering = Query $ addClauses $ H.orderBy $ map orderingToH ordering
  where orderingToH (Asc x) = H.Asc (coerce x)
        orderingToH (Desc x) = H.Desc (coerce x)

limit :: Int -> Query database ()
limit count = Query $ addClauses $ H.limit count

limitOffset :: Int -> Int -> Query database ()
limitOffset count offset = Query $ addClauses $ H.limitOffset count offset

data Into database = Into
  { intoTo :: Text
  , intoFrom :: H.QueryBuilder
  }

exprInto :: CheckExprNullable exprNullable fieldNullable
         => Expression exprNullable a ->
            Field table database fieldNullable a ->
            Into database
exprInto expr (Field _ fieldName) = Into fieldName (coerce expr)

insertSelect :: Table table database
             -> Query database [Into database]
             -> H.Command
insertSelect (Table tbl) (Query query) =
  H.insertSelect (H.rawSql tbl) (map (H.rawSql . intoTo) intos)
  (map intoFrom intos) clauses
  where (intos, ClauseState clauses _) = runState query emptyClauseState
  
{-

data SqoDb

id_ :: Field "Interventions" SqoDb 'NotNull String
id_ = Field "id" "Interventions"

data sqlInterventionOverviewTables = sqlInterventionOverviewTables {
  intervention :: TableAlias SqoDb InterventionTabel
  
  }

sqlInterventionOverview = do
  i <- from intervention
  od <- join objectDomain $ \od -> od@@uuid .= i@@objectUuid
  p <- join plant $ \p -> p@@uuid .= od@@domain_object_uuid
  upc <- join userPlantCache $ \upc -> upc@@plantId .= p@@id_
  io <- leftJoin identityObject $ \io -> io@name .= i@assignee_key
  for_ mbUserId $ \userId ->
    where_ $ user_id .= io@@userId
  let sqlInterventionOverviewSelector = do
     id_ <- col ( i@@id_)
     leoId <- col $ i@@interventionNumber
       ref <- ("interventions/" <>) <$> col i@@interventionNumber
     objectUuid <- col $ 
       (case_ (field inverterObjectType)
         [ ("PLANT", "plants/")
         , ("INVERTER", "inverters/")
         , ("LOGGER", "loggers/") ]
         , inverterObjectUuid ])
     pure $ Intervention {id_, leoId, ref, objectUuid}
  pure ((i, od, p), sqlInterventionOverviewSelector)
-}
