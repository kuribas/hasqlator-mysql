{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE TupleSections #-}
module Database.Hasqlator.Typed
  ( -- * Database Types
    Table(..), Field(..), TableRef, LeftTableRef, Nullable (..),
    FromNullable, 
    
    -- * Querying
    Query, mergeQuery, updateQuery, select, mergeSelect,
    replaceSelect,

    -- * Query Clauses
    QueryClauses, mergeClauses,

    -- * Selectors
    Selector, col, colMaybe,
    -- * Expressions
    Expression, Operator, 
    arg, argMaybe, (@@), nullable, cast, unsafeCast,
    op, fun1, fun2, fun3, (.>), (.<), (.>=), (.<=), (.&&), (.||), 

    -- * Insertion
    Insertor, insertValues, into, lensInto,
    insert1, insert2, insert3, insert4, insert5,
    insert6, insert7, insert8, insert9, insert10,
    
    
    -- * Rendering Queries

    -- * imported from Database.Hasqlator
    H.Getter, H.ToSql, H.FromSql
  )
where
import Data.Text (Text)
import Data.Coerce
import qualified Data.ByteString as StrictBS
import Data.Scientific
import Data.Word
import Data.Int
import Data.Time
import Data.String
import qualified Data.HashMap.Strict as HashMap
import Control.Monad.State

import qualified Database.Hasqlator as H

data Nullable = Nullable | NotNull

type family FromNullable (nullable :: Nullable) a where
  FromNullable 'Nullable a = Maybe a
  FromNullable 'NotNull a = a

data Field table database (nullable :: Nullable) a =
  Field Text Text

newtype Expression database (nullable :: Nullable) a =
  Expression { exprBuilder :: H.QueryBuilder }

instance IsString (Expression database nullable Text) where
  fromString = arg . fromString

instance Semigroup (Expression database nullable Text) where
  (<>) = fun2 (H..++)

instance Monoid (Expression database nullable Text) where
  mempty = arg ""

instance (Num n, H.ToSql n) => Num (Expression database nullable n) where
  (+) = op (H..+)
  (-) = op (H..-)
  (*) = op (H..*)
  negate = fun1 H.negate_
  abs = fun1 H.abs_
  signum = fun1 H.signum_
  fromInteger = arg . fromInteger

instance (Fractional n, H.ToSql n)
         => Fractional (Expression database nullable n) where
  (/) = op (H../)
  fromRational = arg . fromRational
  
data Table table database = Table Text
data TableRef table database = TableRef Text
data LeftTableRef table database = LeftTableRef Text

newtype Selector database a = Selector (H.Selector a)
  deriving (Functor, Applicative, Monoid, Semigroup)

newtype Insertor table database a = Insertor (H.Insertor a)
  deriving (Monoid, Semigroup)

data LastJoin =
  NoJoin  |
  LeftJoin Text |
  RightJoin Text

data ClauseState database = ClauseState
  { clauses :: H.QueryClauses  -- clauses build so far
  , lastJoin :: LastJoin       -- type of last join, if any
  , joinCondition :: Maybe H.QueryBuilder -- conditions for last join, if any
  , aliases :: HashMap.HashMap Text Int   -- map of table names to times used
  }

newtype QueryClauses database a = QueryClauses {
  runQueryClauses :: State (ClauseState database) a }
  deriving (Functor, Applicative, Monad)

type Operator a b c = forall database nullable .
                      (Expression database nullable a ->
                       Expression database nullable b ->
                       Expression database nullable c)

data Query database tables a =
  forall hiddenState .
  Query
  (QueryClauses database (hiddenState, tables))
  ((hiddenState, tables) -> Selector database a)

class TableField tableRef nullableIn nullableOut where
  -- | reference a field from a (joined) table
  (@@) :: tableRef table database
       -> Field table database nullableIn a
       -> Expression database nullableOut a

-- a nullable field can be only used in nullable context
instance TableField TableRef 'Nullable 'Nullable where
  TableRef tableName @@ Field _ fieldName =
    Expression $ H.rawText $ tableName <> "." <> fieldName

-- a NotNull field can be used in both contexts
instance TableField TableRef 'NotNull maybeNullable where
  TableRef tableName @@ Field _ fieldName =
    Expression $ H.rawText $ tableName <> "." <> fieldName

-- left joined tables are always nullable
instance TableField LeftTableRef nullable 'Nullable where
  LeftTableRef tableName @@ Field _ fieldName =
    Expression $ H.rawText $ tableName <> "." <> fieldName

select :: QueryClauses database tables
       -> (tables -> Selector database a)
       -> Query database tables a
select body f = Query ( ((),) <$> body) (f . snd)

mergeSelect :: (tables -> Selector database a -> Selector database b) 
            -> Query database tables a
            -> Query database tables b
mergeSelect sel2 (Query body sel1) =
  Query body $ \(s, tables) -> sel2 tables (sel1 (s, tables))

replaceSelect :: (tables -> Selector database b)
              -> Query database tables a
              -> Query database tables b
replaceSelect sel (Query body _) = Query body (sel . snd)

mergeClauses :: (tables -> QueryClauses database tables)
             -> Query database tables a
             -> Query database tables a
mergeClauses updateClauses (Query body sel)  =
  Query
  (do (s, tables) <- body
      (s,) <$> updateClauses tables)
  sel

mergeQuery :: (tablesOut -> Selector database a -> Selector database b)
           -> (tablesIn -> QueryClauses database tablesOut)
           -> Query database tablesIn a
           -> Query database tablesOut b
mergeQuery updateSelector updateClauses (Query body sel) =
  Query
  (do (s, tables) <- body;
      ((s, tables), ) <$> updateClauses tables)
  (\(tablesIn, tablesOut) -> updateSelector tablesOut $ sel tablesIn)

updateQuery :: (t -> tables -> Selector database a -> Selector database b)
            -> (tables -> QueryClauses database (t, tables))
            -> Query database tables a
            -> Query database tables b
updateQuery updateSelector updateClauses (Query body sel) =
  Query
  (do (s, tables) <- body
      (t, tables') <- updateClauses tables
      pure ((s, t), tables'))
  (\((s, t), tables) -> updateSelector t tables $ sel (s, tables))

-- | make a selector from a column
col :: H.FromSql a
    => Expression database 'NotNull a
    -> Selector database a
col = Selector . H.col . exprBuilder

-- | make a selector from a column that can be null
colMaybe :: H.FromSql (Maybe a)
         => Expression database 'Nullable a
         -> Selector database (Maybe a)
colMaybe = Selector . H.col . exprBuilder

-- | pass an argument
arg :: H.ToSql a => a -> Expression database nullable a
arg = Expression . H.arg

-- | pass an argument which can be null
argMaybe :: H.ToSql a => Maybe a -> Expression database 'Nullable a
argMaybe = Expression . H.arg

-- | create an operator
op :: (H.QueryBuilder -> H.QueryBuilder -> H.QueryBuilder)
   -> Operator a b c
op = coerce

fun1 :: (H.QueryBuilder -> H.QueryBuilder)
     -> Expression database nullable a
     -> Expression database nullable b
fun1 = coerce

fun2 :: (H.QueryBuilder -> H.QueryBuilder -> H.QueryBuilder)
     -> Expression database nullable a
     -> Expression database nullable b
     -> Expression database nullable c
fun2 = coerce

fun3 :: (H.QueryBuilder -> H.QueryBuilder -> H.QueryBuilder -> H.QueryBuilder)
     -> Expression database nullable a
     -> Expression database nullable b
     -> Expression database nullable c
     -> Expression database nullable d
fun3 = coerce

(.>), (.<), (.>=), (.<=) :: H.ToSql a => Operator a a Bool
(.>) = op (H..>)
(.<) = op (H..<)
(.>=) = op (H..>=)
(.<=) = op (H..<=)

(.||), (.&&) :: Operator Bool Bool Bool
(.||) = op (H..||)
(.&&) = op (H..&&)

-- | make expression nullable
nullable :: Expression database nullable a -> Expression database 'Nullable a
nullable = coerce

class Castable a where
  -- | Safe cast.  This uses the SQL CAST function to convert safely
  -- from one type to another.
  cast :: Expression database nullable b
       -> Expression database nullable a

castTo :: H.QueryBuilder
       -> Expression database nullable b
       -> Expression database nullable a
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
unsafeCast :: Expression database nullable a -> Expression database nullable b
unsafeCast = coerce

fieldText :: Field table database nullable a -> Text
fieldText (Field table fieldName) = table <> "." <> fieldName

insert1 :: H.ToSql (FromNullable nullable a)
        => Field table database nullable a
        -> Insertor table database (FromNullable nullable a)
insert1 a = Insertor $ H.insert1 (fieldText a)

-- | insert the values of a tuple
insert2 :: (H.ToSql (FromNullable nullable1 a1),
            H.ToSql (FromNullable nullable2 a2))
        => Field table database nullable1 a1
        -> Field table database nullable2 a2
        -> Insertor table database (FromNullable nullable1 a1,
                                    FromNullable nullable2 a2)
insert2 a1 a2 = Insertor $ H.insert2 (fieldText a1) (fieldText a2)

insert3 :: (H.ToSql (FromNullable nullable1 a1),
            H.ToSql (FromNullable nullable2 a2),
            H.ToSql (FromNullable nullable3 a3))
        => Field table database nullable1 a1
        -> Field table database nullable2 a2
        -> Field table database nullable3 a3
        -> Insertor table database (FromNullable nullable1 a1,
                                    FromNullable nullable2 a2,
                                    FromNullable nullable3 a3)
insert3 a1 a2 a3 = Insertor $ H.insert3 (fieldText a1) (fieldText a2)
                   (fieldText a3)

insert4 :: (H.ToSql (FromNullable nullable1 a1),
            H.ToSql (FromNullable nullable2 a2),
            H.ToSql (FromNullable nullable3 a3),
            H.ToSql (FromNullable nullable4 a4))
        => Field table database nullable1 a1
        -> Field table database nullable2 a2
        -> Field table database nullable3 a3
        -> Field table database nullable4 a4
        -> Insertor table database (FromNullable nullable1 a1,
                                    FromNullable nullable2 a2,
                                    FromNullable nullable3 a3,
                                    FromNullable nullable4 a4)
insert4 a1 a2 a3 a4 = Insertor $
  H.insert4 (fieldText a1) (fieldText a2) (fieldText a3) (fieldText a4)

insert5 :: (H.ToSql (FromNullable nullable1 a1),
            H.ToSql (FromNullable nullable2 a2),
            H.ToSql (FromNullable nullable3 a3),
            H.ToSql (FromNullable nullable4 a4),
            H.ToSql (FromNullable nullable5 a5))
        => Field table database nullable1 a1
        -> Field table database nullable2 a2
        -> Field table database nullable3 a3
        -> Field table database nullable4 a4
        -> Field table database nullable5 a5
        -> Insertor table database (FromNullable nullable1 a1,
                                    FromNullable nullable2 a2,
                                    FromNullable nullable3 a3,
                                    FromNullable nullable4 a4,
                                    FromNullable nullable5 a5)
insert5 a1 a2 a3 a4 a5 = Insertor $
  H.insert5 (fieldText a1) (fieldText a2) (fieldText a3) (fieldText a4)
  (fieldText a5)
  
insert6 :: (H.ToSql (FromNullable nullable1 a1),
            H.ToSql (FromNullable nullable2 a2),
            H.ToSql (FromNullable nullable3 a3),
            H.ToSql (FromNullable nullable4 a4),
            H.ToSql (FromNullable nullable5 a5),
            H.ToSql (FromNullable nullable6 a6))
        => Field table database nullable1 a1
        -> Field table database nullable2 a2
        -> Field table database nullable3 a3
        -> Field table database nullable4 a4
        -> Field table database nullable5 a5
        -> Field table database nullable6 a6
        -> Insertor table database (FromNullable nullable1 a1,
                                    FromNullable nullable2 a2,
                                    FromNullable nullable3 a3,
                                    FromNullable nullable4 a4,
                                    FromNullable nullable5 a5,
                                    FromNullable nullable6 a6)
insert6 a1 a2 a3 a4 a5 a6 = Insertor $
  H.insert6 (fieldText a1) (fieldText a2) (fieldText a3) (fieldText a4)
  (fieldText a5) (fieldText a6)
  
insert7 :: (H.ToSql (FromNullable nullable1 a1),
            H.ToSql (FromNullable nullable2 a2),
            H.ToSql (FromNullable nullable3 a3),
            H.ToSql (FromNullable nullable4 a4),
            H.ToSql (FromNullable nullable5 a5),
            H.ToSql (FromNullable nullable6 a6),
            H.ToSql (FromNullable nullable7 a7))
        => Field table database nullable1 a1
        -> Field table database nullable2 a2
        -> Field table database nullable3 a3
        -> Field table database nullable4 a4
        -> Field table database nullable5 a5
        -> Field table database nullable6 a6
        -> Field table database nullable7 a7
        -> Insertor table database (FromNullable nullable1 a1,
                                    FromNullable nullable2 a2,
                                    FromNullable nullable3 a3,
                                    FromNullable nullable4 a4,
                                    FromNullable nullable5 a5,
                                    FromNullable nullable6 a6,
                                    FromNullable nullable7 a7)
insert7 a1 a2 a3 a4 a5 a6 a7 = Insertor $
  H.insert7 (fieldText a1) (fieldText a2) (fieldText a3) (fieldText a4)
  (fieldText a5) (fieldText a6) (fieldText a7)
  
insert8 :: (H.ToSql (FromNullable nullable1 a1),
            H.ToSql (FromNullable nullable2 a2),
            H.ToSql (FromNullable nullable3 a3),
            H.ToSql (FromNullable nullable4 a4),
            H.ToSql (FromNullable nullable5 a5),
            H.ToSql (FromNullable nullable6 a6),
            H.ToSql (FromNullable nullable7 a7),
            H.ToSql (FromNullable nullable8 a8))
        => Field table database nullable1 a1
        -> Field table database nullable2 a2
        -> Field table database nullable3 a3
        -> Field table database nullable4 a4
        -> Field table database nullable5 a5
        -> Field table database nullable6 a6
        -> Field table database nullable7 a7
        -> Field table database nullable8 a8
        -> Insertor table database (FromNullable nullable1 a1,
                                    FromNullable nullable2 a2,
                                    FromNullable nullable3 a3,
                                    FromNullable nullable4 a4,
                                    FromNullable nullable5 a5,
                                    FromNullable nullable6 a6,
                                    FromNullable nullable7 a7,
                                    FromNullable nullable8 a8)
insert8 a1 a2 a3 a4 a5 a6 a7 a8 = Insertor $
  H.insert8 (fieldText a1) (fieldText a2) (fieldText a3) (fieldText a4)
  (fieldText a5) (fieldText a6) (fieldText a7) (fieldText a8)
  
insert9 :: (H.ToSql (FromNullable nullable1 a1),
            H.ToSql (FromNullable nullable2 a2),
            H.ToSql (FromNullable nullable3 a3),
            H.ToSql (FromNullable nullable4 a4),
            H.ToSql (FromNullable nullable5 a5),
            H.ToSql (FromNullable nullable6 a6),
            H.ToSql (FromNullable nullable7 a7),
            H.ToSql (FromNullable nullable8 a8),
            H.ToSql (FromNullable nullable9 a9))
        => Field table database nullable1 a1
        -> Field table database nullable2 a2
        -> Field table database nullable3 a3
        -> Field table database nullable4 a4
        -> Field table database nullable5 a5
        -> Field table database nullable6 a6
        -> Field table database nullable7 a7
        -> Field table database nullable8 a8
        -> Field table database nullable9 a9
        -> Insertor table database (FromNullable nullable1 a1,
                                    FromNullable nullable2 a2,
                                    FromNullable nullable3 a3,
                                    FromNullable nullable4 a4,
                                    FromNullable nullable5 a5,
                                    FromNullable nullable6 a6,
                                    FromNullable nullable7 a7,
                                    FromNullable nullable8 a8,
                                    FromNullable nullable9 a9)
insert9 a1 a2 a3 a4 a5 a6 a7 a8 a9 = Insertor $
  H.insert9 (fieldText a1) (fieldText a2) (fieldText a3) (fieldText a4)
  (fieldText a5) (fieldText a6) (fieldText a7) (fieldText a8) (fieldText a9)
  
insert10 :: (H.ToSql (FromNullable nullable1 a1),
             H.ToSql (FromNullable nullable2 a2),
             H.ToSql (FromNullable nullable3 a3),
             H.ToSql (FromNullable nullable4 a4),
             H.ToSql (FromNullable nullable5 a5),
             H.ToSql (FromNullable nullable6 a6),
             H.ToSql (FromNullable nullable7 a7),
             H.ToSql (FromNullable nullable8 a8),
             H.ToSql (FromNullable nullable9 a9),
             H.ToSql (FromNullable nullable10 a10))
         => Field table database nullable1 a1
         -> Field table database nullable2 a2
         -> Field table database nullable3 a3
         -> Field table database nullable4 a4
         -> Field table database nullable5 a5
         -> Field table database nullable6 a6
         -> Field table database nullable7 a7
         -> Field table database nullable8 a8
         -> Field table database nullable9 a9
         -> Field table database nullable10 a10
         -> Insertor table database ((FromNullable nullable1 a1,
                                      FromNullable nullable2 a2,
                                      FromNullable nullable3 a3,
                                      FromNullable nullable4 a4,
                                      FromNullable nullable5 a5,
                                      FromNullable nullable6 a6,
                                      FromNullable nullable7 a7,
                                      FromNullable nullable8 a8,
                                      FromNullable nullable9 a9,
                                      FromNullable nullable10 a10))
insert10 a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 = Insertor $
  H.insert10 (fieldText a1) (fieldText a2) (fieldText a3) (fieldText a4)
  (fieldText a5) (fieldText a6) (fieldText a7) (fieldText a8) (fieldText a9)
  (fieldText a10)

into :: H.ToSql (FromNullable nullable b)
     => (a -> FromNullable nullable b)
     -> Field table database nullable b
     -> Insertor table database a
into f a = Insertor $ H.into f $ fieldText a

lensInto :: H.ToSql (FromNullable nullable b)
         => H.Getter a (FromNullable nullable b)
         -> Field table database nullable b
         -> Insertor table database a
lensInto lens a = Insertor $ H.lensInto lens $ fieldText a

insertValues :: Table table database
             -> Insertor table database a
             -> [a]
             -> H.Command
insertValues (Table tableName) (Insertor i) =
  H.insertValues (H.rawText tableName) i

{-
sqlInterventionOverviewClauses = do
  i <- from intervention
  od <- join objectDomain
  on $ od@@uuid .= i@@objectUuid
  p <- join plant
  on $ p@@uuid .= od@@domain_object_uuid
  upc <- join userPlantCache
  on $ upc@@plantId .= p@@id_
  io <- leftJoin identityObject
  on (io@name .= i@assignee_key)
  for_ mbUserId $ \userId -> where_ $ user_id .= io@@userId
   
sqlInterventionOverviewQuery =
  select sqlInterventionOverviewClauses $
  \(i, l, ld) -> do
    do id_ <- col $ i@@id_
       leoId <- col $ i@@interventionNumber
       ref <- ("interventions/" <>) <$> col i@@interventionNumber
       objectUuid <- col $ 
         (case_ (field inverterObjectType)
           [ ("PLANT", "plants/")
           , ("INVERTER", "inverters/")
           , ("LOGGER", "loggers/") ]
         , inverterObjectUuid ]
       pure $ Intervention {..}
-}
