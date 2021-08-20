{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
module Database.MySQL.Hasqlator.Typed.Schema
  (TableInfo(..), ColumnInfo(..), fetchTableInfo, Sign(..), ColumnType(..),
   pprTableInfo, Properties(..), defaultProperties, makeFields,
   smartUpcase, smartDowncase,
   makeRecords, makeSelectors, makeInsertors) where
import Database.MySQL.Hasqlator
import qualified Database.MySQL.Hasqlator.Typed as T
import Database.MySQL.Base(MySQLConn)
import qualified Data.Text as Text
import qualified Data.Text.Lazy as LazyText
import Text.Megaparsec
import qualified Data.Map as Map
import qualified Data.Set as Set
import Text.Megaparsec.Char
import Data.Text (Text)
import Control.Applicative (liftA2)
import Language.Haskell.TH
import Data.Word
import Data.Int
import Data.Scientific
import Data.Time
import qualified Data.ByteString as StrictBS
import Data.Aeson(Value)
import Data.Char
import GHC.TypeLits(Symbol)
import Text.Pretty.Simple

data Sign = Signed | Unsigned
  deriving Show

data ColumnType =
  TinyIntColumn Sign |
  SmallIntColumn Sign |
  MediumIntColumn Sign |
  IntColumn Sign |
  BigIntColumn Sign |
  DecimalColumn Int Int Sign |
  VarCharColumn Int |
  CharColumn Int |
  TextColumn |
  BlobColumn |
  DateTimeColumn Int |
  TimestampColumn Int |
  DateColumn |
  TimeColumn Int |
  DoubleColumn |
  FloatColumn |
  EnumColumn [Text] |
  SetColumn [Text] |
  BinaryColumn Int |
  VarBinaryColumn Int | 
  BitColumn Int |
  JsonColumn
  deriving (Show)

data ColumnInfo = ColumnInfo
  { columnTableSchema :: Text
  , columnTableName :: Text
  , columnName :: Text
  , columnType :: ColumnType
  , columnNullable :: Bool
  , primaryKey :: Bool
  , autoIncrement :: Bool
  , foreignKey :: Maybe (Text, Text, Text)
  } deriving (Show)

data TableInfo = TableInfo
  { tableName :: Text
  , tableSchema :: Text
  , tableColumns :: [ColumnInfo]
  } deriving (Show)

argsP :: Parsec () Text a -> Parsec () Text [a]
argsP p = between (single '(') (single ')') $ sepBy p (single ',')

arg1P :: Parsec () Text a -> Parsec () Text a
arg1P = between (single '(') (single ')')

arg2P :: Parsec () Text a -> Parsec () Text (a, a)
arg2P p = between (single '(') (single ')') $ liftA2 (,) p (single ',' >> p)

stringP :: Parsec () Text Text
stringP = fmap Text.pack $ between (single '\'') (single '\'') $ many $
          noneOf ['\'', '\\'] <|> (single '\\' >> anySingle)

intP :: Parsec () Text Int
intP = read <$> many digitChar

signP :: Parsec () Text Sign
signP = option Signed $ Unsigned <$ string "unsigned"

parseType :: Text -> ColumnType
parseType t = case runParser typeParser "COLUMN_TYPE" t of
  Left err -> error $ show err
  Right res -> res

typeParser :: Parsec () Text ColumnType
typeParser = do
  typename <- many letterChar
  case typename of
    -- ignore the "display width" argument
    "tinyint" -> fmap TinyIntColumn $ optional (argsP intP) *> space *> signP
    "smallint" -> fmap SmallIntColumn $ optional (argsP intP) *> space *> signP
    "mediumint" -> fmap MediumIntColumn $ optional (argsP intP) *> space *> signP
    "int" -> fmap IntColumn $ optional (argsP intP) *> space *> signP
    "bigint" -> fmap BigIntColumn $ optional (argsP intP) *> space *> signP
    "decimal" -> do (m, d) <- arg2P intP
                    space
                    sign <- signP
                    pure $ DecimalColumn m d sign
    "varchar" -> VarCharColumn <$> arg1P intP
    "char" -> CharColumn <$> arg1P intP
    "tinytext" -> pure TextColumn
    "text" -> pure TextColumn
    "mediumtext" -> pure TextColumn
    "longtext" -> pure TextColumn
    "tinyblob" -> pure BlobColumn
    "blob" -> pure BlobColumn
    "mediumblob" -> pure BlobColumn
    "longblob" -> pure BlobColumn
    "datetime" -> fmap DateTimeColumn $ option 0 $ arg1P intP
    "date" -> pure DateColumn 
    "time" -> fmap TimeColumn $ option 0 $ arg1P intP
    "timestamp" -> fmap TimestampColumn $ option 0 $ arg1P intP
    "float" -> pure FloatColumn
    "double" -> pure DoubleColumn
    "enum" -> EnumColumn <$> argsP stringP
    "set" -> SetColumn <$> argsP stringP
    "binary" -> VarBinaryColumn <$> arg1P intP
    "varbinary" -> VarBinaryColumn <$> arg1P intP
    "json" -> pure JsonColumn
    "bit" -> BitColumn <$> arg1P intP
    x -> fail $ "Don't know how to parse COLUMN_TYPE " <> show x

tableQuery :: [Text] -> Query TableInfo
tableQuery schemas =
  select tableSelector $
  from "information_schema.TABLES" <>
  where_ ["TABLE_SCHEMA" `in_` map arg schemas]
  where tableSelector = do
          tableName_ <- textSel "TABLE_NAME"
          tableSchema_ <- textSel "TABLE_SCHEMA"
          pure $ TableInfo{ tableName = tableName_
                          , tableSchema = tableSchema_
                          , tableColumns = []}
  
columnsQuery :: [Text] -> Query ColumnInfo
columnsQuery schemas =
  select columnSelector $
  from ("information_schema.COLUMNS" `as` "c")
  <> leftJoin ["information_schema.KEY_COLUMN_USAGE" `as` "k"]
  [ "k.TABLE_SCHEMA" =. "c.TABLE_SCHEMA"
  , "k.TABLE_NAME" =. "c.TABLE_NAME"
  , "k.COLUMN_NAME" =. "c.COLUMN_NAME"
  , "k.REFERENCED_COLUMN_NAME IS NOT NULL" 
  ]
  <> where_ ["c.TABLE_SCHEMA" `in_` map arg schemas]
  where columnSelector = do
          tableName_ <- textSel "c.TABLE_NAME"
          tableSchema_ <- textSel "c.TABLE_SCHEMA"
          columnName_ <- textSel "c.COLUMN_NAME"
          nullable <- textSel "c.IS_NULLABLE"
          columnType_ <- textSel "c.COLUMN_TYPE"
          columnKey <- textSel "c.COLUMN_KEY"
          extra <- textSel "c.EXTRA"
          referencedTableSchema <-
            sel "k.REFERENCED_TABLE_SCHEMA" :: Selector (Maybe Text)
          referencedTableName <- 
            sel "k.REFERENCED_TABLE_NAME" :: Selector (Maybe Text)
          referencedColumnName <- 
            sel "k.REFERENCED_COLUMN_NAME" :: Selector (Maybe Text)
          pure $ ColumnInfo
            { columnTableSchema = tableSchema_
            , columnTableName = tableName_
            , columnType = parseType columnType_
            , columnName = columnName_
            , columnNullable = nullable == "YES"
            , primaryKey = columnKey == "PRI"
            , autoIncrement = "auto_increment" `Text.isInfixOf` extra
            , foreignKey = (,,)
                           <$> referencedTableSchema
                           <*> referencedTableName
                           <*> referencedColumnName
            }

-- | Fetch TableInfo structures for each of the given schemas, using
-- the given `MySQLConn` connection.
fetchTableInfo :: MySQLConn -> [Text] -> IO [TableInfo]
fetchTableInfo conn schemas = do
  tables <- executeQuery conn $ tableQuery schemas
  columns <- executeQuery conn $ columnsQuery schemas
  let columnMap = Map.fromListWith (++) $
                  map (\ci -> ( (columnTableSchema ci, columnTableName ci)
                              , [ci]))
                  columns
  pure $
    map (\tbl@TableInfo{tableName, tableSchema} ->
           tbl {tableColumns = Map.findWithDefault [] (tableSchema, tableName)
                               columnMap} )
    tables

pprTableInfo :: LazyText.Text -> [TableInfo] -> LazyText.Text
pprTableInfo name ti =
  LazyText.unlines $ ((name <> " = ") : ) $
  map ("  " <>) $ LazyText.lines $ pShowNoColor ti

columnTHType :: Bool -> ColumnInfo -> Q Type
columnTHType ignoreMaybe ColumnInfo{ columnType, columnNullable}
  | columnNullable && not ignoreMaybe = [t| Maybe $(tp) |]
  | otherwise = tp
  where tp = case columnType of
          TinyIntColumn _  -> [t| Int |]
          SmallIntColumn _ -> [t| Int |]
          MediumIntColumn _ -> [t| Int |]
          IntColumn _ -> [t| Int |]
          BigIntColumn Signed -> [t| Int64 |]
          BigIntColumn Unsigned -> [t| Word64 |]
          DecimalColumn{} -> [t| Scientific |]
          VarCharColumn _ -> [t| Text |]
          CharColumn _ -> [t| Text |]
          TextColumn -> [t| Text |]
          DateTimeColumn _ -> [t| LocalTime |]
          TimestampColumn _ -> [t| LocalTime |]
          DateColumn -> [t| Day |]
          TimeColumn _ -> [t| TimeOfDay |]
          DoubleColumn -> [t| Double |]
          FloatColumn -> [t| Float |]
          EnumColumn _ -> [t| Text |]
          SetColumn _ -> [t| Text |]
          BinaryColumn _ -> [t| StrictBS.ByteString |]
          VarBinaryColumn _ -> [t| StrictBS.ByteString |]
          BlobColumn -> [t| StrictBS.ByteString |]
          BitColumn _ -> [t| Word64 |]
          JsonColumn -> [t| Value |]

data Properties = Properties
  { fieldNameModifier :: ColumnInfo -> String
  , tableNameModifier :: TableInfo -> String
  , classNameModifier :: String -> String
  , includeSchema :: Bool
  , insertorTypeModifier :: TableInfo -> String
  , insertorNameModifier :: TableInfo -> String
  , selectorNameModifier :: TableInfo -> String
  , fieldsQualifier :: String
  , insertorFieldModifier :: ColumnInfo -> String
  }


smartDowncase :: String -> String
smartDowncase (c:cs)
  | isAlpha c = let (l, r) = span isUpper (c:cs)
                in map toLower l ++ r
  | otherwise = '_' : c : cs
smartDowncase "" = ""

smartUpcase :: String -> String
smartUpcase (c:cs)
  | isAlpha c = toUpper c : cs
  | otherwise = '_' : c : cs
smartUpcase "" = ""

removeUnderscore :: String -> String
removeUnderscore ('_':c:cs)
  | isAlpha c = toUpper c : removeUnderscore cs
removeUnderscore (c:cs) = c:removeUnderscore cs
removeUnderscore [] = []

defaultProperties :: Properties
defaultProperties = Properties
  { fieldNameModifier = \ColumnInfo{columnName} ->
      let n = Text.unpack columnName
      in smartDowncase n <> (if smartDowncase n `elem` reserved then "_" else mempty)
  , tableNameModifier = \TableInfo{tableName} ->
      let n = Text.unpack tableName
      in smartDowncase n <>
         (if smartDowncase n `elem` reserved then "_" else mempty) <> "_tbl"
  , classNameModifier = \n -> "Has_" <> n <> "_field"
  , includeSchema = True
  , insertorTypeModifier = \TableInfo{tableName} ->
      let n = Text.unpack tableName
      in removeUnderscore (smartUpcase n) <> "Fields"
  , insertorNameModifier = \TableInfo{tableName} ->
      let n = Text.unpack tableName
      in smartDowncase (removeUnderscore n) <> "Insertor"
  , selectorNameModifier = \TableInfo{tableName} ->
      let n = Text.unpack tableName
      in n <> "_sel"
  , fieldsQualifier = ""
  , insertorFieldModifier = \ColumnInfo{columnName} ->
      let n = Text.unpack columnName
      in smartDowncase n <> "_field"
  }
    where reserved :: [String]
          reserved = ["id", "class", "data", "type", "foreign", "import",
                      "default", "case"]

getColumnTableName :: Properties -> ColumnInfo -> String
getColumnTableName Properties{includeSchema}
                   ColumnInfo{columnTableSchema, columnTableName}
  | includeSchema = Text.unpack columnTableSchema <> "." <>
                    Text.unpack columnTableName
  | otherwise = Text.unpack columnTableName
          
getTableName :: Properties -> TableInfo -> String
getTableName Properties{includeSchema}
             TableInfo{tableSchema, tableName}
  | includeSchema = Text.unpack tableSchema <> "." <>
                    Text.unpack tableName
  | otherwise = Text.unpack tableName
          
makeField :: Properties -> Name -> ColumnInfo -> Q [Dec]
makeField props dbName ci@ColumnInfo{columnName
                                    ,columnNullable} =
  sequence [ sigD
             (mkName $ fieldNameModifier props ci)
             [t| T.Field
                 $(litT $ strTyLit tableName)
                 $(conT dbName)
                 $(if columnNullable
                    then promotedT 'T.Nullable
                    else promotedT 'T.NotNull)
                 $(columnTHType True ci)
               |]
           , valD (varP $ mkName $ fieldNameModifier props ci)
             (normalB [e| T.Field
                          $(litE $ stringL tableName)
                          $(litE $ stringL fieldName)
                        |])
             []
           ]
  where tableName = getColumnTableName props ci
        fieldName = Text.unpack columnName
        
makeTable :: Properties -> Name -> TableInfo -> Q [Dec]
makeTable props@Properties{tableNameModifier, includeSchema}
          dbname
          ti@TableInfo{tableName, tableSchema} =
  sequence [ sigD
             (mkName $ tableNameModifier ti)
             [t| T.Table
                 $(litT $ strTyLit $ getTableName props ti)
                 $(conT dbname)
               |]
           , valD (varP $ mkName $ tableNameModifier ti)
             (normalB [e| T.Table
                          $(if includeSchema
                            then appE (conE 'Just) $
                                 litE $ stringL $ Text.unpack tableSchema
                            else conE 'Nothing)
                          $(litE $ stringL tableString)
                        |])
             []
           ]
  where tableString = Text.unpack tableName
           
fieldClass :: Properties -> Name -> String -> Q Dec
fieldClass props dbName columnName =
  classD (pure []) (mkName $ classNameModifier props columnName)
    [ kindedTV (mkName "table") (ConT ''Symbol)
    , kindedTV (mkName "nullable") (ConT ''T.Nullable)
    , plainTV $ mkName "a"]
    [FunDep [mkName "table"] [mkName "nullable", mkName "a"]]
    [ sigD
      (mkName columnName)
      [t| T.Field
          $(varT $ mkName "table")
          $(conT dbName)
          $(varT $ mkName "nullable")
          $(varT $ mkName "a")
        |]]

fieldInstance :: Properties -> ColumnInfo -> Q Dec
fieldInstance props ci@ColumnInfo{columnName,
                                  columnNullable} =
  instanceD (pure [])
  [t| $(conT $ mkName $ classNameModifier props $ fieldNameModifier props ci)
      $(litT $ strTyLit tableName)
      $(promotedT $ if columnNullable then 'T.Nullable else 'T.NotNull)
      $(columnTHType True ci)
      |]
  [valD (varP $ mkName $ fieldNameModifier props ci)
             (normalB [e| T.Field
                          $(litE $ stringL tableName)
                          $(litE $ stringL fieldName)
                        |])
             []]
    where fieldName = Text.unpack columnName
          tableName = getColumnTableName props ci

  
insertorType :: Properties -> TableInfo -> Q Dec
insertorType props ti =
  dataD (pure [])
  (mkName typeName)
  []
  Nothing
  [recC (mkName typeName) $ map columnTypes $ tableColumns ti ]
  []
  where typeName = insertorTypeModifier props ti
        columnTypes ci =
          ( mkName $ insertorFieldModifier props ci
          , Bang NoSourceUnpackedness NoSourceStrictness
          ,
          ) <$> columnTHType False ci

insertor :: Properties -> Name -> TableInfo -> Q [Dec]
insertor props dbName ti =
  sequence [ sigD
             insertorName
             [t| T.Insertor
                 $(litT $ strTyLit $ getTableName props ti)
                 $(conT dbName)
                 $(conT insertorTypeName)
                 |]
           , valD (varP insertorName)
             (normalB $ foldr1 (\x y -> [| $(x) <> $(y) |]) $
              map insertorField $ tableColumns ti)
             []
           ]
  where 
    insertorName = mkName $ insertorNameModifier props ti
    insertorTypeName = mkName $ insertorTypeModifier props ti 
    insertorField :: ColumnInfo -> Q Exp
    insertorField ci = [e| $(sigE
                             (varE $ mkName $ insertorFieldModifier props ci)
                             [t| $(conT insertorTypeName) ->
                                 $(columnTHType False ci)  |])
                           `T.into`
                           $(varE $ mkName $
                             fieldsQualifier props <>
                             fieldNameModifier props ci) |]

makeSelector :: Properties -> Name -> TableInfo -> Q [Dec]
makeSelector props dbName ti =
  sequence [ sigD
             selectorName
             [t| T.Alias
                 $(litT $ strTyLit $ getTableName props ti)
                 $(conT dbName)
                 'T.InnerJoined
                 ->
                 T.Selector
                 $(conT insertorTypeName)
                 |]
           , funD selectorName
             [ do alias <- newName "alias"
                  clause [conP 'T.Alias [varP alias]]
                    (normalB $ tableSelector alias)
                    []
             ]]

  where selectorName = mkName $ selectorNameModifier props ti
        insertorTypeName = mkName $ insertorTypeModifier props ti
        tableSelector alias =
          case map (fieldSelector alias) $ tableColumns ti of
            [] -> [e| pure $(conE $ mkName $ insertorTypeModifier props ti) |]
            (sel1:sels) -> foldl
                           (\s1 s2 -> [e| $(s1) <*> $(s2) |])
                           [e| $(conE $ mkName $ insertorTypeModifier props ti)
                               <$> $(sel1) |]
                           sels
        fieldSelector alias ci =
          [e| $(if columnNullable ci then [e| T.selMaybe |] else [e| T.sel |])
              ($(varE alias) $(varE $ mkName $
                               fieldsQualifier props <>
                               fieldNameModifier props ci))
            |]

makeFields :: Properties -> Name -> [TableInfo] -> Q [Dec]
makeFields props dbName tis =
  liftA2 (++) classDecs $ concat <$> traverse tableDecs tis
  where
    classDecs :: Q [Dec]
    classDecs = traverse (fieldClass props dbName) $ Set.toList duplicateCols

    tableDecs :: TableInfo -> Q [Dec]
    tableDecs ti = concat <$> sequence
      [ makeTable props dbName ti
      , fmap concat $ traverse (makeField props dbName) $
        filter (not . duplicateCol . getFieldName) $ tableColumns ti
      , traverse (fieldInstance props) $ filter (duplicateCol . getFieldName) $
        tableColumns ti
      ]

    getFieldName :: ColumnInfo -> String
    getFieldName ti = fieldNameModifier props ti

    duplicateCols :: Set.Set String
    duplicateCols = Set.fromList $ map fst $ filter ((> 1) . snd) $ Map.toList $
                    Map.fromListWith (+) $
                    map (\c -> (getFieldName c, 1 :: Int)) $
                    tis >>= tableColumns
    duplicateCol :: String -> Bool
    duplicateCol c = c `Set.member` duplicateCols

makeRecords :: Properties -> [TableInfo] -> Q [Dec]
makeRecords props = traverse (insertorType props)

makeInsertors :: Properties -> Name -> [TableInfo] -> Q [Dec]
makeInsertors props dbName tis =
  concat <$> traverse (insertor props dbName) tis

makeSelectors :: Properties -> Name -> [TableInfo] -> Q [Dec]
makeSelectors props dbName tis =
  concat <$> traverse (makeSelector props dbName) tis
