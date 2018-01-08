-- original procedure
-- src:https://www.codeproject.com/Articles/21068/Audit-Trail-Generator-for-Microsoft-SQL

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

IF OBJECT_ID('dbo.GenerateAuditTrail') IS NULL
BEGIN
    PRINT ('Create procedure dbo.GenerateAuditTrail');
    EXEC ('CREATE PROCEDURE dbo.GenerateAuditTrail AS SET NOCOUNT ON;');
END;
ELSE
    PRINT ('Alter procedure dbo.GenerateAuditTrail');
GO

ALTER PROCEDURE dbo.GenerateAuditTrail
    @Owner VARCHAR(128)
  , @TableName VARCHAR(128)
  , @AuditNameExtension VARCHAR(128) = '_Audit'
  , @DropAuditTable BIT = 0
  , @AuditDatabaseName VARCHAR(128) = NULL
AS
BEGIN

    -- Internal setting - *** TODO ** update to configuration table
    DECLARE @UseUTCDateTime BIT = 1;

    IF @AuditDatabaseName IS NULL
        SET @AuditDatabaseName =
    (
        SELECT DB_NAME()
    )   ;
    ELSE IF NOT EXISTS
         (
             SELECT name
             FROM sys.databases
             WHERE name = @AuditDatabaseName
         )
    BEGIN
        PRINT 'ERROR: Database''' + @AuditDatabaseName + ''' does not exist';
        RETURN;
    END;

    PRINT 'Current database : ' + @AuditDatabaseName;

    -- Check if table exists
    IF NOT EXISTS
    (
        SELECT *
        FROM dbo.sysobjects
        WHERE id = OBJECT_ID(N'[' + @Owner + '].[' + @TableName + ']')
              AND OBJECTPROPERTY(id, N'IsUserTable') = 1
    )
    BEGIN
        PRINT 'ERROR: Table does not exist';
        RETURN;
    END;

    -- Check @AuditNameExtension
    IF @AuditNameExtension IS NULL
    BEGIN
        PRINT 'ERROR: @AuditNameExtension cannot be null';
        RETURN;
    END;

    DECLARE @ExecuteInAuditDatabase NVARCHAR(MAX);
    SET @ExecuteInAuditDatabase = 'EXEC ' + @AuditDatabaseName + '..sp_executesql N''
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE [name] = ''''' + @Owner + ''''')
	   EXECUTE(N''''CREATE SCHEMA ' + @Owner + ';'''');''';
    EXEC sp_executesql @ExecuteInAuditDatabase;

    -- Drop audit table if it exists and drop should be forced
    SET @ExecuteInAuditDatabase
        = '
    IF (EXISTS(SELECT * FROM ' + @AuditDatabaseName + '.sys.sysobjects WHERE id = OBJECT_ID(N''[' + @Owner + '].[' + @TableName + @AuditNameExtension
          + ']'') AND OBJECTPROPERTY(id, N''IsUserTable'') = 1) AND ' + CAST(@DropAuditTable AS CHAR(1)) + ' = 1)
    BEGIN
	   PRINT ''Dropping audit table [' + @AuditDatabaseName + '].[' + @Owner + '].[' + @TableName + @AuditNameExtension + '];
	   DROP TABLE [' + @AuditDatabaseName + '].[' + @Owner + '].[' + @TableName + @AuditNameExtension + ']'';
    END';
    EXEC sp_executesql @ExecuteInAuditDatabase;

    -- Declare cursor to loop over columns
    DECLARE TableColumns CURSOR READ_ONLY FOR
    SELECT b.name
         , c.name AS TypeName
         , b.length
         , b.isnullable
         , b.collation
         , b.xprec
         , b.xscale
    FROM sys.sysobjects a
    INNER JOIN sys.syscolumns b
        ON a.id = b.id
    INNER JOIN sys.systypes c
        ON b.xusertype = c.xusertype
           AND c.name <> 'sysname'
    WHERE a.id = OBJECT_ID(N'[' + @Owner + '].[' + @TableName + ']')
          AND OBJECTPROPERTY(a.id, N'IsUserTable') = 1
    ORDER BY b.colid;

    OPEN TableColumns;

    -- Declare temp variable to fetch records into
    DECLARE @ColumnName VARCHAR(128);
    DECLARE @ColumnType VARCHAR(128);
    DECLARE @ColumnLength SMALLINT;
    DECLARE @ColumnNullable INT;
    DECLARE @ColumnCollation sysname;
    DECLARE @ColumnPrecision TINYINT;
    DECLARE @ColumnScale TINYINT;

    -- Declare variable to build statements
    DECLARE @CreateStatement VARCHAR(MAX);
    DECLARE @ListOfFields VARCHAR(MAX);
    SET @ListOfFields = '';

    -- Check if audit table exists
    DECLARE @IsAuditTableExistsInAuditDatabase BIT;
    SET @ExecuteInAuditDatabase
        = 'EXEC ' + @AuditDatabaseName + '..sp_executesql N''' + '
    IF EXISTS(SELECT 1 FROM sys.sysobjects WHERE id = OBJECT_ID(N''''[' + @Owner + '].[' + @TableName + @AuditNameExtension
          + ']'''') AND OBJECTPROPERTY(id, N''''IsUserTable'''') = 1) SET @IsAuditTableExistsInAuditDatabase = 1; ELSE SET @IsAuditTableExistsInAuditDatabase = 0;'', N''@IsAuditTableExistsInAuditDatabase BIT OUTPUT'', @IsAuditTableExistsInAuditDatabase OUTPUT';
    EXEC sp_executesql @ExecuteInAuditDatabase
                     , N'@IsAuditTableExistsInAuditDatabase BIT OUTPUT'
                     , @IsAuditTableExistsInAuditDatabase OUTPUT;

    IF @IsAuditTableExistsInAuditDatabase = 1
    BEGIN
        -- AuditTable exists, update needed
        PRINT 'Table already exists. Will update table schema with new fields.';

        DECLARE @NewFields VARCHAR(MAX) = ''
              , @ExistingFields VARCHAR(MAX);

        SET @ExecuteInAuditDatabase
            = 'EXEC ' + @AuditDatabaseName + '..sp_executesql N'''
              + '
	   DECLARE @ExcludeColumnNames VARCHAR(MAX) = '''',AuditId,AuditAction,AuditDate,AuditUtcDate,AuditUser,AuditApp,AuditTransactionId,''''
	   SELECT 
		  @ExistingFields = COALESCE(@ExistingFields, '''''''') + '''','''' + sc.name + '''',''''
	   FROM
		  sysobjects so 
		  INNER JOIN syscolumns sc on so.id = sc.id 
	   WHERE 
		  so.id = OBJECT_ID(N''''['   + @Owner + '].[' + @TableName + @AuditNameExtension
              + ']'''') 
		  AND OBJECTPROPERTY(so.id, N''''IsUserTable'''') = 1 
		  AND CHARINDEX('''','''' + sc.name + '''','''', @ExcludeColumnNames) = 0
	   ORDER BY 
		  sc.colId'', N''@ExistingFields VARCHAR(MAX) OUTPUT'', @ExistingFields OUTPUT;';
        EXEC sp_executesql @ExecuteInAuditDatabase
                         , N'@ExistingFields VARCHAR(MAX) OUTPUT'
                         , @ExistingFields OUTPUT;
        PRINT 'Existing fields : ' + ISNULL(@ExistingFields, '');

        FETCH NEXT FROM TableColumns
        INTO @ColumnName
           , @ColumnType
           , @ColumnLength
           , @ColumnNullable
           , @ColumnCollation
           , @ColumnPrecision
           , @ColumnScale;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF (
                   @ColumnType <> 'text'
                   AND @ColumnType <> 'ntext'
                   AND @ColumnType <> 'image'
                   AND @ColumnType <> 'timestamp'
               )
            BEGIN
                SET @ListOfFields = @ListOfFields + ',[' + @ColumnName + ']';
            END;

            IF (CHARINDEX(',' + @ColumnName + ',', @ExistingFields) = 0)
            BEGIN
                SET @NewFields = @NewFields + ',[' + @ColumnName + '] [' + @ColumnType + '] ';

                IF @ColumnType IN ( 'binary', 'char', 'varbinary', 'varchar' )
                BEGIN
                    IF (@ColumnLength = -1)
                        SET @NewFields = @NewFields + '(MAX) ';
                    ELSE
                        SET @NewFields = @NewFields + '(' + CAST(@ColumnLength AS VARCHAR(10)) + ') ';
                END;

                IF @ColumnType IN ( 'nchar', 'nvarchar' )
                BEGIN
                    IF (@ColumnLength = -1)
                        SET @NewFields = @NewFields + '(MAX) ';
                    ELSE
                        SET @NewFields = @NewFields + '(' + CAST((@ColumnLength / 2) AS VARCHAR(10)) + ') ';
                END;

                IF @ColumnType IN ( 'decimal', 'numeric' )
                    SET @NewFields = @NewFields + '(' + CAST(@ColumnPrecision AS VARCHAR(10)) + ',' + CAST(@ColumnScale AS VARCHAR(10)) + ') ';

                IF @ColumnType IN ( 'char', 'nchar', 'nvarchar', 'varchar', 'text', 'ntext' )
                    SET @NewFields = @NewFields + 'COLLATE ' + @ColumnCollation + ' ';

                -- Why put not nullable? Blocks some changes like new columns
                --IF @ColumnNullable = 0
                --SET @NewFields = @NewFields + 'NOT '	;	

                SET @NewFields = @NewFields + 'NULL';
            END;

            FETCH NEXT FROM TableColumns
            INTO @ColumnName
               , @ColumnType
               , @ColumnLength
               , @ColumnNullable
               , @ColumnCollation
               , @ColumnPrecision
               , @ColumnScale;

        END;

        IF LEN(@NewFields) > 0
        BEGIN
            SET @CreateStatement
                = 'ALTER TABLE [' + @AuditDatabaseName + '].[' + @Owner + '].[' + @TableName + @AuditNameExtension + '] ADD ' + SUBSTRING(@NewFields, 2, LEN(@NewFields));

            PRINT 'Adding new Fields to audit table [' + @AuditDatabaseName + '].[' + @Owner + '].[' + @TableName + @AuditNameExtension + ']';
            PRINT @CreateStatement;
            EXEC (@CreateStatement);
        END;
        ELSE
            PRINT 'No new fields to add to the audit table';
    END;
    ELSE
    BEGIN
        -- AuditTable does not exist, create new

        -- Start of create table
        SET @CreateStatement = 'CREATE TABLE [' + @AuditDatabaseName + '].[' + @Owner + '].[' + @TableName + @AuditNameExtension + '] (';
        SET @CreateStatement = @CreateStatement + '[AuditId] [BIGINT] IDENTITY (1, 1) NOT NULL';

        -- Add audit trail columns
        SET @CreateStatement = @CreateStatement + ',[AuditAction] [CHAR] (1) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL';
        SET @CreateStatement = @CreateStatement + ',[AuditDate] [DATETIME] NOT NULL';
        SET @CreateStatement = @CreateStatement + ',[AuditUser] [VARCHAR] (50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL';
        SET @CreateStatement = @CreateStatement + ',[AuditApp] [VARCHAR](128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL';
        SET @CreateStatement = @CreateStatement + ',[AuditTransactionId] [BIGINT] NOT NULL';

        FETCH NEXT FROM TableColumns
        INTO @ColumnName
           , @ColumnType
           , @ColumnLength
           , @ColumnNullable
           , @ColumnCollation
           , @ColumnPrecision
           , @ColumnScale;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF (
                   @ColumnType <> 'text'
                   AND @ColumnType <> 'ntext'
                   AND @ColumnType <> 'image'
                   AND @ColumnType <> 'timestamp'
               )
            BEGIN
                SET @ListOfFields = @ListOfFields + ',[' + @ColumnName + ']';

                SET @CreateStatement = @CreateStatement + ',[' + @ColumnName + '] [' + @ColumnType + '] ';

                IF @ColumnType IN ( 'binary', 'char', 'varbinary', 'varchar' )
                BEGIN
                    IF (@ColumnLength = -1)
                        SET @CreateStatement = @CreateStatement + '(MAX) ';
                    ELSE
                        SET @CreateStatement = @CreateStatement + '(' + CAST(@ColumnLength AS VARCHAR(10)) + ') ';
                END;

                IF @ColumnType IN ( 'nchar', 'nvarchar' )
                BEGIN
                    IF (@ColumnLength = -1)
                        SET @CreateStatement = @CreateStatement + '(MAX) ';
                    ELSE
                        SET @CreateStatement = @CreateStatement + '(' + CAST((@ColumnLength / 2) AS VARCHAR(10)) + ') ';
                END;

                IF @ColumnType IN ( 'decimal', 'numeric' )
                    SET @CreateStatement = @CreateStatement + '(' + CAST(@ColumnPrecision AS VARCHAR(10)) + ',' + CAST(@ColumnScale AS VARCHAR(10)) + ') ';

                IF @ColumnType IN ( 'char', 'nchar', 'nvarchar', 'varchar', 'text', 'ntext' )
                    SET @CreateStatement = @CreateStatement + 'COLLATE ' + @ColumnCollation + ' ';

                -- Why put not nullable? Blocks some changes like new columns
                --IF @ColumnNullable = 0
                --SET @CreateStatement = @CreateStatement + 'NOT '	;	

                SET @CreateStatement = @CreateStatement + 'NULL';
            END;

            FETCH NEXT FROM TableColumns
            INTO @ColumnName
               , @ColumnType
               , @ColumnLength
               , @ColumnNullable
               , @ColumnCollation
               , @ColumnPrecision
               , @ColumnScale;
        END;

        SET @CreateStatement = @CreateStatement + ')';

        -- Create audit table
        PRINT 'Creating audit table [' + @AuditDatabaseName + '].[' + @Owner + '].[' + @TableName + @AuditNameExtension + ']';
        PRINT @CreateStatement;
        EXEC (@CreateStatement);

        -- Set primary key and default values
        SET @CreateStatement = 'ALTER TABLE [' + @AuditDatabaseName + '].[' + @Owner + '].[' + @TableName + @AuditNameExtension + '] ADD ';
        SET @CreateStatement = @CreateStatement + 'CONSTRAINT [DF_' + @TableName + @AuditNameExtension + '_AuditDate] DEFAULT (';
        SET @CreateStatement = @CreateStatement + CASE
                                                      WHEN @UseUTCDateTime = 0 THEN
                                                          'GETDATE()'
                                                      ELSE
                                                          'GetUTCDate()'
                                                  END + ') FOR [AuditDate]';
        SET @CreateStatement = @CreateStatement + ',CONSTRAINT [DF_' + @TableName + @AuditNameExtension + '_AuditUser] DEFAULT (SUSER_SNAME()) FOR [AuditUser]';
        SET @CreateStatement = @CreateStatement + ',CONSTRAINT [PK_' + @TableName + @AuditNameExtension + '] PRIMARY KEY  CLUSTERED ([AuditId])  ON [PRIMARY]';
        SET @CreateStatement
            = @CreateStatement + ',CONSTRAINT [DF_' + @TableName + @AuditNameExtension + '_AuditApp]  DEFAULT (''App=('' + RTRIM(ISNULL(APP_NAME(),'''')) + '') '') for [AuditApp]';
        SET @CreateStatement = @CreateStatement + ',CONSTRAINT [DF_' + @TableName + @AuditNameExtension + '_AuditTransactionId] DEFAULT (0) FOR [AuditTransactionId]';

        PRINT 'Setting primary key and default values';
        PRINT @CreateStatement;
        EXEC (@CreateStatement);

    END;

    CLOSE TableColumns;
    DEALLOCATE TableColumns;

    /* Drop Triggers, if they exist */
    PRINT 'Dropping triggers';
    IF EXISTS
    (
        SELECT *
        FROM dbo.sysobjects
        WHERE id = OBJECT_ID(N'[' + @Owner + '].[TR_Audit_' + @TableName + '_Insert]')
              AND OBJECTPROPERTY(id, N'IsTrigger') = 1
    )
        EXEC ('DROP TRIGGER [' + @Owner + '].[TR_Audit_' + @TableName + '_Insert]');

    IF EXISTS
    (
        SELECT *
        FROM dbo.sysobjects
        WHERE id = OBJECT_ID(N'[' + @Owner + '].[TR_Audit_' + @TableName + '_Update]')
              AND OBJECTPROPERTY(id, N'IsTrigger') = 1
    )
        EXEC ('DROP TRIGGER [' + @Owner + '].[TR_Audit_' + @TableName + '_Update]');

    IF EXISTS
    (
        SELECT *
        FROM dbo.sysobjects
        WHERE id = OBJECT_ID(N'[' + @Owner + '].[TR_Audit_' + @TableName + '_Delete]')
              AND OBJECTPROPERTY(id, N'IsTrigger') = 1
    )
        EXEC ('DROP TRIGGER [' + @Owner + '].[TR_Audit_' + @TableName + '_Delete]');

    /* Create triggers */
    PRINT 'Creating triggers';
    EXEC ('CREATE TRIGGER TR_Audit_' + @TableName + '_Insert ON ' + @Owner + '.' + @TableName + ' FOR INSERT AS BEGIN /*DECLARE @TransactionId BIGINT; SELECT @TransactionId = transaction_id FROM sys.dm_tran_current_transaction;*/ INSERT INTO [' + @AuditDatabaseName + '].[' + @Owner + '].[' + @TableName + @AuditNameExtension + '](AuditAction/*, AuditTransactionId*/' + @ListOfFields + ') SELECT ''I''/*, @TransactionId*/' + @ListOfFields + ' FROM Inserted; END');
    EXEC ('CREATE TRIGGER TR_Audit_' + @TableName + '_Update ON ' + @Owner + '.' + @TableName + ' FOR UPDATE AS BEGIN /*DECLARE @TransactionId BIGINT; SELECT @TransactionId = transaction_id FROM sys.dm_tran_current_transaction;*/ INSERT INTO [' + @AuditDatabaseName + '].[' + @Owner + '].[' + @TableName + @AuditNameExtension + '](AuditAction/*, AuditTransactionId*/' + @ListOfFields + ') SELECT ''U''/*, @TransactionId*/' + @ListOfFields + ' FROM Inserted; END');
    EXEC ('CREATE TRIGGER TR_Audit_' + @TableName + '_Delete ON ' + @Owner + '.' + @TableName + ' FOR DELETE AS BEGIN /*DECLARE @TransactionId BIGINT; SELECT @TransactionId = transaction_id FROM sys.dm_tran_current_transaction;*/ INSERT INTO [' + @AuditDatabaseName + '].[' + @Owner + '].[' + @TableName + @AuditNameExtension + '](AuditAction/*, AuditTransactionId*/' + @ListOfFields + ') SELECT ''D''/*, @TransactionId*/' + @ListOfFields + ' FROM Deleted; END');

END;
GO
