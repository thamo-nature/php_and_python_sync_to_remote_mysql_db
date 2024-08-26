# This PHP script is used to sync local mysql db to remote mysql db.
# Features :
<pre>
    1. It will check for the tables in the remote database,
          if the table not exists on the remote database,
          create the tables then feed the data from local.
  
    2. Subsequent calls will check the last_sync column and update data accordingly.
          if the table not exists on remote it will create it.
  
                      CREATE TABLE IF NOT EXISTS sync_status (
                            table_name VARCHAR(255) PRIMARY KEY,
                            last_sync TIMESTAMP NULL
                        )
  
   3. For very large tables, Batch Processing is used.
  
   4. No need to specify any table columns. 
         it will automatically get the structure using
         * SHOW Tables
         * DESCRIBE Tables
          
</pre>

# Requirements : 
<pre>
    1. You need to install the doctrine/dbal Package

        composer require doctrine/dbal
    
    2. All tables must have an Column that captures last_updated or modified time
              In my case I have time_stamp across all the tables.

</pre>

