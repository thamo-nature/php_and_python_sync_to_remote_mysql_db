# Both the PHP and Python scripts are used to sync local mysql db to remote mysql db.
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
# PHP Limitations
<pre> It took around 10 minutes to sync around 10k records due to it's limitations in parallel processing </pre>

# Python Record time SYNC
<pre>
    1. Using Python's asyncio and aiomysql
        After creating all the tables
    2. Parallely we open a pool of mysql remote connections to all the tables
       Then for insertions we used batch process that operates in a async fashion.
       Achieved a record time for sync, around 1 lakh records.
</pre>
            --- 10.03 Total Execution Seconds ---
            --- 0.17 Total Execution Minutes ---
            
  !![Screenshot 2024-08-27 014544](https://github.com/user-attachments/assets/46046b44-0692-4779-98f2-deee109efc41)
  ![Screenshot 2024-08-27 015909](https://github.com/user-attachments/assets/4dd51495-eacc-413c-ad4c-f94c0bfe7097)
  ![Screenshot 2024-08-27 014628](https://github.com/user-attachments/assets/a0839020-b19d-4817-ab0f-ac5053042e5b)
