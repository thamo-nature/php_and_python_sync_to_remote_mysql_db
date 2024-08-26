<?php

ini_set('max_execution_time', 0); // Unlimited execution time
ini_set('memory_limit', '1024M'); // Increase memory limit if needed

require 'vendor/autoload.php';

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Exception;

// Local Database Connection
$localParams = [
    'dbname' => 'local_db_name',
    'user' => 'root',
    'password' => '',
    'host' => 'localhost',
    'driver' => 'pdo_mysql',
];

// Remote Database Connection
$remoteParams = [
    'dbname' => 'remote_db_name',
    'user' => 'remote_user',
    'password' => '******',
    'host' => 'IP_or_Host_name',
    'driver' => 'pdo_mysql',
];

// Batching parameters
$batchSize = 1000; // Number of rows to process per batch

try {
    ini_set('max_execution_time', 0); // Unlimited execution time
    ini_set('memory_limit', '1024M'); // Increase memory limit if needed

    // Establish connections to both local and remote databases
    $localConn = DriverManager::getConnection($localParams);
    $remoteConn = DriverManager::getConnection($remoteParams);

    // Create sync_status table if it doesn't exist on the remote database
    $remoteConn->executeStatement("
        CREATE TABLE IF NOT EXISTS sync_status (
            table_name VARCHAR(255) PRIMARY KEY,
            last_sync TIMESTAMP NULL
        )
    ");

    // Fetch list of tables from the local database
    $tables = $localConn->fetchFirstColumn('SHOW TABLES');

    foreach ($tables as $table) {
        echo "Syncing table: $table\n";

        // Check if the table exists on the remote database; if not, create it
        $remoteTableExists = $remoteConn->fetchOne("SHOW TABLES LIKE ?", [$table]);
        if (!$remoteTableExists) {
            echo "Table $table does not exist on remote. Creating table.\n";

            // Get the create table statement from the local database
            $createTableResult = $localConn->fetchAssociative("SHOW CREATE TABLE $table");
            $createTableSQL = $createTableResult['Create Table'];

            // Execute the create table statement on the remote database
            $remoteConn->executeStatement($createTableSQL);
        }

        // Describe the table to dynamically get columns
        $columns = $localConn->fetchAllAssociative("DESCRIBE $table");

        $columnNames = [];
        $placeholders = [];
        $updateColumns = [];

        foreach ($columns as $column) {
            $columnNames[] = $column['Field'];
            $placeholders[] = '?';
            if ($column['Field'] !== 'time_stamp') {
                $updateColumns[] = "{$column['Field']} = VALUES({$column['Field']})";
            }
        }

        $columnsList = implode(', ', $columnNames);
        $placeholdersList = implode(', ', $placeholders);
        $updateList = implode(', ', $updateColumns);

        // Check if this is the first sync for this table
        $lastSync = $remoteConn->fetchOne("SELECT last_sync FROM sync_status WHERE table_name = ?", [$table]);

        if ($lastSync === false) {
            // This is the first sync, so we'll do a full sync
            echo "Performing full sync for table: $table\n";

            $rowCount = $localConn->fetchOne("SELECT COUNT(*) FROM $table");
            for ($offset = 0; $offset < $rowCount; $offset += $batchSize) {
                $data = $localConn->fetchAllAssociative("SELECT * FROM $table LIMIT $batchSize OFFSET $offset");

                // Start a transaction for this batch
                $remoteConn->beginTransaction();

                try {
                    // Insert each row into the remote table
                    foreach ($data as $row) {
                        $values = array_values($row);
                        $remoteConn->executeStatement(
                            "INSERT INTO $table ($columnsList) VALUES ($placeholdersList) 
                            ON DUPLICATE KEY UPDATE $updateList",
                            $values
                        );
                    }

                    // Commit the transaction
                    $remoteConn->commit();

                } catch (Exception $e) {
                    // Rollback the transaction in case of an error
                    $remoteConn->rollBack();
                    echo "Failed to sync table $table: " . $e->getMessage() . "\n";
                }
            }

            // Update the sync status
            $maxTimeStamp = $localConn->fetchOne("SELECT MAX(time_stamp) FROM $table");
            $remoteConn->executeStatement(
                "INSERT INTO sync_status (table_name, last_sync) VALUES (?, ?) ON DUPLICATE KEY UPDATE last_sync = ?",
                [$table, $maxTimeStamp, $maxTimeStamp]
            );

            echo "Full sync completed for table: $table\n";

        } else {
            // Perform incremental sync based on the last sync timestamp
            echo "Performing incremental sync for table: $table\n";

            $rowCount = $localConn->fetchOne("SELECT COUNT(*) FROM $table WHERE time_stamp > ?", [$lastSync]);
            for ($offset = 0; $offset < $rowCount; $offset += $batchSize) {
                $data = $localConn->fetchAllAssociative("SELECT * FROM $table WHERE time_stamp > ? LIMIT $batchSize OFFSET $offset", [$lastSync]);

                // Start a transaction for this batch
                $remoteConn->beginTransaction();

                try {
                    foreach ($data as $row) {
                        $values = array_values($row);
                        $remoteConn->executeStatement(
                            "INSERT INTO $table ($columnsList) VALUES ($placeholdersList) 
                            ON DUPLICATE KEY UPDATE $updateList",
                            $values
                        );
                    }

                    // Commit the transaction
                    $remoteConn->commit();

                } catch (Exception $e) {
                    // Rollback the transaction in case of an error
                    $remoteConn->rollBack();
                    echo "Failed to sync table $table: " . $e->getMessage() . "\n";
                }
            }

            // Update the sync status
            $maxTimeStamp = $localConn->fetchOne("SELECT MAX(time_stamp) FROM $table WHERE time_stamp > ?", [$lastSync]);
            $remoteConn->executeStatement(
                "UPDATE sync_status SET last_sync = ? WHERE table_name = ?",
                [$maxTimeStamp, $table]
            );

            echo "Incremental sync completed for table: $table\n";
        }
    }

    echo "All tables synced successfully!\n";

} catch (Exception $e) {
    echo "Connection failed: " . $e->getMessage();
}

?>
