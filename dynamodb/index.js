import { readFileSync } from "fs";
import jp from "jsonpath";
import log4js from "log4js";
import {
    CreateTableCommand,
    DeleteTableCommand,
    ListTablesCommand,
    DynamoDBClient,
    waitUntilTableExists,
    ExecuteStatementCommand,
} from "@aws-sdk/client-dynamodb";
import { fromIni } from '@aws-sdk/credential-providers';
import { loadSharedConfigFiles } from '@aws-sdk/shared-ini-file-loader';
import { PutItemCommand } from '@aws-sdk/client-dynamodb';
import { UpdateItemCommand } from '@aws-sdk/client-dynamodb';
import { QueryCommand } from '@aws-sdk/client-dynamodb';

log4js.configure({
    appenders: { out: { type: "stdout" } },
    categories: { default: { appenders: ["out"], level: "debug" } },
});
const logger = log4js.getLogger();


async function main() {
    // Set up DynamoDB Client with app-user profile
    const profile = 'app-user';

    // This code reads from the config file (~/.aws/config and ~/.aws/credentials)
    const config = await loadSharedConfigFiles();
    const queryString = `$.configFile['${profile}'].region`;
    const region = jp.query(config, queryString)[0];

    // Creates an  AwsCredentialsIdentityProvider instance to use with the configuration of the client
    const credentials = fromIni({ profile: profile });
    const client = new DynamoDBClient({
        region: region,
        credentials: credentials,
        profile: profile,
    });

    const jsonFileName = "notes.json"
    const tableName = "Notes"

    console.log("Preparing to create table");
    if (! await tableExists(client, tableName)) {
        console.log(`Table '${tableName}' does not currently exist. Creating now.`);
        await createTable(client, tableName)
        console.log(`Waiting for table '${tableName}' creation completion ...`);
        await waitUntilTableExists({ client }, { TableName: tableName })
    } else {
        console.log(`Table '${tableName}' already exists. Proceeding...`);
    }

    // Now, load some data from notes.json
    const jsonData = readFileSync(jsonFileName);
    const notes = JSON.parse(jsonData);
    console.log(`Loaded ${notes.length} notes from ${jsonFileName}`);
    console.log("Inserting notes into table");
    for (const note of notes) {
        await insertNote(client, tableName, note);
    }

    const updateResponse = await updateNote(client, tableName, "student", 5);
    console.log(`Updated Note: ${JSON.stringify(updateResponse)}`);

    // Find all notes for the student having userId: 'student'
    var queryResults = await queryNotes(client, tableName, "student")
    console.log("Results of simple query");
    for (const note of queryResults) {
        console.log(`Note: ${JSON.stringify(note)}`);
    }

    // Perform a PartiQL query
    queryResults = await partiqlQuery(client, tableName, "student", 5)
    console.log("Results of PartiQL query");
    for (const note of queryResults) {
        console.log(`Note: ${JSON.stringify(note)}`);
    }

    console.log("Deleting table");
    await deleteTable(client, tableName);
}

/**
 * Check if the specified table already exists.
 * 
 * Uses a simple check to verify if the table exists.
 * It does this by listing the tables for the current region configured
 * in the client.
 * Then it returns whether or not the specified table name is in the list
 * of tables returned from listing the tables.
 * 
 * @param {DynamoDBClient} client Initialized client (including the designed region)
 * @param {string} tableName Name of the table to check
 * @returns {boolean} True if specified table already exists, False otherwise
 */
async function tableExists(client, tableName) {

    try {
        const command = new ListTablesCommand({});

        const response = await client.send(command);
        return response.TableNames.includes(tableName);
    } catch (error) {
        console.log(error);
        return false;
    }
}

/**
 * Create DynamoDB table with the specified name.
 * 
 * This will create the specified table using the defined attributes and schema
 * definition. The basic configuration is to define two attributes:
 *  - UserId: String ( partition key)
 *  - NoteId: Number (sort key)
 *
 * The schema definition is to define the primary key as a composite key
 * with the UserId and NoteId attributes.
 *
 * The provisioned throughput is set to 5 read and write units.
 *
 * The table will be created in the same region as the client.
 *
 * This function will not wait for the table to be created.
 * It will simply return immediately after the table is created.
 * 
 * @param {DynamoDBClient} client Initialized client (including the designed region)
 * @param {string} tableName Name of the table to create
 */
async function createTable(client, tableName) {
    const params = {
        TableName: tableName,
        AttributeDefinitions: [
            {
                'AttributeName': "UserId",
                'AttributeType': 'S',
            },
            {
                'AttributeName': "NoteId",
                'AttributeType': 'N',
            },
        ],
        KeySchema: [
            {
                'AttributeName': "UserId",
                'KeyType': 'HASH',
            },
            {
                'AttributeName': "NoteId",
                'KeyType': 'RANGE',
            },
        ],
        ProvisionedThroughput: {
            ReadCapacityUnits: 5,
            WriteCapacityUnits: 5,
        },
    };

    const command = new CreateTableCommand(params);
    await client.send(command);
    console.log("Table creation initiated");
}

/**
 * Inserts a note into the specified table.
 * 
 * Inserts a note object into the specified table. This function has a few assumptions
 * - The table has already been created
 * - The note has the following minimum attributes
 *   - UserId: String
 *   - NoteId: Number
 *   - Note: String
 *
 *   No other attributes will be written
 * 
 * @param {DynamoDBClient} client Initialized client (including the designed region)
 * @param {string} tableName Name of the table to insert the note into
 * @param {object} note The note to insert
 */
async function insertNote(client, tableName, note) {
    const params = {
        TableName: tableName,
        Item: {
            "UserId": { S: note.UserId },
            "NoteId": { N: note.NoteId },
            "Note": { S: note.Note },
        },
    };

    const command = new PutItemCommand(params);
    await client.send(command);
}

/**
 * Updates the note item with the specified userId and noteId.
 * 
 * Performs an update to the specified note item by adding an attribute called
 * 'Is_Incomplete' that is a string value type with expected values of 'Y' or 'N'.
 * 
 * @param {DynamoDBClient} client Initialized client (including the designed region)
 * @param {string} tableName Name of the table to update the note from
 * @param {string} userId 
 * @param {number} noteId 
 * @returns {object} The updated note item
 */
async function updateNote(client, tableName, userId, noteId) {
    const params = {
        TableName: tableName,
        Key: {
            "UserId": { S: userId },
            "NoteId": { N: noteId.toString() },
        },
        UpdateExpression: "set Is_Incomplete = :incomplete",
        ExpressionAttributeValues: {
            ":incomplete": { S: "Yes" },
        },
        ReturnValues: "ALL_NEW",
    };

    const command = new UpdateItemCommand(params);
    const response = await client.send(command);
    return response.Attributes; j
}

/**
 * Query for all notes for the specified userId.
 * 
 * Performs a simple query returning the projections of 'NoteId' and 'Note' items
 * 
 * @param {DynamoDBClient} client Initialized client (including the designed region)
 * @param {string} tableName Name of the table to query for items
 * @param {string} userId The student id to query for
 * @returns {object[]} A list (array) of matching items fetched from the table
 */
async function queryNotes(client, tableName, userId) {
    const params = {
        TableName: tableName,
        KeyConditionExpression: "UserId = :userId",
        ExpressionAttributeValues: {
            ":userId": { S: userId },
        },
        ProjectionExpression: "NoteId, Note",
    };

    const command = new QueryCommand(params);
    const response = await client.send(command);
    return response.Items;
}

/**
 * Performs a query using the PartiQL style query.
 * 
 * Performs a query using the PartiQL style query. This query will return all
 * matching items for the specified userId and noteId.
 * 
 * @param {DynamoDBClient} client Initialized client (including the designed region)
 * @param {string} tableName Name of the table to query for items
 * @param {string} userId Representing the partition key
 * @param {number} noteId Representing the sort key
 * @returns {object[]} A list (array) of matching items fetched from the table 
 */
async function partiqlQuery(client, tableName, userId, noteId) {
    const params = {
        Statement: `SELECT * FROM ${tableName} WHERE UserId = ? AND NoteId = ?`,
        Parameters: [{S: userId}, { N:noteId.toString()}]
    };

    const command = new ExecuteStatementCommand(params);
    const response = await client.send(command);
    return response.Items;
}

/**
 * Deletes the specified table.
 * 
 * @param {DynamoDBClient} client Initialized client (including the designed region)
 * @param {string} tableName Name of the table to delete
 */
async function deleteTable(client, tableName) {
    const params = {
        TableName: tableName
    };

    const command = new DeleteTableCommand(params);
    await client.send(command);
}

main()
    .then(() => process.exit(0))
    .catch((error) => console.log(error));