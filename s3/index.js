/**
 * Should be using Node version 16+ for V3 version of AWS SDK
 * These examples demonstrate the various use cases for managing S3 buckets and
 * objects. 
 * The S3 client primarily uses the command pattern to perform the various tasks, 
 * which means that the client is responsible for creating the command and then
 * sending it to the service.
 * The requester is then responsible for handling the response, which will vary depending
 * on the request being sent.
 *
 */
import {
    S3Client,
    ListBucketsCommand,
    ListObjectsV2Command,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutObjectCommand,
    DeleteObjectCommand,
    paginateListObjectsV2,
    GetObjectCommand,
} from "@aws-sdk/client-s3";
import { loadSharedConfigFiles } from '@aws-sdk/shared-ini-file-loader';
import pkg from '@aws-sdk/credential-providers';
import jp from 'jsonpath';

const { fromIni } = pkg;

/**
 * Main entry point for the application.
 * 
 * This function will exercise the core programmatic capabilities of the Node.js SDK for
 * creating and managing S3 buckets and their contents.
 */
async function main() {
    // Load configuration file for a specified profile
    const profile = 'app-user';

    // This code reads from the config file (~/.aws/config and ~/.aws/credentials)
    const config = await loadSharedConfigFiles();
    const queryString = `$.configFile['${profile}'].region`;
    const region = jp.query(config, queryString)[0];

    // Creates an  AwsCredentialsIdentityProvider instance to use with the configuration of the client
    const credentials = fromIni({ profile: profile });
    const client = new S3Client({
        region: region,
        profile: profile,
        credentials: credentials
    });

    // Call S3 to list the buckets
    await listBuckets(client);
    // This works because the specified bucket is found in the region configured in the client
    const newBucket = "mark-test-123459876123";
    await listBucketContents(client, "home.dev2cloud.link");
    await createBucket(client, newBucket);
    await listBuckets(client);
    await deleteBucket(client, newBucket);

}

/**
 * Lists the buckets for the associated AWS account.
 * 
 * Uses the AWS S3Client to send a ListBucketCommand.
 * Note that as of V3, the NodeJS SDK natively uses the
 * Promise API for all asynchronous calls. This means
 * that the call to send() returns a Promise and user must use 'await' in front
 * if expecting the actual returned object.
 *
 * @param {S3Client} s3client 
 */
async function listBuckets(s3client) {
    const command = new ListBucketsCommand({});

    try {
        // Note additional data can be returned, such as the Owner.
        //const { Owner, Buckets } = await s3client.send(command);
        const { Buckets } = await s3client.send(command);
        //console.log(
        //    `${Owner.DisplayName} owns ${Buckets.length} bucket${Buckets.length === 1 ? "" : "s"
        //    }:`,
        //);
        console.log("\nList of buckets:");
        console.log(`${Buckets.map((b) => ` • ${b.Name}`).join("\n")}`);
    } catch (err) {
        console.error(err);
    }

}

/**
 * List the contents of the specified bucket.
 * 
 * Uses the S3Client to list the contents of the bucket specified.
 * 
 * @param {S3Client} s3client Reference to S3Client
 * @param {string} bucketName Name of bucket to list contents of
 */
async function listBucketContents(s3client, bucketName) {
    const command = new ListObjectsV2Command({
        Bucket: bucketName,
    });

    try {
        const { Contents } = await s3client.send(command);
        // Log the object keys on individual output lines
        console.log(`\nContents of bucket: ${bucketName}:`);
        console.log(
            `${Contents.map((o) => ` • ${o.Key}`).join("\n")}`
        );

    } catch (err) {
        console.error(err);
    }
}

/**
 * Create the specified bucket.
 * 
 * Use the S3Client to create the specified bucket using the default region associated
 * when the S3Client was created.
 * 
 * @param {S3Client} s3client The initialized S3 client reference
 * @param {string} bucketName The name of the bucket to create
 */
async function createBucket(s3client, bucketName) {
    const command = new CreateBucketCommand({
        Bucket: bucketName,
    });
    console.log("\n");
    console.log(`Creating bucket: ${bucketName}`);

    try {
        const { Location } =  await s3client.send(command);
        console.log(`Bucket created at: ${Location}`)
    } catch (err) {
        if (err.name === 'BucketAlreadyOwnedByYou' || err.name === 'BucketAlreadyExists') {
            console.log('Bucket already exists');
        }
        else {
            console.error(err);
        }
    }
}

/**
 * Delete the specified bucket.
 * 
 * Use the S3Client to delete the specified bucket.
 * 
 * @param {S3Client} s3client The initialized S3 client reference
 * @param {string} bucketName The name of the bucket to create
 */
async function deleteBucket(s3client, bucketName) {
    const command = new DeleteBucketCommand({
        Bucket: bucketName,
    });
    console.log("\n");
    console.log(`Deleting bucket: ${bucketName}`);

    try {
        await s3client.send(command);
        console.log(`Bucket deleted`)
    } catch (err) {
        console.error(err);
    }
}

main()
    .then(() => process.exit(0))
    .catch((error) => console.log(error));