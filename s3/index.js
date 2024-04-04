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
 * Note: Much of the code for these examples was generated initially by Code Whisperer and
 * cleaned up and documented by me.
 *
 */
import {
    S3Client,
    ListBucketsCommand,
    ListObjectsV2Command,
    CreateBucketCommand,
    DeleteBucketCommand,
    DeleteObjectsCommand,
    PutObjectCommand,
    GetObjectCommand,
} from "@aws-sdk/client-s3";
import { loadSharedConfigFiles } from '@aws-sdk/shared-ini-file-loader';
import pkg from '@aws-sdk/credential-providers';
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import jp from 'jsonpath';
import { readFileSync} from "fs";

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
    
    const sourceFileName = "notes.csv";
    const sourceContentType= "text/csv";
    await uploadFile(client, newBucket, sourceFileName, sourceContentType,  { "myVal": "Upload Testing"} );
    await listBucketContents(client, newBucket);
    
    const url = await createPresignedUrl(client, newBucket, sourceFileName);
    console.log(`Presigned URL is: ${url}`);
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
    // Clear out bucket contents before deleting bucket
    await clearBucketContents(s3client, bucketName);

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

/**
 * Delete all objects in the specified bucket.
 * 
 * Use the S3Client to delete all objects in the specified bucket.
 * This is typically used as a precursor to deleting the bucket itself.
 * 
 * @param {S3Client} s3client The initialized S3 client reference
 * @param {string} bucketName The name of the bucket to clear contents for
 */
async function clearBucketContents(s3client, bucketName) {
    const command = new ListObjectsV2Command({
        Bucket: bucketName,
    });

    try {
        const { Contents } = await s3client.send(command);
        // Delete all the objects obtained from the prior ListObjects command
        const deleteObjectsCommand = new DeleteObjectsCommand({
            Bucket: bucketName,
            Delete: { Objects: Contents.map((o) => ({ Key: o.Key })) }
        });
        await s3client.send(deleteObjectsCommand);

    } catch (err) {
        console.error(err);
    }
}

/**
 * Upload the specified file to the specified bucket.
 * 
 * Uses the S3 client to upload the file to the specified bucket.
 * This approach will use a local file and will read the contents and send
 * that as the body of the object being uploaded.
 * 
 * @param {S3Client} s3client The initialized S3 client reference
 * @param {string} bucketName The name of the bucket to upload to
 * @param {string} fileName The name of the source file (which will also be the key in S3)
 * @param {string} contentType the content type of the file being uploaded
 * @param {object} metadata Metadata to associate with the object
 */
async function uploadFile(s3client, bucketName, fileName, contentType, metadata) {
    const command = new PutObjectCommand({
        Bucket: bucketName,
        Key: fileName,
        Body: readFileSync(fileName),
        ContentType: contentType,
        Metadata: metadata
    });

    try {
        const { Location } =  await s3client.send(command);
        console.log(`File uploaded at: ${Location}`)
    } catch (err) {
        console.error(err);
    }
}

/**
 * Generate a presigned URL for the specified object.
 * 
 * Use the S3Client and the helper method `getSignedUrl` to produce a temporary
 * presigned URL.
 * 
 * @param {S3Client} s3client The initialized S3 client reference
 * @param {string} bucketName The name of the bucket where the object resides
 * @param {string} objectKey The key to the object to generate the URL for
 * @returns 
 */
async function createPresignedUrl(s3client, bucketName, objectKey) {
    const command = new GetObjectCommand({
        Bucket: bucketName,
        Key: objectKey
    });

    try {
        return getSignedUrl(s3client, command, { expiresIn: 3600 }  );
    } catch (err) {
        console.error(err);
    }
}


main()
    .then(() => process.exit(0))
    .catch((error) => console.log(error));