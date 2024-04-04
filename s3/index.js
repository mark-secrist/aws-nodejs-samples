/**
 * Should be using Node version 16+ for V3 version of AWS SDK
 */
import {
    S3Client,
    ListBucketsCommand,
    ListObjectsV2Command,
    PutObjectCommand,
    CreateBucketCommand,
    DeleteObjectCommand,
    DeleteBucketCommand,
    paginateListObjectsV2,
    GetObjectCommand,
} from "@aws-sdk/client-s3";
import { loadSharedConfigFiles } from '@aws-sdk/shared-ini-file-loader';
import pkg from '@aws-sdk/credential-providers';
import jp  from 'jsonpath';

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
    const credentials =  fromIni({ profile: profile });
    const client = new S3Client({
        region: region,
        profile: profile,
        credentials: credentials
    });

    // Call S3 to list the buckets
    await listBuckets(client);
    // This works because the specified bucket is found in the region configured in the client
    await listBucketContents(client, "home.dev2cloud.link");

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

main()
    .then(() => process.exit(0))
    .catch((error) => console.log(error));