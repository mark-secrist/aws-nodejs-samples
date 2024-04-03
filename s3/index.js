var AWS = require('aws-sdk');
// Set the region
AWS.config.update({ region: 'us-west-2' });

async function main() {
    // Create S3 service object
    s3 = new AWS.S3({ apiVersion: '2006-03-01' });
    // Call S3 to list the buckets
    await listBuckets(s3);

}

async function listBuckets(s3client) {
    try {
        buckets = await s3client.listBuckets().promise();
        for (var i = 0; i < buckets.Buckets.length; i++) {
            console.log(buckets.Buckets[i].Name);
        }

    } catch (err) {
        console.log("Error", err);
    }
}

main()
   .then(() => process.exit(0))
   .catch((error) => console.log(error));