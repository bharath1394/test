const AWS = require('aws-sdk');
const csv = require('csv-parser');
const { S3_BUCKET, CSV_KEY, SNS_TOPIC_ARN, THRESHOLD_DAYS = 90 } = process.env;

// Initialize AWS SDK clients
const s3 = new AWS.S3();
const sns = new AWS.SNS();

// Lambda Handler
exports.lambdaHandler = async (event, context) => {
  try {
    const certificates = await fetchCertificatesFromS3();
    const expiringCertificates = checkExpiringCertificates(certificates);

    if (expiringCertificates.length > 0) {
      const message = expiringCertificates.join('\n');
      await sendNotification(message);
      console.log('Notification sent successfully.');
    } else {
      console.log('No certificates expiring within the threshold.');
    }

    return {
      statusCode: 200,
      body: JSON.stringify('Expiry check completed successfully.'),
    };
  } catch (error) {
    console.error('Error:', error);
    return {
      statusCode: 500,
      body: JSON.stringify(`An error occurred: ${error.message}`),
    };
  }
};

// Fetch certificates from S3
async function fetchCertificatesFromS3() {
  const s3Params = { Bucket: S3_BUCKET, Key: CSV_KEY };
  const certificates = [];

  return new Promise((resolve, reject) => {
    const stream = s3.getObject(s3Params).createReadStream();
    stream
      .pipe(csv())
      .on('data', (data) => certificates.push(data))
      .on('end', () => resolve(certificates))
      .on('error', (error) => reject(error));
  });
}

// Check which certificates are expiring within the threshold
function checkExpiringCertificates(certificates) {
  const today = new Date();

  return certificates
    .filter(({ 'Valid to': validTo }) => {
      const expiryDate = new Date(validTo);
      const daysToExpiry = Math.ceil((expiryDate - today) / (1000 * 60 * 60 * 24));
      return daysToExpiry <= THRESHOLD_DAYS;
    })
    .map(({ Title, Env, 'App Code': appCode, 'App Name': appName, 'Valid to': validTo }) => {
      return `Certificate '${Title}' for application '${appName}' (${appCode}) in environment '${Env}' is expiring in ${validTo}.`;
    });
}

// Send notification using SNS
async function sendNotification(message) {
  const params = {
    TopicArn: SNS_TOPIC_ARN,
    Message: message,
    Subject: 'Certificate Expiry Alert',
  };
  return sns.publish(params).promise();
}
