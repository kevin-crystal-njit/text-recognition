package com.aws.rekognition;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public class SqsMessagePrinter {
    public static void main(String[] args) {
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/280014048542/car-image-indices";
        String bucketName = "njit-cs-643";

        // Create an SQS client
        SqsClient sqsClient = SqsClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        // Create an S3 client
        S3Client s3Client = S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        try {
            // Infinite loop to keep polling for messages
            while (true) {
                ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(1)  // One message at a time
                        .waitTimeSeconds(10)
                        .build();

                ReceiveMessageResponse response = sqsClient.receiveMessage(receiveRequest);

                for (Message message : response.messages()) {
                    String messageBody = message.body();
                    System.out.println("\nMessage ID: " + message.messageId());
                    System.out.println("Body: " + messageBody);

                    // Check if this is the end-of-stream indicator
                    if ("-1".equals(messageBody)) {
                        System.out.println("Received end-of-stream marker. Stopping.");
                        return; // Exit the main method, ending the program
                    }

                    // Use the message body as the S3 key to get the object
                    try {
                        s3Client.getObject(GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(messageBody)
                            .build());
                        System.out.println("Fetched S3 object with key: " + messageBody);
                    } catch (Exception e) {
                        System.err.println("Error fetching S3 metadata for key: " + messageBody);
                        e.printStackTrace();
                    }
                }

                // Add a 2-second delay before the next iteration
                Thread.sleep(2000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sqsClient.close();
        }
    }}

