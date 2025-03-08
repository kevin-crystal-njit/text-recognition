package com.aws.rekognition;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.rekognition.model.DetectTextRequest;
import software.amazon.awssdk.services.rekognition.model.DetectTextResponse;
import software.amazon.awssdk.services.rekognition.model.Image;
import software.amazon.awssdk.services.rekognition.model.TextDetection;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;

public class SqsMessagePrinter {
    public static void main(String[] args) {
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/280014048542/car-image-indices.fifo";
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
        
        // Create a Rekognition client
        RekognitionClient rekognitionClient = RekognitionClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        try {
            // Infinite loop to keep polling for messages
            while (true) {
                ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(1)  // One message at a time
                        .waitTimeSeconds(5)
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

                        // After processing the message delete it
                        sqsClient.deleteMessage(DeleteMessageRequest.builder()
                                .queueUrl(queueUrl)
                                .receiptHandle(message.receiptHandle())
                                .build());

                    } catch (Exception e) {
                        System.err.println("Error fetching S3 metadata for key: " + messageBody);
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sqsClient.close();
        }
    }}

