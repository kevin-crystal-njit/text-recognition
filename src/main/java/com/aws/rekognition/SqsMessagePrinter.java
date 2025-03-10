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
import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.rekognition.model.DetectTextRequest;
import software.amazon.awssdk.services.rekognition.model.DetectTextResponse;
import software.amazon.awssdk.services.rekognition.model.Image;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;
import java.nio.charset.StandardCharsets;



public class SqsMessagePrinter {
    public static void main(String[] args) {
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/280014048542/car-image-indices.fifo";
        String bucketName = "njit-cs-643";
        String destinationBucket = "kevins-project-1-output-bucket";
        String s3FileKey = "detected_texts.txt";
        StringBuilder allDetectedText = new StringBuilder();

        SqsClient sqsClient = SqsClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        S3Client s3Client = S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        RekognitionClient rekognitionClient = RekognitionClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        try {
            // Infinite loop to keep polling for messages till -1 encountered
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
                        // Ensure the -1 message is also deleted
                        sqsClient.deleteMessage(DeleteMessageRequest.builder()
                                .queueUrl(queueUrl)
                                .receiptHandle(message.receiptHandle())
                                .build());

                        System.out.println("Received end-of-stream marker. Uploading collected text to S3...");

                        // Upload entire collected text to S3
                        try {
                            s3Client.putObject(PutObjectRequest.builder()
                                    .bucket(destinationBucket)
                                    .key(s3FileKey)
                                    .build(),
                                    RequestBody.fromString(allDetectedText.toString(), StandardCharsets.UTF_8));
                            System.out.println("Upload successful!");
                        } catch (Exception e) {
                            System.err.println("Error uploading file to S3.");
                            e.printStackTrace();
                        }


                        return; // Exit the main method, ending the program
                    }

                    // Analyze the object for text using Rekognition
                    try {
                        DetectTextRequest textRequest = DetectTextRequest.builder()
                            .image(Image.builder()
                                .s3Object(software.amazon.awssdk.services.rekognition.model.S3Object.builder()
                                    .bucket(bucketName)
                                    .name(messageBody)
                                    .build())
                                .build())
                            .build();

                        DetectTextResponse textResponse = rekognitionClient.detectText(textRequest);
                        if (!textResponse.textDetections().isEmpty()) {

                            StringBuilder detectedTexts = new StringBuilder();
                            for (TextDetection detection : textResponse.textDetections()) {
                                detectedTexts.append(detection.detectedText()).append(", ");
                            }
                            System.out.println("Detected text: " + detectedTexts.toString());


                            // Store collected text with index in memory
                            allDetectedText.append(messageBody).append(": ").append(detectedTexts).append("\n");
                        }

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
	    rekognitionClient.close();
      	    s3Client.close();
            sqsClient.close();
        }
    }}

