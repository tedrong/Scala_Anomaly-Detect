package com.spark.fjuted;

import java.util.Properties;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

/**
 * Created by rong on 5/9/17.
 */
public class gmail {
    // Recipient's email ID needs to be mentioned.
    String to = "";
    // Sender's email ID needs to be mentioned
    String from = "";
    String username = "";
    String password = "";

    String host = "smtp.gmail.com";

    public void setSender(String address, String password){
        this.username = address;
        this.from = address;
        this.password = password;
    }

    public void setReceiver(String address){
        this.to= address;
    }

    public void Send_Email(String subject, String body) {
        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", host);
        props.put("mail.smtp.port", "587");
        // Get the Session object.
        Session session = Session.getInstance(
                props,
                new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, password);
                    }
                }
        );

        try {
            // Create a default MimeMessage object.
            Message message = new MimeMessage(session);

            // Set From: header field of the header.
            message.setFrom(new InternetAddress(from));

            // Set To: header field of the header.
            message.setRecipients(Message.RecipientType.TO,
                    InternetAddress.parse(to));

            // Set Subject: header field
            message.setSubject(subject);

            // Now set the actual message
            message.setText(body);

            // Send message
            Transport.send(message);
            System.out.println("Sent message successfully....");
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }//Send_Email
}
