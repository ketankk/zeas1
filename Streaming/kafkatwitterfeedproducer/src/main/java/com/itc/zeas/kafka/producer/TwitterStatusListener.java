package com.itc.zeas.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.User;

import com.itc.zeas.kafka.producer.constant.Constants;

/**
 * status listener class its onStatus function will get called once a twitter
 * feed comes
 * 
 * @author 19217
 * 
 */
public class TwitterStatusListener implements StatusListener {

	Producer<String, String> producer;

	public TwitterStatusListener(Producer<String, String> producer) {
		super();
		this.producer = producer;
	}

	@Override
	public void onException(Exception ex) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onStatus(Status status) {
		User user = status.getUser();
		// gets Username
		String username = status.getUser().getScreenName();
		// System.out.println("username: " + username);
		String profileLocation = user.getLocation();
		// System.out.println("profileLocation: " + profileLocation);
		long tweetId = status.getId();
		// System.out.println("tweetId: " + tweetId);
		String content = status.getText();
		// System.out.println("content: " + content + "\n");
		// String twitterFeedJson = TwitterObjectFactory.getRawJSON(status);
		String tweetContent = username + "," + profileLocation + "," + tweetId
				+ "," + content;
		System.out.println("tweetContent: " + tweetContent);
		publishMessage(tweetContent);
	}

	private void publishMessage(String tweetContent) {
		KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>(
				Constants.getKafkaTopic(), tweetContent);// "india"
		producer.send(keyedMessage);
	}

	@Override
	public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onScrubGeo(long userId, long upToStatusId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onStallWarning(StallWarning warning) {
		// TODO Auto-generated method stub

	}

}
