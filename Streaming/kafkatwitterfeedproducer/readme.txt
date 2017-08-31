-->configuration required:

	twitterConsumerKey = UKcLjfDczeWQ65ZF60X95TSxy
	twitterConsumerSecret = sK94arVjqmgsJUY13Jz3AlNdluiVLEfD1uQjWrizxLCVpuKptU
	twitterAccessToken = 3359356825-KPTi1HhFCVuy8omW8bWgHWSUNC6oIuOPZOrFfiI
	twitterAccessTokenSecret = zWAMJCsCzYD0GPr8xcwRyVmHIouBF3yF87YSKvOJcs1de

	twitterTopic = bigdata
	KafkaTopic = bigdata

	# kafka producer config details 
	metadataBroakerList = ec2-54-174-149-226.compute-1.amazonaws.com:6667
	serializerCalss = kafka.serializer.StringEncoder
	producerType = async
	ackRequired = 1

	httpProxyHost = 10.6.13.11
	httpProxyPort = 8080
	
--> command 
	--
	/usr/hdp/2.2.0.0-2041/kafka/bin

	-- list all Kafka topic 
	./kafka-topics.sh --list --zookeeper 54.174.149.226:2181

	-- command for creating new topic
		./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic #bigdata
		
		Exception in thread "main" joptsimple.OptionMissingRequiredArgumentException: Option ['topic'] requires an argument
        at joptsimple.RequiredArgumentOptionSpec.detectOptionArgument(RequiredArgumentOptionSpec.java:49)
		
		"looks kafka doesn't allow a topic starting with # "
	
		./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic bigdata
		Created topic "bigdata".

		trying to cretae an existing topic
		./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic bigdata
		Error while executing topic command Topic "bigdata" already exists.
		kafka.common.TopicExistsException: Topic "bigdata" already exists.
		
--> console consumer which is very useful for verifying produced data at kafka broker
	./kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic bigdata
	
