package ch.noisette.doodle.services.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.stereotype.Service;

import ch.noisette.doodle.domains.Poll;
import ch.noisette.doodle.domains.Subscriber;
import ch.noisette.doodle.services.PollService;

@Service
public class DefaultPollService implements PollService {

	private static final Logger log = Logger
			.getLogger(DefaultPollService.class);

	private static final String KEYSPACE = "doodle";
	private static final String COL_FAMILIY_POLL = "poll";
	private static final String COL_FAMILIY_SUBSCRIBER = "subscriber";
	private static final String LABEL = "label";
	private static final String EMAIL = "email";
	private static final String CHOICES = "choices";

	private final Cluster cluster;
	private final Keyspace keyspace;
	private final ColumnFamilyTemplate<String, String> pollTemplate;
	private final ColumnFamilyTemplate<String, String> subscriberTemplate;

	private final ObjectMapper mapper;

	public DefaultPollService() {
		this.mapper = new ObjectMapper();
		this.mapper.configure(
				DeserializationConfig.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY,
				true);
		this.cluster = HFactory.getOrCreateCluster("clst", "localhost:9160");
		this.keyspace = HFactory.createKeyspace(KEYSPACE, this.cluster);
		this.pollTemplate = new ThriftColumnFamilyTemplate<>(this.keyspace,
				COL_FAMILIY_POLL, StringSerializer.get(),
				StringSerializer.get());
		this.subscriberTemplate = new ThriftColumnFamilyTemplate<>(
				this.keyspace, COL_FAMILIY_SUBSCRIBER, StringSerializer.get(),
				StringSerializer.get());
	}

	@Override
	public Poll addSubscriber(String pollId, Subscriber subscriber) {
		ColumnFamilyUpdater<String, String> subsUpdater = null;
		String key = pollId + subscriber.getLabel();
		subsUpdater = this.subscriberTemplate.createUpdater(key);
		subsUpdater.setString(LABEL, subscriber.getLabel());
		try {
			subsUpdater.setString(CHOICES,
					this.mapper.writeValueAsString(subscriber.getChoices()));
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.subscriberTemplate.update(subsUpdater);
		return this.getPollById(pollId);
	}

	@Override
	public Poll createPoll(Poll poll) {
		if (poll.getId() == null || poll.getId().isEmpty()) {
			poll.setId(TimeUUIDUtils.getUniqueTimeUUIDinMillis().toString());
		}
		ColumnFamilyUpdater<String, String> pollUpdater = this.pollTemplate
				.createUpdater(poll.getId());
		pollUpdater.setString(LABEL, poll.getLabel());
		try {
			pollUpdater.setString(CHOICES,
					this.mapper.writeValueAsString(poll.getChoices()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		pollUpdater.setString(EMAIL, poll.getEmail());
		this.pollTemplate.update(pollUpdater);
		if (poll.getSubscribers() != null) {
			for (Subscriber subs : poll.getSubscribers()) {
				this.addSubscriber(poll.getId(), subs);
			}
		}

		return poll;
	}

	@Override
	public void deletePoll(String pollId) {
		this.pollTemplate.deleteRow(pollId);
		for (Subscriber s : this.getSubscribersByPollId(pollId)) {
			String key = pollId + s.getLabel();
			this.subscriberTemplate.deleteRow(key);
		}
	}

	@Override
	public List<Poll> getAllPolls() {
		List<Poll> polls = new ArrayList<>();
		CqlQuery<String, String, String> query = new CqlQuery<>(this.keyspace,
				StringSerializer.get(), StringSerializer.get(),
				StringSerializer.get());
		query.setQuery("SELECT * FROM " + COL_FAMILIY_POLL);
		QueryResult<CqlRows<String, String, String>> result = query.execute();
		for (Row<String, String, String> row : result.get().getList()) {
			Poll poll = new Poll();
			poll.setId(row.getKey());
			poll.setLabel(row.getColumnSlice().getColumnByName(LABEL)
					.getValue());
			poll.setEmail(row.getColumnSlice().getColumnByName(EMAIL)
					.getValue());
			String choices = row.getColumnSlice().getColumnByName(CHOICES)
					.getValue();
			try {
				poll.setChoices(this.mapper.readValue(choices, ArrayList.class));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			poll.setSubscribers(this.getSubscribersByPollId(poll.getId()));
			polls.add(poll);
		}

		return polls;
	}

	@Override
	public Poll getPollById(String pollId) {
		Poll poll = new Poll();
		ColumnFamilyResult<String, String> res = this.pollTemplate
				.queryColumns(pollId);
		if (!res.hasResults()) {
			return null;
		}
		poll.setId(pollId);
		poll.setLabel(res.getString(LABEL));
		try {
			String choicesJSON = res.getString(CHOICES);
			ArrayList<String> choices = this.mapper.readValue(choicesJSON,
					ArrayList.class);
			poll.setChoices(choices);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		poll.setSubscribers(this.getSubscribersByPollId(pollId));

		return poll;
	}

	private List<Subscriber> getSubscribersByPollId(String pollId) {
		List<Subscriber> subscribers = new ArrayList<>();
		CqlQuery<String, String, String> query = new CqlQuery<>(this.keyspace,
				StringSerializer.get(), StringSerializer.get(),
				StringSerializer.get());
		query.setQuery("SELECT * FROM " + COL_FAMILIY_SUBSCRIBER
				+ " WHERE pollId='" + pollId + "'");
		QueryResult<CqlRows<String, String, String>> result = query.execute();
		return this.getSubscribersFromResult(result);
	}

	private List<Subscriber> getSubscribersFromResult(
			QueryResult<CqlRows<String, String, String>> result) {
		List<Subscriber> subscribers = new ArrayList<>();
		if (result.get() == null) {
			return subscribers;
		}
		for (Row<String, String, String> row : result.get().getList()) {
			Subscriber subscriber = new Subscriber();
			subscriber.setLabel(row.getColumnSlice().getColumnByName(LABEL)
					.getValue());
			String choices = row.getColumnSlice().getColumnByName(CHOICES)
					.getValue();
			try {
				subscriber.setChoices(this.mapper.readValue(choices,
						ArrayList.class));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return subscribers;
	}
}
