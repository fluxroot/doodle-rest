package ch.noisette.doodle.services.impl.dummy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.ObjectMapper;

import ch.noisette.doodle.domains.Poll;
import ch.noisette.doodle.domains.Subscriber;
import ch.noisette.doodle.services.PollService;

//@Service
public class DummyPollServiceImpl implements PollService {

	private static final Logger logger_c = Logger
			.getLogger(DummyPollServiceImpl.class);
	private final Cluster cluster;
	private JsonSerializer<String> ser;

	private static final String POOL = "pool";
	private static final String KEYSPACE = "doodle";
	private static final String COL_FAMILIY = "poll";
	private static final String COL_FAMILIY_SUBSCRIBER = "subscriber";

	public static void main(String[] args) throws JsonGenerationException,
			JsonMappingException, IOException {
		DummyPollServiceImpl s = new DummyPollServiceImpl();
		s.execute();
	}

	public DummyPollServiceImpl() {
		this.cluster = HFactory.getOrCreateCluster("clst", "localhost:9160");
	}

	@Override
	public Poll addSubscriber(String pollId, Subscriber subscriber) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Poll createPoll(Poll poll) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deletePoll(String pollId) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<Poll> getAllPolls() {
		// TODO Auto-generated method stub
		return Collections.<Poll> emptyList();
	}

	@Override
	public Poll getPollById(String pollId) {
		// TODO Auto-generated method stub
		return null;
	}

	private void execute() throws JsonGenerationException,
			JsonMappingException, IOException {
		Keyspace ksp = HFactory.createKeyspace(KEYSPACE, this.cluster);
		ColumnFamilyTemplate<String, String> template = new ThriftColumnFamilyTemplate<>(
				ksp, COL_FAMILIY, StringSerializer.get(),
				StringSerializer.get());
		ThriftColumnFamilyTemplate<String, String> subscriberTemplate = new ThriftColumnFamilyTemplate<>(
				ksp, COL_FAMILIY_SUBSCRIBER, StringSerializer.get(),
				StringSerializer.get());

		String rowKey = "1234";
		Poll poll = new Poll();
		poll.setLabel("Afterwork");
		String[] ch = { "Monday", "Tuesday", "Friday" };
		poll.setChoices(Arrays.asList(ch));
		poll.setEmail("foo@bar.baz");

		Subscriber subs1 = new Subscriber();
		subs1.setChoices(Arrays.asList(ch));
		subs1.setLabel("subs1");

		Subscriber subs2 = new Subscriber();
		subs2.setChoices(Arrays.asList(ch));
		subs2.setLabel("subs2");

		List<Subscriber> subs = new ArrayList<>();
		subs.add(subs1);
		subs.add(subs2);

		poll.setSubscribers(subs);

		String pollId = "abc1234";

		ColumnFamilyUpdater<String, String> updater = template
				.createUpdater(pollId);
		updater.setString("label", poll.getLabel());
		updater.setString("choices",
				new ObjectMapper().writeValueAsString(poll.getChoices()));
		updater.setString("email", poll.getEmail());
		template.update(updater);

		ColumnFamilyResult<String, String> result = template
				.queryColumns(pollId);
		for (String name : result.getColumnNames()) {
			System.out.println(result.getString(name));
		}

		ArrayList<String> choices = new ObjectMapper().readValue(
				result.getString("choices"), ArrayList.class);
		for (String string : choices) {
			System.out.println(string);
		}

		for (Subscriber sub : subs) {
			ColumnFamilyUpdater<String, String> subsUpdater = null;
			String key = pollId + sub.getLabel();
			subsUpdater = subscriberTemplate.createUpdater(key);
			subsUpdater.setString("label", sub.getLabel());
			subsUpdater.setString("pollId", pollId);
			try {
				subsUpdater
						.setString("choices", new ObjectMapper()
								.writeValueAsString(sub.getChoices()));
			} catch (IOException e) {
				e.printStackTrace();
			}
			subscriberTemplate.update(subsUpdater);
		}

		CqlQuery<String, String, String> query = new CqlQuery<>(ksp,
				StringSerializer.get(), StringSerializer.get(),
				StringSerializer.get());
		query.setQuery("SELECT * FROM " + COL_FAMILIY_SUBSCRIBER
				+ " WHERE pollId='abc1234'");
		QueryResult<CqlRows<String, String, String>> res = query.execute();
		System.out.println(res.get());
		for (Row<String, String, String> row : res.get().getList()) {
			System.out.println(row.getKey()
					+ " "
					+ row.getColumnSlice().getColumnByName("label").getValue()
					+ " "
					+ row.getColumnSlice().getColumnByName("choices")
							.getValue());
		}

	}

}
