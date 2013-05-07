package ch.noisette.doodle.web.rest;

import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.View;

import ch.noisette.doodle.domains.Poll;
import ch.noisette.doodle.domains.Subscriber;
import ch.noisette.doodle.services.PollService;

/**
 * PollsController class will expose a series of RESTful endpoints
 */
@Controller
public class PollController {

	@Autowired
	private PollService pollService;

	@Autowired
	private View jsonView_i;

	private static final String DATA_FIELD = "data";
	private static final String ERROR_FIELD = "error";

	private static final Logger logger_c = Logger
			.getLogger(PollController.class);

	public static boolean isEmpty(String s_p) {
		return (null == s_p) || s_p.trim().length() == 0;
	}

	/**
	 * Updates poll with given poll id.
	 * 
	 * @param poll
	 *            the poll
	 * @return the model and view
	 */
	@RequestMapping(value = { "/rest/polls/{pollId}" }, method = { RequestMethod.PUT })
	public ModelAndView addSubscriber(@RequestBody Subscriber subscriber,
			@PathVariable("pollId") String pollId,
			HttpServletResponse httpResponse_p) {

		logger_c.debug("Add Subscriber: " + subscriber.toString());

		/* validate poll Id parameter */
		if (isEmpty(pollId) || pollId.length() < 5) {
			String sMessage = "Error updating poll - Invalid poll Id parameter";
			return this.createErrorResponse(sMessage);
		}

		Poll updatedPoll = null;

		try {
			updatedPoll = this.pollService.addSubscriber(pollId, subscriber);
		} catch (Exception e) {
			String sMessage = "Error updating poll. [%1$s]";
			return this.createErrorResponse(String.format(sMessage,
					e.toString()));
		}

		httpResponse_p.setStatus(HttpStatus.OK.value());
		return new ModelAndView(this.jsonView_i, DATA_FIELD, updatedPoll);
	}

	/**
	 * Creates a new poll.
	 * 
	 * @param poll
	 *            the poll
	 * @return the model and view
	 */
	@RequestMapping(value = { "/rest/polls/" }, method = { RequestMethod.POST })
	public ModelAndView createPoll(@RequestBody Poll poll,
			HttpServletResponse httpResponse_p, WebRequest request_p) {

		Poll createdPoll;
		logger_c.debug("Creating Poll: " + poll.toString());

		try {
			createdPoll = this.pollService.createPoll(poll);
		} catch (Exception e) {
			String sMessage = "Error creating new poll. [%1$s]";
			return this.createErrorResponse(String.format(sMessage,
					e.toString()));
		}

		/* set HTTP response code */
		httpResponse_p.setStatus(HttpStatus.CREATED.value());

		/* set location of created resource */
		httpResponse_p.setHeader("Location", request_p.getContextPath()
				+ "/rest/polls/" + poll.getId());

		/**
		 * Return the view
		 */
		return new ModelAndView(this.jsonView_i, DATA_FIELD, createdPoll);
	}

	/**
	 * Gets a poll by poll id.
	 * 
	 * @param pollId
	 *            the poll id
	 * @return the poll
	 */
	@RequestMapping(value = "/rest/polls/{pollId}", method = RequestMethod.GET)
	public ModelAndView getPoll(@PathVariable("pollId") String pollId) {
		Poll poll = null;

		/* validate poll Id parameter */
		if (isEmpty(pollId) || pollId.length() < 5) {
			String sMessage = "Error invoking getPoll - Invalid poll Id parameter";
			return this.createErrorResponse(sMessage);
		}

		try {
			poll = this.pollService.getPollById(pollId);
		} catch (Exception e) {
			String sMessage = "Error invoking getPoll. [%1$s]";
			return this.createErrorResponse(String.format(sMessage,
					e.toString()));
		}

		return new ModelAndView(this.jsonView_i, DATA_FIELD, poll);
	}

	/**
	 * Gets all polls.
	 * 
	 * @return the polls
	 */
	@RequestMapping(value = "/rest/polls/", method = RequestMethod.GET)
	public ModelAndView getPolls() {
		List<Poll> polls = null;

		try {
			polls = this.pollService.getAllPolls();
		} catch (Exception e) {
			String sMessage = "Error getting all polls. [%1$s]";
			return this.createErrorResponse(String.format(sMessage,
					e.toString()));
		}

		logger_c.debug("Returing Polls: " + polls.toString());
		return new ModelAndView(this.jsonView_i, DATA_FIELD, polls);
	}

	/**
	 * Deletes the poll with the given poll id.
	 * 
	 * @param pollId
	 *            the poll id
	 * @return the model and view
	 */
	@RequestMapping(value = "/rest/polls/{pollId}", method = RequestMethod.DELETE)
	public ModelAndView removePoll(@PathVariable("pollId") String pollId,
			HttpServletResponse httpResponse_p) {

		logger_c.debug("Deleting Poll Id: " + pollId);

		/* validate poll Id parameter */
		if (isEmpty(pollId) || pollId.length() < 5) {
			String sMessage = "Error deleting poll - Invalid poll Id parameter";
			return this.createErrorResponse(sMessage);
		}

		try {
			this.pollService.deletePoll(pollId);
		} catch (Exception e) {
			String sMessage = "Error invoking getPolls. [%1$s]";
			return this.createErrorResponse(String.format(sMessage,
					e.toString()));
		}

		httpResponse_p.setStatus(HttpStatus.OK.value());
		return new ModelAndView(this.jsonView_i, DATA_FIELD, null);
	}

	/**
	 * Injector methods.
	 * 
	 * @param view
	 *            the new json view
	 */
	public void setJsonView(View view) {
		this.jsonView_i = view;
	}

	/**
	 * Injector methods.
	 * 
	 * @param pollService_p
	 *            the new poll service
	 */
	public void setPollService(PollService pollService_p) {
		this.pollService = pollService_p;
	}

	/**
	 * Create an error REST response.
	 * 
	 * @param sMessage
	 *            the s message
	 * @return the model and view
	 */
	private ModelAndView createErrorResponse(String sMessage) {
		return new ModelAndView(this.jsonView_i, ERROR_FIELD, sMessage);
	}

}
