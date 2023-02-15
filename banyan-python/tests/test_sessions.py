from banyan import start_session

# TODO: Check that the required environment variables are set before
# running tests: api_key, user_id, endpoint

# start_session was tested by calling start_session with a path
# to a site_packages directory. It was manually verified that
# a new Lambda function was created if not already existing and
# that it was invoked once.
