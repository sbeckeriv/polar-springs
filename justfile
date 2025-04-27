# Justfile for expanding tests/request_logs.json to expanded_big_test.json

expand-logs:
	cp tests/request_logs.json expanded_big_test.json
	while [ $(stat -c%s expanded_big_test.json 2>/dev/null || stat -f%z expanded_big_test.json) -lt 1073741824 ]; do \
		cat tests/request_logs.json >> expanded_big_test.json; \
	done
expand-logs-2gb:
	cp tests/request_logs.json expanded_big_test.json
	while [ $(stat -c%s expanded_big_test.json 2>/dev/null || stat -f%z expanded_big_test.json) -lt 2173741824 ]; do \
		cat tests/request_logs.json >> expanded_big_test.json; \
	done

expand-logs-10gb:
	cp tests/request_logs.json expanded_big_test.json
	while [ $(stat -c%s expanded_big_test.json 2>/dev/null || stat -f%z expanded_big_test.json) -lt 10173741824 ]; do \
		cat tests/request_logs.json >> expanded_big_test.json; \
	done

expand-logs-50gb:
	cp tests/request_logs.json expanded_big_test.json
	while [ $(stat -c%s expanded_big_test.json 2>/dev/null || stat -f%z expanded_big_test.json) -lt 50173741824 ]; do \
		cat tests/request_logs.json >> expanded_big_test.json; \
	done

expand-logs-100gb:
	cp tests/request_logs.json expanded_big_test.json
	while [ $(stat -c%s expanded_big_test.json 2>/dev/null || stat -f%z expanded_big_test.json) -lt 100173741824 ]; do \
		cat tests/request_logs.json >> expanded_big_test.json; \
	done