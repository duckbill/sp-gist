CREATE EXTENSION spgist;

CREATE TABLE test_text(t text);

\copy test_text from 'data/text.data'

CREATE INDEX ttidx ON test_text USING spgist (t);

SET enable_seqscan=off;

EXPLAIN (COSTS OFF)
SELECT * FROM test_text WHERE t = 'http://0-2000webhosting.co.uk/email-configuration.htm';

SELECT * FROM test_text WHERE t = 'http://0-2000webhosting.co.uk/email-configuration.htm';

SELECT * FROM test_text WHERE t = 'http://www.data-wales.co.uk/lamb.htm';

SELECT * FROM test_text WHERE t = 'http://comwebhosting.co.uk/plan-order.asp?plan=Windows_E_Biz';

SELECT * FROM test_text WHERE t = 'http://abcde.co.uk/betterhearingservices/about_us.html';

SELECT * FROM test_text WHERE t = 'http://www.airportparkingexpress.co.uk/belfast.htm';

SELECT * FROM test_text WHERE t = 'http://www.skiing-holidays-austria.co.uk/16/panorama-ski-austria-from-prestwick.html';

SELECT * FROM test_text WHERE t = 'http://www.all-inclusive-holiday-bargains.co.uk/all-inclusive-holidays-austria.htm';


CREATE TABLE test_quad(p point);

\copy test_quad from 'data/point.data'

CREATE INDEX tqidx ON test_quad USING spgist (p);

EXPLAIN (COSTS OFF)
SELECT * FROM test_quad WHERE p ~= '(8.51277472174491,5.86434731598175)';

SELECT * FROM test_quad WHERE p ~= '(8.51277472174491,5.86434731598175)';

SELECT * FROM test_quad WHERE p ~= '(1.39955907884019,9.12045046572942)';
