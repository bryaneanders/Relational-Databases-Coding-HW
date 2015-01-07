1)				  
SELECT first, last, cnt
FROM (SELECT first, last, count(crime_id) as cnt
	  FROM officers NATURAL JOIN crime_officers
	  GROUP BY first, last) temp
WHERE cnt >= ( SELECT avg(count) 
			   FROM (SELECT count(crime_id) count
					 FROM officers NATURAL JOIN crime_officers
					 GROUP BY first, last) temp2);

2)
SELECT charge_id
FROM crime_charges
WHERE fine_amount > (SELECT avg(fine_amount)
					 FROM crime_charges)
AND amount_paid < (SELECT avg(amount_paid)
				   FROM crime_charges);

3)
SELECT DISTINCT first, last
FROM criminals NATURAL JOIN crimes
NATURAL JOIN crime_charges
WHERE crime_code IN (SELECT crime_code 
					 FROM crime_charges
					 WHERE crime_id = 10085);

4)
SELECT criminal_id, last, first, count(sentence_id) cnt_sentence
FROM criminals NATURAL JOIN sentences
GROUP BY criminal_id
HAVING count(sentence_id) > 1;

5)
SELECT precinct, count(charge_id) cnt_charges
FROM officers NATURAL JOIN crime_officers
NATURAL JOIN crime_charges
WHERE charge_status = 'GL'
GROUP BY precinct
HAVING count(charge_id) >= 7;

6)
DROP PROCEDURE IF EXISTS getNumber;

DELIMITER //
CREATE PROCEDURE getNumber(
IN off_id INT, 
OUT num_arrests INT)
BEGIN
SELECT count(crime_id)
INTO num_arrests
FROM crime_officers
WHERE officer_id = off_id;
END //
DELIMITER ;
