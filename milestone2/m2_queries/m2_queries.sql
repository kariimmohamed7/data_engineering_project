--query 1
SELECT * FROM cleaned_data ORDER BY loan_amount DESC LIMIT 20;
--query 2
SELECT state, AVG(annual_inc) AS average_income FROM cleaned_data GROUP BY state;
--query 3
SELECT state, AVG(int_rate) AS average_int_rate FROM cleaned_data GROUP BY state
ORDER BY average_int_rate DESC LIMIT 1;
--query 4
SELECT state, AVG(int_rate) AS average_int_rate FROM cleaned_data GROUP BY state
ORDER BY average_int_rate ASC LIMIT 1;
--query 5
SELECT state, grade, MAX(grade_count) AS max_frequency
FROM (SELECT state, grade, COUNT(grade) AS grade_count, RANK() OVER (PARTITION BY state 
ORDER BY COUNT(grade) DESC) AS rank FROM cleaned_data GROUP BY state, grade) subquery
WHERE rank = 1 GROUP BY state, grade;
--query 6, 0 stands for 'Charged off' and 5 stands for 'Late' according to encoded values
SELECT state, COUNT(CASE WHEN loan_status IN ('0', '5') THEN 1 END) * 1.0 / COUNT(*) 
AS non_payment_rate FROM cleaned_data GROUP BY state ORDER BY non_payment_rate DESC LIMIT 1;
--query 7
SELECT AVG(loan_amount) AS average_loan_amount FROM cleaned_data
WHERE issue_date BETWEEN '2015-01-01' AND '2018-12-31';