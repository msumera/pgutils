
ALTER TABLE testtable ADD COLUMN description TEXT;

UPDATE testtable SET description = name WHERE id = 1;
